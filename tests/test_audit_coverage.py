"""Coverage for load-bearing paths the audit flagged as untested: client
typed-error mapping and network retries, the REST refresh success path, the MCP
refresh wrapper, the drift CLI, and LocalStorage.read_previous.
"""

from __future__ import annotations

import httpx
import pytest

from metadata_service.clients.fivetran_client import FivetranClient
from metadata_service.config import Settings
from metadata_service.exceptions import (
    FivetranAuthError,
    FivetranError,
    FivetranNotFoundError,
    FivetranPermissionError,
)


def _fivetran(handler, **kw) -> FivetranClient:
    http = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.fivetran.com/v1")
    return FivetranClient(Settings(fivetran_api_key="k", fivetran_api_secret="s"),
                          client=http, sleep=lambda _s: None, **kw)


# ---- Fivetran typed-error mapping (the extractor's fail-fast rests on this) ----
@pytest.mark.parametrize("status, exc", [
    (401, FivetranAuthError),
    (403, FivetranPermissionError),
    (404, FivetranNotFoundError),
])
def test_fivetran_status_maps_to_typed_error(status, exc):
    client = _fivetran(lambda r: httpx.Response(status, text="nope"))
    with pytest.raises(exc):
        client._request("GET", "/connections/x")


def test_fivetran_retries_network_error_then_raises_typed():
    attempts = {"n": 0}

    def handler(request):
        attempts["n"] += 1
        raise httpx.ConnectError("boom", request=request)

    client = _fivetran(handler, max_retries=3)
    with pytest.raises(FivetranError):
        client._request("GET", "/connections")
    assert attempts["n"] == 3  # retried to the cap, not one-and-done


def test_fivetran_retries_5xx_then_succeeds():
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        if calls["n"] < 2:
            return httpx.Response(503, text="unavailable")
        return httpx.Response(200, json={"data": {"ok": True}})

    client = _fivetran(handler, max_retries=3)
    assert client._request("GET", "/x") == {"data": {"ok": True}}
    assert calls["n"] == 2


# ---- REST refresh success path -------------------------------------------------
def test_rest_refresh_success_maps_body_and_strips_doc(monkeypatch, tmp_path):
    settings = Settings(metadata_storage_backend="local", metadata_local_path=str(tmp_path))
    monkeypatch.setattr("metadata_service.api.routes.get_settings", lambda: settings)

    captured = {}

    def fake_build(_settings, **kwargs):
        captured.update(kwargs)
        return {"status": "success", "latest_updated": True, "snapshot_uri": "file://x",
                "generated_at": "2026-07-15T00:00:00Z", "object_count": 3, "error_count": 0,
                "doc": {"huge": "payload"}}

    monkeypatch.setattr("metadata_service.api.routes.build_and_store", fake_build)
    from fastapi.testclient import TestClient

    from metadata_service.api.main import create_app

    client = TestClient(create_app())
    resp = client.post("/metadata/refresh", json={"fivetran_group_id": "g1", "include_dbt": False})
    assert resp.status_code == 200
    body = resp.json()
    assert "doc" not in body  # the ~1MB document must not be echoed inline
    assert body["object_count"] == 3
    # RefreshRequest fields map into build_and_store kwargs.
    assert captured["group_id"] == "g1" and captured["include_dbt"] is False


# ---- MCP refresh wrapper (in-progress + failure mapping) -----------------------
def _refresh_tool():
    from metadata_service.mcp import server as mcp_server

    return mcp_server.build_server()._tool_manager.get_tool("refresh_metadata").fn


def test_mcp_refresh_maps_in_progress(monkeypatch):
    import anyio

    from metadata_service.exceptions import RefreshInProgressError
    from metadata_service.mcp import tools

    def boom(*a, **k):
        raise RefreshInProgressError("busy")

    monkeypatch.setattr(tools, "refresh_metadata", boom)
    result = anyio.run(_refresh_tool())
    assert result["status"] == "in_progress_error"


def test_mcp_refresh_sanitizes_failure(monkeypatch):
    import anyio

    from metadata_service.mcp import tools

    def boom(*a, **k):
        raise ValueError("snowflake acct xy123 unreachable at /srv/secret")

    monkeypatch.setattr(tools, "refresh_metadata", boom)
    result = anyio.run(_refresh_tool())
    assert result["status"] == "error"
    assert "secret" not in result["message"]  # internal detail not leaked


# ---- drift CLI + LocalStorage.read_previous ------------------------------------
def test_local_read_previous_returns_prior_snapshot(tmp_path):
    from metadata_service.storage.local_storage import LocalStorage

    storage = LocalStorage(str(tmp_path))
    storage.write_snapshot({"generated_at": "2026-07-01T00:00:00Z", "n": 1},
                           snapshot_name="2026-07-01T00-00-00Z")
    storage.write_snapshot({"generated_at": "2026-07-02T00:00:00Z", "n": 2},
                           snapshot_name="2026-07-02T00-00-00Z")
    assert storage.read_latest()["n"] == 2
    assert storage.read_previous()["n"] == 1


def test_drift_cli_reports_records(monkeypatch, tmp_path):
    from typer.testing import CliRunner

    from metadata_service.cli import app
    from metadata_service.storage.local_storage import LocalStorage

    settings = Settings(metadata_storage_backend="local", metadata_local_path=str(tmp_path))
    monkeypatch.setattr("metadata_service.cli.get_settings", lambda: settings)

    scope = {"include_fivetran": True}
    storage = LocalStorage(str(tmp_path))

    def snap(name, objs):
        return {"generated_at": f"2026-07-{name}", "build_scope": scope,
                "warehouse_objects": [{"object_id": o, "origin": {}, "columns": [], "dbt": {}}
                                      for o in objs]}

    storage.write_snapshot(snap("01T00:00:00Z", ["a", "b"]), snapshot_name="2026-07-01T00-00-00Z")
    storage.write_snapshot(snap("02T00:00:00Z", ["a"]), snapshot_name="2026-07-02T00-00-00Z")

    result = CliRunner().invoke(app, ["drift"])
    assert result.exit_code == 0
    assert "removed_table" in result.output


def test_local_read_latest_caches_until_changed(tmp_path):
    from metadata_service.storage.local_storage import LocalStorage

    s = LocalStorage(str(tmp_path))
    s.write_snapshot({"generated_at": "2026-07-01T00:00:00Z", "n": 1},
                     snapshot_name="2026-07-01T00-00-00Z")
    a = s.read_latest()
    b = s.read_latest()
    assert a is b  # cache hit: unchanged latest.json isn't re-parsed
    # A new build rewrites latest.json (different size) -> cache invalidated.
    s.write_snapshot({"generated_at": "2026-07-02T00:00:00Z", "n": 2, "extra": "x"},
                     snapshot_name="2026-07-02T00-00-00Z")
    c = s.read_latest()
    assert c["n"] == 2
