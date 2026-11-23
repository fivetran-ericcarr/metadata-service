"""Remote MCP over Streamable HTTP: tool allowlist, API-key auth, OAuth
resource-server auth (JWT against a locally generated key), health, and the
CLI's public-bind guard. In-process only (Starlette TestClient)."""

from __future__ import annotations

import asyncio
import json
import time

import pytest

pytest.importorskip("mcp")

from fastapi.testclient import TestClient  # noqa: E402

from metadata_service.config import Settings  # noqa: E402
from metadata_service.mcp import server as mcp_server  # noqa: E402
from metadata_service.mcp.auth import JWTVerifier, resolve_auth_mode  # noqa: E402
from metadata_service.storage.base import get_storage  # noqa: E402

KEY = "k" * 32
HEADERS = {"accept": "application/json, text/event-stream", "content-type": "application/json"}
INIT = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
    "protocolVersion": "2025-06-18", "capabilities": {},
    "clientInfo": {"name": "pytest", "version": "1"}}}
LIST = {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}


def _settings(tmp_path, **kw) -> Settings:
    return Settings(metadata_storage_backend="local", metadata_local_path=str(tmp_path), **kw)


def _rpc(client, body, headers=None):
    return client.post("/mcp", headers={**HEADERS, **(headers or {})}, content=json.dumps(body))


def test_allowlist_defaults():
    assert "refresh_metadata" in mcp_server.resolve_tool_allowlist("auto", "stdio")
    http = mcp_server.resolve_tool_allowlist("auto", "streamable-http")
    assert "refresh_metadata" not in http and "get_semantic_context" in http and "search" in http
    assert mcp_server.resolve_tool_allowlist("search,fetch,nope", "stdio") == ["search", "fetch"]
    assert "refresh_metadata" in mcp_server.resolve_tool_allowlist("all", "streamable-http")


def test_tools_carry_read_only_annotations(tmp_path):
    server = mcp_server.build_server(settings=_settings(tmp_path))
    tools = {t.name: t for t in asyncio.run(server.list_tools())}
    assert tools["get_semantic_context"].annotations.readOnlyHint is True
    assert tools["refresh_metadata"].annotations.readOnlyHint is False


def test_auth_mode_resolution():
    assert resolve_auth_mode(Settings()) == "none"
    assert resolve_auth_mode(Settings(metadata_api_key=KEY)) == "api_key"
    assert resolve_auth_mode(Settings(mcp_api_keys="a, b")) == "api_key"
    assert resolve_auth_mode(Settings(mcp_oauth_issuer="https://idp.example")) == "oauth"
    assert Settings(metadata_api_key="x", mcp_api_keys="y,,z").mcp_bearer_keys() == ["x", "y", "z"]


def test_api_key_http_server(tmp_path, doc_snapshot):
    settings = _settings(tmp_path, metadata_api_key=KEY, mcp_api_keys="agent2")
    get_storage(settings).write_snapshot(doc_snapshot)
    app = mcp_server.build_http_app(settings, host="0.0.0.0")
    with TestClient(app) as client:
        assert client.get("/health").json()["status"] == "ok"  # public
        r = _rpc(client, INIT)
        assert r.status_code == 401 and "Bearer" in r.headers["www-authenticate"]
        assert _rpc(client, INIT, {"authorization": "Bearer wrong"}).status_code == 401
        assert _rpc(client, INIT, {"authorization": f"Bearer {KEY}"}).status_code == 200
        assert _rpc(client, INIT, {"x-api-key": "agent2"}).status_code == 200
        r = _rpc(client, LIST, {"authorization": f"Bearer {KEY}"})
        names = {t["name"] for t in r.json()["result"]["tools"]}
        assert "refresh_metadata" not in names and {"get_semantic_context", "search", "fetch"} <= names
        call = {"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                "params": {"name": "get_dq_summary", "arguments": {}}}
        result = _rpc(client, call, {"authorization": f"Bearer {KEY}"}).json()["result"]
        assert result["structuredContent"]["object_count"] >= 1


def test_api_key_mode_requires_a_key(tmp_path):
    with pytest.raises(ValueError):
        mcp_server.build_http_app(_settings(tmp_path, mcp_auth="api_key"))
    with pytest.raises(ValueError):
        mcp_server.build_http_app(_settings(tmp_path, mcp_auth="oauth"))  # no issuer/public url


# -- OAuth -------------------------------------------------------------------
@pytest.fixture(scope="module")
def rsa_key():
    from cryptography.hazmat.primitives.asymmetric import rsa

    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


class _JWKClient:
    def __init__(self, key):
        self._key = key

    def get_signing_key_from_jwt(self, token):
        class K:
            key = self._key.public_key()
        return K()


def _token(key, **claims):
    import jwt

    base = {"iss": "https://idp.example", "aud": "https://mcp.example/mcp", "sub": "user-1",
            "client_id": "chatgpt", "scope": "metadata:read", "exp": int(time.time()) + 600}
    base.update(claims)
    return jwt.encode(base, key, algorithm="RS256")


def test_oauth_http_server(tmp_path, doc_snapshot, rsa_key, monkeypatch):
    monkeypatch.setattr(JWTVerifier, "_client", lambda self: _JWKClient(rsa_key))
    settings = _settings(tmp_path, mcp_oauth_issuer="https://idp.example",
                         mcp_oauth_audience="https://mcp.example/mcp",
                         mcp_public_url="https://mcp.example/mcp",
                         mcp_oauth_required_scopes="metadata:read", metadata_api_key=KEY)
    get_storage(settings).write_snapshot(doc_snapshot)
    app = mcp_server.build_http_app(settings)
    with TestClient(app, base_url="https://mcp.example") as client:
        meta = client.get("/.well-known/oauth-protected-resource/mcp")
        assert meta.status_code == 200
        assert meta.json()["authorization_servers"] == ["https://idp.example/"]
        r = _rpc(client, INIT)
        assert r.status_code == 401
        assert "resource_metadata=" in r.headers["www-authenticate"]
        good = {"authorization": f"Bearer {_token(rsa_key)}"}
        assert _rpc(client, INIT, good).status_code == 200
        for bad in (_token(rsa_key, iss="https://evil.example"), _token(rsa_key, aud="other"),
                    _token(rsa_key, exp=int(time.time()) - 3600), _token(rsa_key, scope="other")):
            assert _rpc(client, INIT, {"authorization": f"Bearer {bad}"}).status_code == 401
        # A static key works alongside OAuth (Claude headers / ADK / Databricks).
        assert _rpc(client, INIT, {"authorization": f"Bearer {KEY}"}).status_code == 200
    # Host allowlist from MCP_PUBLIC_URL: another Host is refused.
    with TestClient(mcp_server.build_http_app(settings), base_url="https://attacker.example") as client:
        assert _rpc(client, INIT, {"authorization": f"Bearer {KEY}"}).status_code == 421


def test_serve_mcp_guard(monkeypatch):
    from typer.testing import CliRunner

    from metadata_service import cli
    from metadata_service.config import get_settings

    calls = []
    monkeypatch.setattr("metadata_service.mcp.server.run_server", lambda **kw: calls.append(kw))
    monkeypatch.delenv("METADATA_API_KEY", raising=False)
    monkeypatch.delenv("METADATA_ALLOW_UNAUTHENTICATED", raising=False)
    get_settings.cache_clear()
    try:
        r = CliRunner().invoke(cli.app, ["serve-mcp", "--transport", "http", "--host", "0.0.0.0"])
        assert r.exit_code == 2 and not calls
        monkeypatch.setenv("METADATA_API_KEY", KEY)
        get_settings.cache_clear()
        r = CliRunner().invoke(cli.app, ["serve-mcp", "--transport", "http", "--host", "0.0.0.0"])
        assert r.exit_code == 0, r.output
        assert calls and calls[0]["transport"] == "http"
        # SSE has no auth hook: still refused on a public bind.
        r = CliRunner().invoke(cli.app, ["serve-mcp", "--transport", "sse", "--host", "0.0.0.0"])
        assert r.exit_code == 2
    finally:
        get_settings.cache_clear()


@pytest.fixture(scope="module")
def doc_snapshot():
    from metadata_service import pipeline

    from .conftest import FIXTURES

    return pipeline.build_metadata(Settings(), fixtures_dir=str(FIXTURES))
