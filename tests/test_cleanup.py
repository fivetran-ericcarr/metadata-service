"""Tests for the medium/low review-finding cleanup: client retry/pagination
behavior, artifact anchoring, matching/recommendation fixes, storage retention,
surface fixes, and previously-untested modules (S3, Snowflake reader, CLI, MCP
server bindings)."""

from __future__ import annotations

import json

import httpx
import pytest

from metadata_service.config import Settings
from metadata_service.exceptions import (
    ActivationsRateLimitError,
    DbtArtifactNotFoundError,
    DbtAuthError,
    DbtNotFoundError,
    DbtRateLimitError,
)

from .conftest import FIXTURES, load_fixture


# -- Fivetran client ----------------------------------------------------------
def _ft_client(handler, sleeps=None):
    from metadata_service.clients import FivetranClient

    http = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://ft.test")
    return FivetranClient(Settings(), client=http,
                          sleep=(sleeps.append if sleeps is not None else lambda s: None))


def test_fivetran_repeated_cursor_stops_instead_of_looping():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        # Always return the same non-empty cursor — a buggy API that would
        # previously loop (and duplicate items) forever.
        return httpx.Response(200, json={"data": {"items": [{"id": calls["n"]}], "next_cursor": "STUCK"}})

    items = _ft_client(handler)._get_paginated("/connections")
    assert calls["n"] == 2  # page 1 + the one repeat, then stop
    assert len(items) == 2


def test_fivetran_retry_after_is_clamped():
    from metadata_service.clients.fivetran_client import _parse_retry_after

    assert _parse_retry_after("86400") == 60.0  # one header can't stall a run for a day
    assert _parse_retry_after("Wed, 21 Oct 2026 07:28:00 GMT") == 2.0  # HTTP-date -> default
    assert _parse_retry_after("5") == 5.0


def test_fivetran_no_sleep_after_final_attempt():
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    with pytest.raises(Exception):
        _ft_client(handler, sleeps)._request("GET", "/x")
    # 3 attempts -> only 2 sleeps (none wasted after the last failure)
    assert len(sleeps) == 2


# -- dbt client ---------------------------------------------------------------
def _dbt_client(handler, sleeps=None):
    from metadata_service.clients import DbtClient

    http = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://dbt.test")
    return DbtClient(Settings(), client=http,
                     sleep=(sleeps.append if sleeps is not None else lambda s: None))


def test_dbt_429_honors_retry_after():
    sleeps: list[float] = []
    state = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        state["n"] += 1
        if state["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "7"})
        return httpx.Response(200, json={"data": []})

    _dbt_client(handler, sleeps)._get_json("/v2/x")
    assert sleeps == [7.0]  # not the old fixed 2s backoff


def test_dbt_404_is_generic_not_artifact():
    client = _dbt_client(lambda r: httpx.Response(404))
    with pytest.raises(DbtNotFoundError) as exc_info:
        client.list_projects("123")
    assert not isinstance(exc_info.value, DbtArtifactNotFoundError)
    # ...but the artifact endpoint maps it back to the artifact-specific error
    with pytest.raises(DbtArtifactNotFoundError):
        client.get_run_artifact("123", 1, "manifest.json")


def test_dbt_list_runs_paginates_past_100():
    pages: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        limit = int(request.url.params["limit"])
        offset = int(request.url.params["offset"])
        pages.append({"limit": limit, "offset": offset})
        assert limit <= 100  # the Admin API cap is never exceeded
        return httpx.Response(200, json={"data": [{"id": offset + i} for i in range(limit)]})

    runs = _dbt_client(handler).list_runs("123", limit=250)
    assert len(runs) == 250
    assert [p["offset"] for p in pages] == [0, 100, 200]


def test_dbt_discovery_requires_token():
    client = _dbt_client(lambda r: httpx.Response(200, json={}))
    client._settings = Settings(dbt_metadata_api_url="https://meta.test/graphql", dbt_service_token=None)
    with pytest.raises(DbtAuthError):  # never sends "Bearer None" over the wire
        client.query_discovery("query { x }")


# -- Activations client ---------------------------------------------------------
def test_activations_429_raises_typed_error():
    from metadata_service.clients import ActivationsClient

    http = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(429)),
                        base_url="https://census.test")
    client = ActivationsClient(Settings(activations_api_token="tok"), client=http,
                               sleep=lambda s: None)
    with pytest.raises(ActivationsRateLimitError):
        client.list_syncs()


# -- dbt extractor: artifacts anchored to ONE run --------------------------------
class _MixingClient:
    """Newest run has run_results but NO manifest; older run has everything.
    The old first-found-wins walk would mix run 2's run_results with run 1's
    manifest — different code versions whose unique_ids don't line up."""

    def get_run_artifact(self, account_id, run_id, path):
        if run_id == 2 and path == "manifest.json":
            raise DbtArtifactNotFoundError("no manifest on run 2")
        return {"from_run": run_id, "artifact": path}


def test_dbt_artifacts_come_from_a_single_run():
    from metadata_service.extractors.dbt_extractor import DbtExtractor

    extractor = DbtExtractor(_MixingClient(), "acct")
    runs = [{"id": 2, "status": "10"}, {"id": 1, "status": "10"}]
    errors: list[dict] = []
    artifacts = extractor._download_from_recent_success(runs, errors)
    origins = {v["from_run"] for k, v in artifacts.items() if isinstance(v, dict)}
    assert origins == {1}  # run 2 (no manifest) contributed NOTHING
    assert artifacts["artifacts_run_id"] == 1


def test_dbt_non_success_fallback_is_recorded():
    from metadata_service.extractors.dbt_extractor import DbtExtractor

    class AllArtifacts:
        def get_run_artifact(self, account_id, run_id, path):
            return {"from_run": run_id}

    errors: list[dict] = []
    artifacts = DbtExtractor(AllArtifacts(), "acct")._download_from_recent_success(
        [{"id": 9, "status": "20", "status_humanized": "Error", "finished_at": "2026-07-01"}], errors)
    assert artifacts["artifacts_run_id"] == 9
    assert any(e["error_type"] == "NonSuccessRunArtifacts" for e in errors)


# -- matching + recommendations ---------------------------------------------------
def test_alias_overrides_exact_match(settings, fivetran_normalized, dbt_normalized):
    from metadata_service.normalizers import CombinedNormalizer

    # Two sources: the exact-name match binds salesforce.account to the WRONG
    # one; the alias must beat the exact hit (it used to be checked after).
    dbt = dict(dbt_normalized)
    wrong = dict(dbt_normalized["sources"][0])
    right = dict(wrong, unique_id="source.demo.salesforce_v2.account",
                 schema="salesforce_v2", source_name="salesforce_v2")
    dbt["sources"] = [wrong, right]
    doc = CombinedNormalizer(settings, aliases={"salesforce.account": "salesforce_v2.account"}).build(
        fivetran_normalized, dbt)
    account = next(o for o in doc["warehouse_objects"] if o["name"] == "account")
    assert account["match_confidence"] == "configured_alias"
    assert account["dbt"]["source_unique_id"] == "source.demo.salesforce_v2.account"


def test_malformed_aliases_are_skipped_not_fatal(settings, fivetran_normalized, dbt_normalized):
    from metadata_service.normalizers import CombinedNormalizer

    # None value / dotless target used to crash at construction (AttributeError).
    normalizer = CombinedNormalizer(settings, aliases={"a.b": None, "c.d": "nodot", 42: "x.y"})
    doc = normalizer.build(fivetran_normalized, dbt_normalized)
    assert doc["warehouse_objects"]  # build survived


def test_freshness_rec_not_raised_when_configured_but_unrun():
    from metadata_service.dq.recommendations import recommend_for_object

    obj = {
        "object_id": "warehouse://x/s/t", "schema": "s", "name": "t", "columns": [],
        "match_confidence": "exact_schema_table", "origin": {"enabled": True},
        "dbt": {"source_unique_id": "source.p.s.t", "model_unique_ids": [],
                "tests": [{"name": "n", "status": "pass"}],
                # sources.json missing this run, but freshness IS configured
                "freshness": {"status": None, "max_loaded_at": None, "configured": True}},
    }
    recs = recommend_for_object(obj)
    assert not any(r.get("test_name") == "source_freshness" for r in recs)


def test_composite_pk_rec_does_not_refire_when_test_exists():
    from metadata_service.dq.recommendations import recommend_for_object

    obj = {
        "object_id": "warehouse://x/s/t", "schema": "s", "name": "t",
        "match_confidence": "exact_schema_table", "origin": {"enabled": True},
        "columns": [
            {"name": "a", "is_primary_key": True, "dbt_tests": []},
            {"name": "b", "is_primary_key": True, "dbt_tests": []},
        ],
        "dbt": {"source_unique_id": "source.p.s.t", "model_unique_ids": [],
                "tests": [{"name": "dbt_utils_unique_combination_of_columns_t_a__b",
                            "test_type": "unique_combination_of_columns", "status": "pass"}]},
    }
    recs = recommend_for_object(obj)
    assert not any(r.get("test_name") == "dbt_utils.unique_combination_of_columns" for r in recs)


def test_drift_improvement_is_not_high_severity():
    from metadata_service.dq.drift import detect_drift

    scope = {"g": 1}
    def snap(status):
        return {"build_scope": scope, "warehouse_objects": [{
            "object_id": "warehouse://x/s/t", "origin": {}, "columns": [],
            "dbt": {"tests": [{"unique_id": "t1", "name": "t1", "status": status}]},
        }]}

    improved = detect_drift(snap("fail"), snap("pass"))
    assert improved[0]["change_type"] == "dbt_test_status_changed"
    assert improved[0]["severity"] == "low"
    regressed = detect_drift(snap("pass"), snap("fail"))
    assert regressed[0]["severity"] == "high"


# -- pipeline / surfaces -----------------------------------------------------------
def test_write_latest_false_does_not_persist(tmp_path):
    from metadata_service.pipeline import build_and_store

    settings = Settings(metadata_storage_backend="local", metadata_local_path=str(tmp_path))
    result = build_and_store(settings, fixtures_dir=str(FIXTURES), write_latest=False)
    assert result["snapshot_uri"] is None
    assert not (tmp_path / "latest.json").exists()


@pytest.fixture()
def seeded(tmp_path):
    from metadata_service.pipeline import build_and_store

    settings = Settings(metadata_storage_backend="local", metadata_local_path=str(tmp_path))
    build_and_store(settings, fixtures_dir=str(FIXTURES))
    return settings


def test_get_latest_metadata_all_is_slim_full_is_verbatim(seeded):
    from metadata_service.mcp import tools

    slim = tools.get_latest_metadata("all", settings=seeded)
    # raw per-source payloads replaced by counts (no table/column dumps inline)
    assert "destination_table" not in json.dumps(slim["sources"])
    assert slim["sources"]["fivetran"]["sizes"]["connections"] >= 1
    assert slim["warehouse_objects"]  # the join is still inline

    full = tools.get_latest_metadata("full", settings=seeded)
    assert full["sources"]["fivetran"]["connections"]  # verbatim


def test_schema_drift_table_only_filter_matches(seeded, monkeypatch):
    from metadata_service.mcp import tools
    from metadata_service.storage.base import get_storage

    # inject a drift record, then filter by table only (used to build "//account")
    storage = get_storage(seeded)
    doc = storage.read_latest()
    doc["schema_drift"] = [{"object_id": "warehouse://unknown/salesforce/account",
                            "change_type": "new_column", "severity": "medium", "details": {}}]
    storage.write_snapshot(doc)
    out = tools.get_schema_drift(table="account", settings=seeded)
    assert out["count"] == 1
    assert tools.get_schema_drift(table="nope", settings=seeded)["count"] == 0


def test_activation_readiness_by_schema_table(seeded):
    from metadata_service.mcp import tools

    hit = tools.get_activation_readiness(schema="marts", table="dim_account", settings=seeded)
    assert hit["sync_id"] == 900
    miss = tools.get_activation_readiness(table="not_activated", settings=seeded)
    assert miss["found"] is False


def test_offset_pagination(seeded):
    from metadata_service.mcp import tools

    page1 = tools.list_warehouse_objects(limit=1, offset=0, settings=seeded)
    page2 = tools.list_warehouse_objects(limit=1, offset=1, settings=seeded)
    assert page1["count"] == page2["count"] == 2
    assert page1["objects"][0]["name"] != page2["objects"][0]["name"]


def test_rest_object_lookup_requires_delimited_suffix(seeded, monkeypatch):
    from fastapi.testclient import TestClient

    monkeypatch.setattr("metadata_service.api.routes.get_settings", lambda: seeded)
    from metadata_service.api.main import create_app

    client = TestClient(create_app())
    ok = client.get("/metadata/warehouse-objects/salesforce/account")
    assert ok.status_code == 200
    # a bare name must 404, not match the first id that merely ends with it
    assert client.get("/metadata/warehouse-objects/account").status_code == 404


# -- storage: retention + S3 -------------------------------------------------------
def test_local_retention_prunes_oldest(tmp_path):
    from metadata_service.storage.local_storage import LocalStorage

    storage = LocalStorage(str(tmp_path), retain=2)
    for i in range(4):
        storage.write_snapshot({"generated_at": f"2026-07-0{i+1}T00:00:00Z"},
                               snapshot_name=f"2026-07-0{i+1}T00-00-00Z")
    assert len(storage.list_snapshots()) == 2
    assert storage.read_latest()["generated_at"] == "2026-07-04T00:00:00Z"


def test_local_storage_ignores_stray_json(tmp_path):
    from metadata_service.storage.local_storage import LocalStorage

    storage = LocalStorage(str(tmp_path))
    (tmp_path / "aliases.json").write_text("{}")  # must never become a "snapshot"
    storage.write_snapshot({"generated_at": "2026-07-01T00:00:00Z"},
                           snapshot_name="2026-07-01T00-00-00Z")
    assert len(storage.list_snapshots()) == 1


class _FakeS3:
    class exceptions:
        class NoSuchKey(Exception):
            pass

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise self.exceptions.NoSuchKey()
        import io
        return {"Body": io.BytesIO(self.objects[Key])}

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)

    def get_paginator(self, name):
        objects = self.objects

        class P:
            def paginate(self, Bucket, Prefix):
                yield {"Contents": [{"Key": k} for k in sorted(objects) if k.startswith(Prefix)]}
        return P()


def test_s3_storage_roundtrip_prune_and_missing_latest():
    from metadata_service.storage.s3_storage import S3Storage

    storage = S3Storage("bkt", "meta", client=_FakeS3(), retain=2)
    assert storage.read_latest() is None  # NoSuchKey -> None, not a 500
    for i in range(3):
        storage.write_snapshot({"generated_at": f"2026-07-0{i+1}T00:00:00Z", "n": i},
                               snapshot_name=f"2026-07-0{i+1}T00-00-00Z")
    assert storage.read_latest()["n"] == 2
    assert len(storage.list_snapshots()) == 2  # pruned to retention
    assert storage.read_previous()["n"] == 1


# -- Snowflake reader ---------------------------------------------------------------
def test_snowflake_reader_folds_pk_rows():
    from metadata_service.warehouse.snowflake_reader import SnowflakeMetadataReader

    class FakeCursor:
        def execute(self, sql, params=None):
            self.sql, self.params = sql, params

        def fetchall(self):
            return [("GITHUB", "ISSUE", "ID"), ("GITHUB", "ISSUE", "REPO_ID"), ("GITHUB", "USER", "ID")]

        def close(self):
            pass

    class FakeConn:
        def cursor(self):
            return FakeCursor()

    settings = Settings(warehouse_type="snowflake", warehouse_account="a", warehouse_user="u",
                        warehouse_database="DB", warehouse_password="p")
    reader = SnowflakeMetadataReader(settings)
    reader._conn = FakeConn()
    pk_map = reader.read_primary_keys(["conn1"])
    # contract: keys lowercased, column names preserved as the warehouse returns them
    assert pk_map[("github", "issue")] == ["ID", "REPO_ID"]
    assert pk_map[("github", "user")] == ["ID"]


def test_snowflake_reader_loads_passphrase_protected_key(tmp_path):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    from metadata_service.warehouse.snowflake_reader import SnowflakeMetadataReader

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(b"hunter2"),
    )
    path = tmp_path / "enc_key.pem"
    path.write_bytes(pem)
    der = SnowflakeMetadataReader._load_private_key(str(path), passphrase="hunter2")
    assert der[:5]  # decrypted + re-serialized to DER
    with pytest.raises(TypeError):
        SnowflakeMetadataReader._load_private_key(str(path), passphrase=None)


# -- CLI + MCP server bindings ------------------------------------------------------
def test_cli_fixtures_build_defaults_to_dry_run(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from metadata_service import cli
    from metadata_service.config import get_settings

    monkeypatch.setenv("METADATA_STORAGE_BACKEND", "local")
    monkeypatch.setenv("METADATA_LOCAL_PATH", str(tmp_path))
    get_settings.cache_clear()
    try:
        result = CliRunner().invoke(cli.app, ["build", "--fixtures-dir", str(FIXTURES)])
        assert result.exit_code == 0, result.output
        assert "NOT persisted" in result.output
        assert not (tmp_path / "latest.json").exists()  # fixtures never clobber latest
        result = CliRunner().invoke(cli.app, ["build", "--fixtures-dir", str(FIXTURES), "--write-latest"])
        assert result.exit_code == 0, result.output
        assert (tmp_path / "latest.json").exists()
    finally:
        get_settings.cache_clear()


def test_mcp_server_exposes_all_tools():
    import asyncio

    from metadata_service.mcp.server import build_server

    server = build_server()
    tools = asyncio.run(server.list_tools())
    names = {t.name for t in tools}
    assert names == {
        "get_dq_summary", "list_warehouse_objects", "get_warehouse_object",
        "get_impact", "get_column_impact", "list_metrics", "get_metric_quality",
        "list_activations", "get_activation_readiness", "get_dq_recommendations",
        "get_schema_drift", "get_latest_metadata", "refresh_metadata",
    }
