"""Regression tests for the whole-repo audit fixes.

Each test pins a specific finding from the max-effort audit so the corrected
behavior can't silently regress. Grouped by subsystem.
"""

from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient

from metadata_service.clients.fivetran_client import FivetranClient
from metadata_service.config import Settings, guard_remote_bind
from metadata_service.dq.activation_gate import evaluate_syncs
from metadata_service.dq.lineage import LineageGraph
from metadata_service.exceptions import DbtAuthError, DbtError
from metadata_service.models.common import build_object_id
from metadata_service.normalizers import ActivationsNormalizer
from metadata_service.normalizers.combined_normalizer import CombinedNormalizer, _index_dbt
from metadata_service.pipeline import _assess_build_health, build_and_store

from .conftest import FIXTURES


# ---- Finding 1: stale block propagates through model-matched objects ----------
def test_gate_blocks_stale_object_matched_to_model():
    lineage = LineageGraph([])  # activation reads the model directly, no source
    models = [{"unique_id": "model.p.dim_acct", "schema": "marts", "alias": "dim_acct",
               "name": "dim_acct", "governance": {"contract_enforced": True},
               "tests": [{"name": "n", "status": "pass", "severity": "error", "failures": 0}]}]
    sync = {"sync_id": 1, "label": "x", "source_object": {"table_schema": "marts", "table_name": "dim_acct"},
            "destination_name": "sf", "destination_object": "Account", "mappings": []}
    # A stale Fivetran object matched to the model (not a source).
    coverage = [{"object_id": "warehouse://x/marts/dim_acct",
                 "dbt": {"source_unique_id": None, "model_unique_ids": ["model.p.dim_acct"]}}]
    out = evaluate_syncs([sync], models=models, sources=[], lineage=lineage,
                         warehouse_objects=coverage,
                         stale_object_ids={"warehouse://x/marts/dim_acct"})
    r = out[0]["readiness"]
    assert r["verdict"] == "block"
    assert r["upstream"]["stale_objects"] == 1


# ---- Finding 2: unmatched-upstream fires even when nothing matched -------------
def test_gate_flags_unmatched_upstream_when_no_sources_matched():
    lineage = LineageGraph([{"from_unique_id": "source.p.raw.acct",
                             "to_unique_id": "model.p.dim_acct"}])
    models = [{"unique_id": "model.p.dim_acct", "schema": "marts", "alias": "dim_acct",
               "name": "dim_acct", "governance": {"contract_enforced": True},
               "tests": [{"name": "n", "status": "pass", "severity": "error", "failures": 0}]}]
    sources = [{"unique_id": "source.p.raw.acct", "schema": "raw", "identifier": "acct",
                "name": "acct", "tests": []}]
    sync = {"sync_id": 1, "label": "x", "source_object": {"table_schema": "marts", "table_name": "dim_acct"},
            "destination_name": "sf", "destination_object": "Account", "mappings": []}
    # Coverage data exists, but NOTHING matched a dbt source (empty matched set).
    coverage = [{"object_id": "warehouse://x/other/thing",
                 "dbt": {"source_unique_id": None, "model_unique_ids": []}}]
    out = evaluate_syncs([sync], models=models, sources=sources, lineage=lineage,
                         warehouse_objects=coverage)
    r = out[0]["readiness"]
    assert r["upstream"]["unmatched_upstream"] == 1
    assert r["verdict"] == "warn"


# ---- Finding 4: a degraded build must not become the served baseline ----------
def test_assess_build_health_classifies():
    assert _assess_build_health({"warehouse_objects": [1, 2], "errors": []}, None) == "success"
    # errors but inventory intact -> partial (still publishable)
    assert _assess_build_health({"warehouse_objects": [1, 2], "errors": [{"e": 1}]},
                                {"warehouse_objects": [1, 2]}) == "partial"
    # errored and empty -> degraded
    assert _assess_build_health({"warehouse_objects": [], "errors": [{"e": 1}]}, None) == "degraded"
    # errored and lost >half the inventory -> degraded
    assert _assess_build_health({"warehouse_objects": [1], "errors": [{"e": 1}]},
                                {"warehouse_objects": [1, 2, 3, 4]}) == "degraded"


def test_degraded_build_does_not_overwrite_latest(monkeypatch, tmp_path):
    settings = Settings(metadata_storage_backend="local", metadata_local_path=str(tmp_path),
                        warehouse_type="warehouse", stale_sync_threshold_hours=24)
    # First: a healthy fixture build establishes a good latest.json.
    build_and_store(settings, fixtures_dir=str(FIXTURES))
    from metadata_service.storage.base import get_storage
    good = get_storage(settings).read_latest()
    good_count = len(good["warehouse_objects"])
    assert good_count > 0

    # Now force a degraded build (errors + empty inventory) and confirm latest is untouched.
    import metadata_service.pipeline as pipeline

    def fake_build(*a, **k):
        return {"generated_at": "2099-01-01T00:00:00Z", "version": "1.0",
                "warehouse_objects": [], "errors": [{"error_type": "Boom"}], "schema_drift": []}

    monkeypatch.setattr(pipeline, "build_metadata", fake_build)
    result = build_and_store(settings)
    assert result["status"] == "degraded"
    assert result["latest_updated"] is False
    after = get_storage(settings).read_latest()
    assert len(after["warehouse_objects"]) == good_count  # still the good snapshot


# ---- Finding 5: object_id scheme is fixed, not derived from WAREHOUSE_TYPE -----
def test_object_id_scheme_is_warehouse_agnostic():
    assert build_object_id(None, "marts", "dim_account") == "warehouse://unknown/marts/dim_account"


# ---- Finding 6: Fivetran items:null does not crash pagination -----------------
def test_fivetran_pagination_handles_null_items():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": {"items": None, "next_cursor": None}})

    http = httpx.Client(transport=httpx.MockTransport(handler),
                        base_url="https://api.fivetran.com/v1")
    client = FivetranClient(Settings(fivetran_api_key="k", fivetran_api_secret="s"), client=http)
    assert client._get_paginated("/connections") == []  # no TypeError


# ---- Finding 7: dbt auth/rate-limit errors are fatal for the whole extraction --
def test_dbt_extractor_reraises_auth_error():
    from metadata_service.extractors.dbt_extractor import DbtExtractor

    class FakeClient:
        def list_projects(self, *a, **k):
            raise DbtAuthError("token revoked")

    with pytest.raises(DbtAuthError):
        DbtExtractor(FakeClient(), "acct").extract()


# ---- Finding 8: activations paginator returns partial at the page cap ---------
def test_activations_pagination_returns_partial_at_cap(monkeypatch):
    from metadata_service.clients import activations_client as ac

    monkeypatch.setattr(ac, "_MAX_PAGES", 2)
    monkeypatch.setattr(ac, "_PER_PAGE", 1)
    client = ac.ActivationsClient(Settings(activations_api_token="t"))

    pages = {1: {"data": [{"id": 1}], "pagination": {"next_page": 2}},
             2: {"data": [{"id": 2}], "pagination": {"next_page": 3}}}  # never terminates
    monkeypatch.setattr(client, "_get", lambda path, params=None: pages[params["page"]])
    items = client._list_paginated("/syncs")
    assert [i["id"] for i in items] == [1, 2]  # partial, not an exception or []


# ---- Finding 9: non-ASCII API key header -> 401, not a 500 --------------------
def test_non_ascii_api_key_is_rejected_cleanly(monkeypatch, tmp_path):
    settings = Settings(metadata_storage_backend="local", metadata_local_path=str(tmp_path),
                        warehouse_type="warehouse", stale_sync_threshold_hours=24,
                        metadata_api_key="s3cret")
    monkeypatch.setattr("metadata_service.api.routes.get_settings", lambda: settings)
    build_and_store(settings, fixtures_dir=str(FIXTURES))
    from metadata_service.api.main import create_app

    client = TestClient(create_app())
    # Send the raw latin-1 bytes a real client can put on the wire (Starlette
    # decodes headers as latin-1); a str "café" is rejected by httpx itself.
    resp = client.get("/metadata/latest", headers={"X-API-Key": "café".encode("latin-1")})
    assert resp.status_code == 401  # clean 401, not a compare_digest TypeError -> 500


# ---- Findings 10/11: refuse unauthenticated non-loopback bind -----------------
def test_guard_remote_bind():
    guard_remote_bind("127.0.0.1", None, surface="x")   # loopback: fine
    guard_remote_bind("localhost", "", surface="x")     # loopback: fine
    guard_remote_bind("0.0.0.0", "key", surface="x")    # remote + key: fine
    with pytest.raises(ValueError):
        guard_remote_bind("0.0.0.0", None, surface="the REST API")
    with pytest.raises(ValueError):
        guard_remote_bind("0.0.0.0", "", surface="the REST API")  # empty key == no key


# ---- Finding 12: composite-PK tables can satisfy has_primary_key_tests --------
def _composite_pk_obj(combo_test: bool):
    tests = [{"unique_id": "t1", "test_type": "not_null", "status": "pass"}]
    if combo_test:
        tests.append({"unique_id": "t2", "test_type": "dbt_utils.unique_combination_of_columns",
                      "status": "pass"})
    return {
        "columns": [{"name": "order_id", "is_primary_key": True, "dbt_tests": ["not_null"]},
                    {"name": "line_no", "is_primary_key": True, "dbt_tests": ["not_null"]}],
        "dbt": {"tests": tests, "freshness": None},
        "match_confidence": "exact_schema_table",
    }


def test_composite_pk_with_combination_test_is_covered():
    summary = CombinedNormalizer()._summarize(_composite_pk_obj(combo_test=True), [])
    assert summary["has_primary_key_tests"] is True
    assert summary["risk_level"] == "low"


def test_composite_pk_without_uniqueness_is_not_covered():
    summary = CombinedNormalizer()._summarize(_composite_pk_obj(combo_test=False), [])
    assert summary["has_primary_key_tests"] is False


# ---- Finding 13: null-filled dbt freshness dict is not "configured" -----------
def test_null_filled_freshness_is_not_configured():
    unconfigured = {"freshness": {"warn_after": {"count": None, "period": None},
                                  "error_after": {"count": None, "period": None}, "filter": None}}
    assert CombinedNormalizer._freshness(unconfigured) is None
    configured = {"freshness": {"warn_after": {"count": 24, "period": "hour"}}}
    assert CombinedNormalizer._freshness(configured) == {"status": None, "max_loaded_at": None,
                                                         "configured": True}


# ---- Finding 14: a configured alias to a model beats a source name hit --------
def test_alias_to_model_beats_source_name_hit():
    cn = CombinedNormalizer(aliases={"analytics.orders": "marts.orders_final"})
    sources = [{"unique_id": "source.p.analytics.orders", "schema": "analytics",
                "identifier": "orders", "name": "orders", "tests": []}]
    models = [{"unique_id": "model.p.orders_final", "schema": "marts", "alias": "orders_final",
               "name": "orders_final", "governance": {}, "tests": []}]
    source_index = _index_dbt(sources, id_field="identifier", name_field="table_name")
    model_index = _index_dbt(models, id_field="alias", name_field="name")
    table = {"destination_schema": "analytics", "destination_table": "orders", "columns": []}
    obj = cn._build_object({}, table, source_index, model_index,
                           {s["unique_id"]: s for s in sources},
                           {m["unique_id"]: m for m in models}, LineageGraph([]), [], [])
    assert obj["match_confidence"] == "configured_alias"
    assert obj["dbt"]["model_unique_ids"] == ["model.p.orders_final"]
    assert obj["dbt"]["source_unique_id"] is None


# ---- Finding 17: dbt index collision loser wins neither tier ------------------
def test_index_collision_loser_indexed_nowhere():
    objs = [{"unique_id": "model.p.one", "schema": "marts", "alias": "Dim_Account", "name": "Dim_Account"},
            {"unique_id": "model.p.two", "schema": "marts", "alias": "dim_account", "name": "dim_account"}]
    index = _index_dbt(objs, id_field="alias", name_field="name")
    # Both exact-case keys resolve to the FIRST (kept) object, matching the warning.
    assert index["exact"].get(("marts", "dim_account")) is None or \
           index["exact"][("marts", "dim_account")]["unique_id"] == "model.p.one"
    assert index["ci"][("marts", "dim_account")]["unique_id"] == "model.p.one"


# ---- Finding 18: scoped column-lineage tier marks cross-database ambiguity -----
def test_column_lineage_scoped_tier_is_ambiguity_safe():
    from metadata_service.dq.column_lineage import _build_name_index

    manifest = {"sources": {
        "source.d.dev.orders": {"database": "DEV", "schema": "RAW", "identifier": "orders", "name": "orders"},
        "source.d.prod.orders": {"database": "PROD", "schema": "RAW", "identifier": "orders", "name": "orders"},
    }, "nodes": {}}
    idx = _build_name_index(manifest)
    # Two databases share RAW.orders -> the (schema, table) tier must be ambiguous.
    assert idx["scoped"].get(("RAW", "ORDERS")) is None
    # ...but the fully-qualified tier still resolves each precisely.
    assert idx["full"][("DEV", "RAW", "ORDERS")] == "source.d.dev.orders"
    assert idx["full"][("PROD", "RAW", "ORDERS")] == "source.d.prod.orders"


# ---- Finding 20: last_synced_at is not the config-edit time -------------------
def test_last_synced_at_is_not_config_edit_time():
    raw = {"syncs": [{"id": 1, "label": "x", "updated_at": "2026-06-30T00:00:00Z",
                      "source_attributes": {}, "destination_attributes": {}, "mappings": []}]}
    s = ActivationsNormalizer().normalize(raw)["syncs"][0]
    assert s["last_synced_at"] is None            # no real run timestamp available
    assert s["config_updated_at"] == "2026-06-30T00:00:00Z"

    raw2 = {"syncs": [{"id": 2, "label": "y", "updated_at": "2026-06-30T00:00:00Z",
                       "latest_sync_run": {"completed_at": "2026-07-10T09:00:00Z"},
                       "source_attributes": {}, "destination_attributes": {}, "mappings": []}]}
    s2 = ActivationsNormalizer().normalize(raw2)["syncs"][0]
    assert s2["last_synced_at"] == "2026-07-10T09:00:00Z"


# ---- Storage durability -------------------------------------------------------
def test_default_snapshot_names_are_collision_free(tmp_path):
    from metadata_service.storage.local_storage import LocalStorage

    storage = LocalStorage(str(tmp_path))
    storage.write_snapshot({"generated_at": "2026-07-01T00:00:00Z"})  # default (microsecond) name
    storage.write_snapshot({"generated_at": "2026-07-01T00:00:00Z"})  # same second
    assert len(storage.list_snapshots()) == 2  # distinct history files, neither overwritten


def test_snapshot_written_data_is_flushed(tmp_path):
    # A written snapshot is fully readable (exercises the fsync path end-to-end).
    from metadata_service.storage.local_storage import LocalStorage

    storage = LocalStorage(str(tmp_path))
    storage.write_snapshot({"generated_at": "2026-07-01T00:00:00Z", "n": 1},
                           snapshot_name="2026-07-01T00-00-00Z")
    assert storage.read_latest()["n"] == 1


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


def test_s3_prefix_boundary_isolates_sibling_deployments():
    from metadata_service.storage.s3_storage import S3Storage

    shared = _FakeS3()
    prod = S3Storage("bkt", "metadata", client=shared, retain=2)
    # A sibling deployment on a prefix that shares the stem.
    other = S3Storage("bkt", "metadata-prod", client=shared, retain=2)
    for i in range(3):
        prod.write_snapshot({"generated_at": f"2026-07-0{i+1}T00:00:00Z", "n": i},
                            snapshot_name=f"2026-07-0{i+1}T00-00-00Z")
    other.write_snapshot({"generated_at": "2026-07-01T00:00:00Z", "who": "other"},
                         snapshot_name="2026-07-01T00-00-00Z")

    # prod's view must not include the sibling's key, and retention (2) must not
    # have deleted the sibling's snapshot.
    assert all("metadata-prod" not in k for k in prod.list_snapshots())
    assert len(prod.list_snapshots()) == 2
    assert "metadata-prod/2026/07/01/2026-07-01T00-00-00Z.json" in shared.objects


# ---- Concurrency --------------------------------------------------------------
def test_build_lock_serializes_cross_process(tmp_path):
    # The file lock is held for the duration of a build; a second acquisition
    # from a would-be concurrent process is refused.
    import metadata_service.pipeline as pipeline
    from metadata_service.exceptions import RefreshInProgressError

    settings = Settings(metadata_storage_backend="local", metadata_local_path=str(tmp_path))
    with pipeline._build_lock(settings):
        # Simulate another process: a fresh flock on the same lockfile must fail.
        import fcntl
        fh = open(tmp_path / ".build.lock", "w")
        try:
            with pytest.raises(OSError):
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            fh.close()
    # Released after the block: a new build lock acquires cleanly.
    with pipeline._build_lock(settings):
        pass


def test_mcp_read_tools_are_async_offloaded():
    # Every read tool must be a coroutine so FastMCP doesn't run blocking storage
    # I/O inline on the event loop (only refresh was threaded before).
    import inspect

    from metadata_service.mcp import server as mcp_server

    tools_registered = mcp_server.build_server()._tool_manager.list_tools()
    assert tools_registered, "no MCP tools registered"
    assert all(inspect.iscoroutinefunction(t.fn) for t in tools_registered)
