"""Tests for Fivetran Activations (reverse ETL): normalization, the readiness
gate, the combined-doc join, and the MCP/REST surfaces."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from metadata_service.config import Settings
from metadata_service.dq.activation_gate import evaluate_syncs
from metadata_service.dq.lineage import LineageGraph
from metadata_service.mcp import tools
from metadata_service.normalizers import ActivationsNormalizer
from metadata_service.pipeline import build_and_store, build_metadata

from .conftest import FIXTURES, load_fixture


# -- normalizer -----------------------------------------------------------
def test_activations_normalizer_shapes_a_sync():
    raw = load_fixture("activations_syncs.json")
    norm = ActivationsNormalizer().normalize(raw)
    assert len(norm["syncs"]) == 1
    s = norm["syncs"][0]
    assert s["sync_id"] == 900
    assert s["paused"] is True
    assert s["source_object"] == {"table_catalog": "DEMO_DB", "table_schema": "marts", "table_name": "dim_account"}
    assert s["destination_name"] == "salesforce_prod"
    assert s["destination_type"] == "salesforce"
    assert s["destination_object"] == "Account"
    pk = [m for m in s["mappings"] if m["is_primary_identifier"]]
    assert pk == [{"source_column": "account_id", "destination_field": "Id", "is_primary_identifier": True}]


# -- gate (hand-built lineage for deterministic verdicts) -----------------
def _graph():
    # source -> stg -> mart
    return LineageGraph([
        {"from_unique_id": "source.p.raw.acct", "to_unique_id": "model.p.stg_acct"},
        {"from_unique_id": "model.p.stg_acct", "to_unique_id": "model.p.dim_acct"},
    ])


def _sync(schema="marts", table="dim_acct"):
    return {"sync_id": 1, "label": "x", "source_object": {"table_schema": schema, "table_name": table},
            "destination_name": "sf", "destination_object": "Account", "mappings": []}


def _passing(name="not_null_id"):
    return {"name": name, "status": "pass", "severity": "ERROR", "failures": 0}


# Minimal Fivetran coverage so the gate has stale/unmatched evidence to work with.
_COVERAGE = [{"object_id": "warehouse://x/raw/acct",
              "dbt": {"source_unique_id": "source.p.raw.acct", "model_unique_ids": []}}]


def test_gate_blocks_on_upstream_failing_test():
    models = [
        {"unique_id": "model.p.dim_acct", "schema": "marts", "alias": "dim_acct", "name": "dim_acct",
         "governance": {"contract_enforced": True}, "tests": []},
        {"unique_id": "model.p.stg_acct", "schema": "staging", "alias": "stg_acct", "name": "stg_acct",
         "governance": {"contract_enforced": False},
         "tests": [{"name": "unique_id", "status": "fail", "severity": "error", "failures": 5}]},
    ]
    out = evaluate_syncs([_sync()], models=models, sources=[], lineage=_graph())
    r = out[0]["readiness"]
    assert r["verdict"] == "block"
    assert r["upstream"]["failing_tests"] == 1
    assert any(x["code"] == "upstream_failing_tests" for x in r["reasons"])


def test_gate_blocks_on_warn_severity_test_with_failures():
    # A warn-severity test that is actually firing (failures>0) still blocks a push to prod.
    models = [
        {"unique_id": "model.p.dim_acct", "schema": "marts", "alias": "dim_acct", "name": "dim_acct",
         "governance": {"contract_enforced": True}, "tests": []},
        {"unique_id": "model.p.stg_acct", "schema": "staging", "alias": "stg_acct", "name": "stg_acct",
         "governance": {"contract_enforced": True},
         "tests": [{"name": "unique_id", "status": "warn", "severity": "warn", "failures": 143}]},
    ]
    out = evaluate_syncs([_sync()], models=models, sources=[], lineage=_graph())
    r = out[0]["readiness"]
    assert r["verdict"] == "block"
    assert r["upstream"]["warn_tests_with_failures"] == 1
    assert any(x["code"] == "upstream_warn_test_failures" for x in r["reasons"])


def test_gate_warns_on_missing_contract_when_clean():
    models = [
        {"unique_id": "model.p.dim_acct", "schema": "marts", "alias": "dim_acct", "name": "dim_acct",
         "governance": {"contract_enforced": False}, "tests": [_passing()]},
        {"unique_id": "model.p.stg_acct", "schema": "staging", "alias": "stg_acct", "name": "stg_acct",
         "governance": {"contract_enforced": True}, "tests": [_passing()]},
    ]
    out = evaluate_syncs([_sync()], models=models, sources=[], lineage=_graph(),
                         warehouse_objects=_COVERAGE)
    r = out[0]["readiness"]
    assert r["verdict"] == "warn"
    assert any(x["code"] == "source_model_no_contract" for x in r["reasons"])


def test_gate_allows_when_clean_and_contracted():
    # allow requires positive evidence: tests exist, ran, and passed.
    models = [
        {"unique_id": "model.p.dim_acct", "schema": "marts", "alias": "dim_acct", "name": "dim_acct",
         "governance": {"contract_enforced": True}, "tests": [_passing()]},
        {"unique_id": "model.p.stg_acct", "schema": "staging", "alias": "stg_acct", "name": "stg_acct",
         "governance": {"contract_enforced": True}, "tests": [_passing()]},
    ]
    out = evaluate_syncs([_sync()], models=models, sources=[], lineage=_graph(),
                         warehouse_objects=_COVERAGE)
    r = out[0]["readiness"]
    assert r["verdict"] == "allow"
    assert r["upstream"]["tests_seen"] == 2
    assert r["upstream"]["tests_with_results"] == 2


def test_gate_unknown_when_source_not_matched():
    out = evaluate_syncs([_sync(schema="nope", table="missing")], models=[], sources=[], lineage=_graph())
    r = out[0]["readiness"]
    assert r["verdict"] == "unknown"
    assert r["reasons"][0]["code"] == "source_not_matched"


# -- fail-closed: absence of evidence is never "allow" ----------------------
def test_gate_warns_when_no_upstream_tests_exist():
    models = [
        {"unique_id": "model.p.dim_acct", "schema": "marts", "alias": "dim_acct", "name": "dim_acct",
         "governance": {"contract_enforced": True}, "tests": []},
        {"unique_id": "model.p.stg_acct", "schema": "staging", "alias": "stg_acct", "name": "stg_acct",
         "governance": {"contract_enforced": True}, "tests": []},
    ]
    out = evaluate_syncs([_sync()], models=models, sources=[], lineage=_graph(),
                         warehouse_objects=_COVERAGE)
    r = out[0]["readiness"]
    assert r["verdict"] == "warn"
    assert any(x["code"] == "no_upstream_tests" for x in r["reasons"])


def test_gate_warns_when_tests_have_no_run_results():
    # Tests defined but run_results missing -> every status is None. The gate
    # must not read "no evidence" as "clean".
    models = [
        {"unique_id": "model.p.dim_acct", "schema": "marts", "alias": "dim_acct", "name": "dim_acct",
         "governance": {"contract_enforced": True},
         "tests": [{"name": "unique_id", "status": None, "severity": "ERROR", "failures": None}]},
        {"unique_id": "model.p.stg_acct", "schema": "staging", "alias": "stg_acct", "name": "stg_acct",
         "governance": {"contract_enforced": True}, "tests": [_passing()]},
    ]
    models[1]["tests"][0]["status"] = None  # both tests lack results
    out = evaluate_syncs([_sync()], models=models, sources=[], lineage=_graph(),
                         warehouse_objects=_COVERAGE)
    r = out[0]["readiness"]
    assert r["verdict"] == "warn"
    assert any(x["code"] == "no_test_results" for x in r["reasons"])
    assert r["upstream"]["tests_with_results"] == 0


def test_gate_warns_when_fivetran_coverage_data_absent():
    # A dbt-only build (include_fivetran=False) cannot run the stale/unmatched
    # checks; the gate must say so instead of silently allowing.
    models = [
        {"unique_id": "model.p.dim_acct", "schema": "marts", "alias": "dim_acct", "name": "dim_acct",
         "governance": {"contract_enforced": True}, "tests": [_passing()]},
        {"unique_id": "model.p.stg_acct", "schema": "staging", "alias": "stg_acct", "name": "stg_acct",
         "governance": {"contract_enforced": True}, "tests": [_passing()]},
    ]
    out = evaluate_syncs([_sync()], models=models, sources=[], lineage=_graph())
    r = out[0]["readiness"]
    assert r["verdict"] == "warn"
    assert any(x["code"] == "coverage_checks_skipped" for x in r["reasons"])


def test_gate_warn_status_with_missing_failures_count_fires():
    # Older artifacts can omit `failures`; a warn STATUS means the test fired,
    # so fail closed rather than assume zero failing rows.
    models = [
        {"unique_id": "model.p.dim_acct", "schema": "marts", "alias": "dim_acct", "name": "dim_acct",
         "governance": {"contract_enforced": True},
         "tests": [{"name": "u", "status": "warn", "severity": "WARN", "failures": None}]},
        {"unique_id": "model.p.stg_acct", "schema": "staging", "alias": "stg_acct", "name": "stg_acct",
         "governance": {"contract_enforced": True}, "tests": [_passing()]},
    ]
    out = evaluate_syncs([_sync()], models=models, sources=[], lineage=_graph(),
                         warehouse_objects=_COVERAGE)
    assert out[0]["readiness"]["verdict"] == "block"


# -- client pagination (mocked transport, no live calls) --------------------
def _paged_client(pages: dict[int, dict]) -> "ActivationsClient":
    import httpx

    from metadata_service.clients import ActivationsClient
    from metadata_service.config import Settings

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("Authorization") == "Bearer tok"
        page = int(request.url.params.get("page", "1"))
        return httpx.Response(200, json=pages[page])

    http = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://census.test",
                        headers={"Authorization": "Bearer tok"})
    return ActivationsClient(Settings(activations_api_token="tok"), client=http)


def test_client_walks_every_page():
    # Census default per_page is 25; page-1-only fetching silently truncates.
    pages = {
        1: {"data": [{"id": i} for i in range(1, 101)],
            "pagination": {"page": 1, "next_page": 2}},
        2: {"data": [{"id": i} for i in range(101, 131)],
            "pagination": {"page": 2, "next_page": None}},
    }
    syncs = _paged_client(pages).list_syncs()
    assert len(syncs) == 130
    assert syncs[0]["id"] == 1 and syncs[-1]["id"] == 130


def test_client_stops_on_empty_page_even_if_next_page_lies():
    pages = {
        1: {"data": [{"id": 1}], "pagination": {"page": 1, "next_page": 2}},
        2: {"data": [], "pagination": {"page": 2, "next_page": 3}},
    }
    assert _paged_client(pages).list_syncs() == [{"id": 1}]


def test_extractor_records_truncation_in_errors():
    from metadata_service.extractors.activations_extractor import ActivationsExtractor

    pages = {1: {"data": [
        {"id": i, "source_attributes": {"object": {"table_catalog": "DB"}}} for i in range(1, 6)
    ], "pagination": {"page": 1, "next_page": None}}}
    client = _paged_client(pages)
    # sources/destinations share the handler; their dicts lack "name"/"type" — fine.
    raw = ActivationsExtractor(client).extract(source_database="DB", max_syncs=3)
    assert len(raw["syncs"]) == 3
    trunc = [e for e in raw["errors"] if e.get("error_type") == "Truncated"]
    assert trunc and "NOT evaluated" in trunc[0]["error_message"]


# -- unmodeled activated data raises a risk ---------------------------------
def test_unknown_verdict_sync_raises_activates_unverified_data(settings, fivetran_normalized, dbt_normalized):
    from metadata_service.normalizers import CombinedNormalizer

    activations_norm = {
        "extracted_at": "2026-06-30T00:00:00Z",
        "syncs": [{
            "sync_id": 42, "label": "mystery table -> sf", "status": "Ready", "paused": False,
            "operation": "upsert", "source_connection_id": "wh1", "source_name": "snowflake",
            "source_object": {"table_catalog": "DB", "table_schema": "raw", "table_name": "unmodeled"},
            "destination_connection_id": "sf1", "destination_name": "salesforce_prod",
            "destination_type": "salesforce", "destination_object": "Contact",
            "mappings": [], "last_synced_at": None,
        }],
        "errors": [],
    }
    doc = CombinedNormalizer(settings).build(fivetran_normalized, dbt_normalized, activations_norm)
    sync = doc["activations"]["syncs"][0]
    assert sync["readiness"]["verdict"] == "unknown"
    risks = [r for r in doc["dq_recommendations"] if r.get("risk") == "activates_unverified_data"]
    assert len(risks) == 1
    assert risks[0]["target"] == {"schema": "raw", "table": "unmodeled"}
    assert risks[0]["details"]["sync_id"] == 42


# -- combined integration (via fixtures) ----------------------------------
@pytest.fixture()
def doc():
    return build_metadata(Settings(warehouse_type="warehouse", stale_sync_threshold_hours=24),
                          fixtures_dir=str(FIXTURES))


def test_combined_attaches_activation_and_blocks(doc):
    act = doc["activations"]
    assert act["summary"] == {"total": 1, "by_verdict": {"block": 1}}
    sync = act["syncs"][0]
    assert sync["readiness"]["verdict"] == "block"
    assert sync["readiness"]["source_node_unique_id"] == "model.demo.dim_account"

    account = next(o for o in doc["warehouse_objects"] if o["name"] == "account")
    assert [a["sync_id"] for a in account.get("activations", [])] == [900]
    assert account["dq_summary"]["risk_level"] == "high"


def test_combined_emits_activates_bad_data_risk(doc):
    risks = [r for r in doc["dq_recommendations"] if r.get("risk") == "activates_bad_data"]
    assert len(risks) == 1
    assert risks[0]["severity"] == "high"
    assert risks[0]["details"]["activations"][0]["destination_name"] == "salesforce_prod"


# -- MCP + REST surfaces ---------------------------------------------------
@pytest.fixture()
def seeded_settings(tmp_path):
    settings = Settings(metadata_storage_backend="local", metadata_local_path=str(tmp_path))
    build_and_store(settings, fixtures_dir=str(FIXTURES))
    return settings


def test_mcp_list_and_readiness(seeded_settings):
    listing = tools.list_activations(settings=seeded_settings)
    assert listing["count"] == 1
    assert listing["summary"]["by_verdict"] == {"block": 1}
    assert listing["activations"][0]["verdict"] == "block"

    blocked = tools.list_activations(verdict="block", settings=seeded_settings)
    assert blocked["count"] == 1
    assert tools.list_activations(verdict="allow", settings=seeded_settings)["count"] == 0

    detail = tools.get_activation_readiness(sync_id=900, settings=seeded_settings)
    assert detail["readiness"]["verdict"] == "block"
    by_label = tools.get_activation_readiness(label="dim_account -> Salesforce Account", settings=seeded_settings)
    assert by_label["sync_id"] == 900


def test_mcp_summary_and_impact_include_activations(seeded_settings):
    summary = tools.get_dq_summary(settings=seeded_settings)
    assert summary["activations"] == {"total": 1, "by_verdict": {"block": 1}}

    impact = tools.get_impact("salesforce", "account", settings=seeded_settings)
    assert [a["sync_id"] for a in impact["activations"]] == [900]


def test_column_impact_reaches_destination_field(tmp_path):
    # Synthetic doc: a source column feeds the activation source model's column,
    # which is mapped to a Salesforce field. Exercises the activation_fields branch.
    from metadata_service.storage.local_storage import LocalStorage

    doc = {
        "generated_at": "2026-06-30T00:00:00Z",
        "version": "1.0",
        "sources": {"dbt": {"metrics": [], "exposures": [], "column_lineage_edges": [
            {"from_unique_id": "source.d.raw.churn_src", "from_column": "customer_email",
             "to_unique_id": "model.d.customer_churn", "to_column": "email"},
        ]}},
        "warehouse_objects": [{
            "object_id": "warehouse://d/raw/churn_src", "schema": "raw", "name": "churn_src",
            "dbt": {"source_unique_id": "source.d.raw.churn_src", "model_unique_ids": ["model.d.customer_churn"]},
            "columns": [], "dq_summary": {},
        }],
        "activations": {"syncs": [{
            "sync_id": 900, "label": "churn", "destination_name": "sf", "destination_object": "Contact",
            "mappings": [{"source_column": "email", "destination_field": "Email", "is_primary_identifier": True}],
            "readiness": {"verdict": "block", "source_node_unique_id": "model.d.customer_churn"},
        }]},
        "dq_recommendations": [], "metric_quality": [], "schema_drift": [], "errors": [],
    }
    settings = Settings(metadata_storage_backend="local", metadata_local_path=str(tmp_path))
    LocalStorage(str(tmp_path)).write_snapshot(doc)

    ci = tools.get_column_impact("raw", "churn_src", "customer_email", settings=settings)
    assert {a["unique_id"] for a in ci["affected_columns"]} == {"model.d.customer_churn"}
    assert ci["activation_fields"] == [{
        "sync_id": 900, "destination_name": "sf", "destination_object": "Contact",
        "destination_field": "Email", "source_column": "email", "readiness_verdict": "block",
    }]


def test_rest_activation_endpoints(tmp_path, monkeypatch):
    settings = Settings(metadata_storage_backend="local", metadata_local_path=str(tmp_path))
    monkeypatch.setattr("metadata_service.api.routes.get_settings", lambda: settings)
    build_and_store(settings, fixtures_dir=str(FIXTURES))

    from metadata_service.api.main import create_app

    client = TestClient(create_app())
    acts = client.get("/metadata/activations")
    assert acts.status_code == 200
    assert acts.json()["count"] == 1

    readiness = client.get("/dq/activation-readiness", params={"sync_id": "900"})
    assert readiness.status_code == 200
    assert readiness.json()["readiness"]["verdict"] == "block"

    assert client.get("/dq/activation-readiness", params={"sync_id": "404"}).status_code == 404
