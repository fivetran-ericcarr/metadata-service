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
         "governance": {"contract_enforced": False}, "tests": []},
        {"unique_id": "model.p.stg_acct", "schema": "staging", "alias": "stg_acct", "name": "stg_acct",
         "governance": {"contract_enforced": True}, "tests": []},
    ]
    out = evaluate_syncs([_sync()], models=models, sources=[], lineage=_graph())
    r = out[0]["readiness"]
    assert r["verdict"] == "warn"
    assert any(x["code"] == "source_model_no_contract" for x in r["reasons"])


def test_gate_allows_when_clean_and_contracted():
    models = [
        {"unique_id": "model.p.dim_acct", "schema": "marts", "alias": "dim_acct", "name": "dim_acct",
         "governance": {"contract_enforced": True}, "tests": []},
        {"unique_id": "model.p.stg_acct", "schema": "staging", "alias": "stg_acct", "name": "stg_acct",
         "governance": {"contract_enforced": True}, "tests": []},
    ]
    out = evaluate_syncs([_sync()], models=models, sources=[], lineage=_graph())
    assert out[0]["readiness"]["verdict"] == "allow"


def test_gate_unknown_when_source_not_matched():
    out = evaluate_syncs([_sync(schema="nope", table="missing")], models=[], sources=[], lineage=_graph())
    r = out[0]["readiness"]
    assert r["verdict"] == "unknown"
    assert r["reasons"][0]["code"] == "source_not_matched"


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
