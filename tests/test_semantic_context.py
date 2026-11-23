"""get_semantic_context / list_semantic_metrics / search+fetch over a small but
complete dbt project: Fivetran source -> staging (renames legacy columns) ->
mart, a semantic model with simple + ratio metrics, a warn test, and a DQ
quarantine register. Built through the real pipeline from local fixtures."""

from __future__ import annotations

import json
import shutil

import pytest

from metadata_service import pipeline
from metadata_service.config import Settings
from metadata_service.mcp import tools
from metadata_service.semantic import (
    list_semantic_metrics,
    normalize_overrides,
    search_documents,
    semantic_context,
)
from metadata_service.storage.base import get_storage

from .conftest import FIXTURES

SRC = "source.demo.salesforce.account"
STG = "model.demo.stg_account"
DIM = "model.demo.dim_account"
QUAR = "model.demo.dq_quarantine"

MANIFEST = {
    "metadata": {"dbt_version": "1.8.0"},
    "sources": {SRC: {
        "resource_type": "source", "source_name": "salesforce", "name": "account",
        "database": "analytics", "schema": "salesforce", "identifier": "account",
        "relation_name": "analytics.salesforce.account", "description": "Raw Salesforce accounts.",
        "columns": {"id": {"description": "Account id"}},
    }},
    "nodes": {
        STG: {
            "resource_type": "model", "name": "stg_account", "package_name": "demo",
            "database": "analytics", "schema": "staging", "alias": "stg_account",
            "relation_name": "analytics.staging.stg_account", "config": {"materialized": "view"},
            "description": "Staged accounts with readable column names.",
            "columns": {"account_id": {"description": "Account id"},
                        "account_status_code": {"description": "Lifecycle status"}},
            "depends_on": {"nodes": [SRC]},
            "compiled_code": "select id as account_id, status as account_status_code, "
                             "owner_id as owner_user_id from analytics.salesforce.account",
        },
        DIM: {
            "resource_type": "model", "name": "dim_account", "package_name": "demo",
            "database": "analytics", "schema": "marts", "alias": "dim_account",
            "relation_name": "analytics.marts.dim_account", "config": {"materialized": "table"},
            "description": "One row per Salesforce account.",
            "meta": {"grain": "one row per account", "owner": "revops", "synonyms": ["customers"]},
            "columns": {
                "account_id": {"description": "Surrogate account key"},
                "account_status_code": {"description": "Lifecycle status code",
                                        "meta": {"synonyms": ["account state"]}},
                "owner_user_id": {"description": "Owning user"},
                "is_active": {"description": "Status is Active"},
            },
            "depends_on": {"nodes": [STG]},
            "compiled_code": "select account_id, account_status_code, owner_user_id, "
                             "case when account_status_code = 'Active' and owner_user_id is not null "
                             "then true else false end as is_active from analytics.staging.stg_account",
        },
        QUAR: {
            "resource_type": "model", "name": "dq_quarantine", "package_name": "demo",
            "database": "analytics", "schema": "silver", "alias": "dq_quarantine",
            "relation_name": "analytics.silver.dq_quarantine", "config": {"materialized": "table"},
            "description": "Records that failed a DQ rule.", "columns": {},
            "depends_on": {"nodes": [STG]},
        },
        "test.demo.accepted_values_stg_account_account_status_code": {
            "resource_type": "test", "name": "accepted_values_stg_account_account_status_code",
            "attached_node": STG, "column_name": "account_status_code",
            "config": {"severity": "warn"}, "test_metadata": {"name": "accepted_values"},
            "depends_on": {"nodes": [STG]},
        },
        "test.demo.unique_dim_account_account_id": {
            "resource_type": "test", "name": "unique_dim_account_account_id",
            "attached_node": DIM, "column_name": "account_id",
            "config": {"severity": "error"}, "test_metadata": {"name": "unique"},
            "depends_on": {"nodes": [DIM]},
        },
    },
    "exposures": {},
    "semantic_models": {"semantic_model.demo.accounts": {
        "name": "accounts", "description": "Accounts semantic model",
        "depends_on": {"nodes": [DIM]},
        "node_relation": {"relation_name": "analytics.marts.dim_account"},
        "defaults": {"agg_time_dimension": "created_date"},
        "entities": [{"name": "account", "type": "primary", "expr": "account_id"},
                     {"name": "owner", "type": "foreign", "expr": "owner_user_id"}],
        "dimensions": [
            {"name": "account_status_code", "type": "categorical"},
            {"name": "created_date", "type": "time", "type_params": {"time_granularity": "day"}},
        ],
        "measures": [
            {"name": "account_count", "agg": "count", "expr": "account_id"},
            {"name": "active_account_count", "agg": "sum",
             "expr": "case when is_active then 1 else 0 end"},
        ],
    }},
    "metrics": {
        "metric.demo.total_accounts": {
            "name": "total_accounts", "label": "Total Accounts", "type": "simple",
            "description": "Number of accounts.",
            "type_params": {"measure": {"name": "account_count"},
                            "input_measures": [{"name": "account_count"}]},
            "filter": None, "meta": {"synonyms": ["account count", "number of customers"]},
            "depends_on": {"nodes": ["semantic_model.demo.accounts"]},
        },
        "metric.demo.active_accounts": {
            "name": "active_accounts", "label": "Active Accounts", "type": "simple",
            "description": "Accounts that are active.",
            "type_params": {"measure": {"name": "active_account_count"}},
            "filter": {"where_filters": [{"where_sql_template": "{{ Dimension('account__account_status_code') }} != 'Test'"}]},
            "depends_on": {"nodes": ["semantic_model.demo.accounts"]},
        },
        "metric.demo.active_rate": {
            "name": "active_rate", "label": "Active Rate", "type": "ratio",
            "description": "Share of accounts that are active.",
            "type_params": {"numerator": {"name": "active_accounts"}, "denominator": {"name": "total_accounts"}},
            "depends_on": {"nodes": ["metric.demo.active_accounts", "metric.demo.total_accounts"]},
        },
    },
}

CATALOG = {
    "nodes": {
        STG: {"columns": {"account_id": {"type": "VARCHAR"}, "account_status_code": {"type": "VARCHAR"},
                          "owner_user_id": {"type": "VARCHAR"}}},
        DIM: {"columns": {"account_id": {"type": "VARCHAR"}, "account_status_code": {"type": "VARCHAR"},
                          "owner_user_id": {"type": "VARCHAR"}, "is_active": {"type": "BOOLEAN"}}},
        QUAR: {"columns": {"rule_id": {"type": "VARCHAR"}}},
    },
    "sources": {SRC: {"columns": {"id": {"type": "VARCHAR"}, "status": {"type": "VARCHAR"},
                                  "owner_id": {"type": "VARCHAR"}, "name": {"type": "VARCHAR"},
                                  "email": {"type": "VARCHAR"}}}},
}

RUN_RESULTS = {"args": {"which": "build"}, "results": [
    {"unique_id": "test.demo.accepted_values_stg_account_account_status_code", "status": "warn", "failures": 3},
    {"unique_id": "test.demo.unique_dim_account_account_id", "status": "pass", "failures": 0},
    {"unique_id": STG, "status": "success"}, {"unique_id": DIM, "status": "success"},
]}


@pytest.fixture(scope="module")
def doc(tmp_path_factory):
    pytest.importorskip("sqlglot")
    d = tmp_path_factory.mktemp("semantic_fixtures")
    for name in ("fivetran_connections.json", "fivetran_schema_config.json", "fivetran_columns.json",
                 "fivetran_destination.json"):
        shutil.copy(FIXTURES / name, d / name)
    (d / "dbt_manifest.json").write_text(json.dumps(MANIFEST))
    (d / "dbt_catalog.json").write_text(json.dumps(CATALOG))
    (d / "dbt_run_results.json").write_text(json.dumps(RUN_RESULTS))
    (d / "dbt_sources.json").write_text("{}")
    return pipeline.build_metadata(Settings(stale_sync_threshold_hours=24), fixtures_dir=str(d))


OVERRIDES = normalize_overrides({
    "synonyms": {"account_status_code": ["status"]},
    "metric_synonyms": {"active_rate": ["activation ratio"]},
    "metric_notes": {"active_rate": "Test accounts are excluded."},
    "dq_rules": [{"id": "BAD_STATUS", "pattern": r"accepted_values_stg_account_account_status_code",
                  "short": "{n} accounts carry an unknown status code"}],
    "guidance": ["Prefer governed metrics."],
})


def test_normalizer_keeps_semantic_definitions(doc):
    dbt = doc["sources"]["dbt"]
    m = {x["name"]: x for x in dbt["metrics"]}
    assert m["active_rate"]["type_params"]["numerator"] == {"name": "active_accounts"}
    assert m["active_accounts"]["filter"] == ["{{ Dimension('account__account_status_code') }} != 'Test'"]
    assert m["total_accounts"]["meta"]["synonyms"] == ["account count", "number of customers"]
    sm = dbt["semantic_models"][0]
    assert sm["measures"] == ["account_count", "active_account_count"]  # unchanged shape
    assert sm["measure_details"][0] == {"name": "account_count", "agg": "count", "expr": "account_id",
                                        "label": None, "description": None, "agg_time_dimension": None,
                                        "non_additive_dimension": None}
    assert sm["agg_time_dimension"] == "created_date"


def test_metric_context_ratio_definition_and_trust(doc):
    r = semantic_context(doc, "active_rate", overrides=OVERRIDES)
    assert r["resolved_as"] == "metric"
    s = r["subjects"][0]
    num, den = s["definition"]["measures"]
    assert num["role"] == "numerator" and num["metric"] == "active_accounts"
    assert num["filter"] == ["{{ Dimension('account__account_status_code') }} != 'Test'"]
    assert num["measures"][0]["expr"] == "case when is_active then 1 else 0 end"
    assert den["metric"] == "total_accounts" and den["measures"][0]["agg"] == "count"
    assert s["definition"]["time_grain"] == "day"
    assert {d["group_by"] for d in s["dimensions"]} == {"account__account_status_code", "account__created_date"}
    assert "Test accounts are excluded." in s["description"]
    assert s["synonyms"] == ["activation ratio"]
    assert s["locations"][0]["fqn"] == "analytics.marts.dim_account"
    # Upstream warn test -> curated caveat; quarantine register surfaced.
    caveats = {c["id"]: c for c in r["trust"]["caveats"]}
    assert caveats["BAD_STATUS"]["message"] == "3 accounts carry an unknown status code"
    assert caveats["BAD_STATUS"]["kind"] == "warn_test"
    assert "QUARANTINE:dq_quarantine" in caveats
    assert r["trust"]["level"] in ("watch", "at_risk")
    assert r["guidance"] == ["Prefer governed metrics."]


def test_legacy_source_column_synonyms_from_lineage(doc):
    r = semantic_context(doc, "dim_account", overrides=OVERRIDES, max_columns=10)
    s = r["subjects"][0]
    assert s["kind"] == "model" and s["grain"] == "one row per account" and s["owner"] == "revops"
    cols = {c["name"]: c for c in s["columns"]}
    status = cols["account_status_code"]
    # dbt meta + overrides + the Fivetran source column it renames.
    assert status["synonyms"][:2] == ["account state", "status"]
    # The Fivetran source system's own spelling of the column.
    assert status["source_columns"][0]["column"] == "Status"
    assert status["source_columns"][0]["connection_id"] == "conn_sf_001"
    assert "OwnerId" in cols["owner_user_id"]["synonyms"]
    # is_active derives from two source columns: lineage listed, not a synonym.
    assert "synonyms" not in cols["is_active"]
    assert len(cols["is_active"]["source_columns"]) == 2
    prov = r["provenance"][0]
    assert prov["connection_id"] == "conn_sf_001" and prov["connector_service"] == "salesforce"
    assert prov["data_mode"] == "fixtures" and prov["tables"][0]["destination"] == "salesforce.account"
    assert s["tests"]["by_status"] == {"pass": 1}


def test_topic_and_synonym_resolution(doc):
    r = semantic_context(doc, "how many customers do we have", overrides=OVERRIDES)
    assert r["subjects"][0]["name"] in ("total_accounts", "dim_account")
    r = semantic_context(doc, "activation ratio", overrides=OVERRIDES, top_k=1)
    assert r["subjects"][0]["name"] == "active_rate"
    # Legacy column name -> the model that renamed it.
    r = semantic_context(doc, "owner_id", kind="model", overrides=OVERRIDES)
    assert {s["name"] for s in r["subjects"]} & {"dim_account", "stg_account"}


def test_table_context_and_ids(doc):
    r = semantic_context(doc, "table:salesforce.account")
    s = r["subjects"][0]
    assert s["kind"] == "table" and s["dbt_source"] == SRC
    assert s["locations"][0]["fqn"] == "ANALYTICS.salesforce.account"
    assert "stg_account" in s["downstream_models"]
    assert r["provenance"][0]["connection_id"] == "conn_sf_001"


def test_disabled_table_caveat_and_lake_only_provenance(doc):
    import copy

    d = copy.deepcopy(doc)
    contact = next(o for o in d["warehouse_objects"] if o["name"] == "contact")
    contact["origin"]["enabled"] = False
    contact["dbt"]["source_unique_id"] = None  # nothing models it (e.g. a raw lake table)
    contact["lake"] = {"table_format": "iceberg", "last_updated": "2026-05-01T15:00:03Z",
                       "identifier_fields": ["ctid_fivetran_id"], "catalog_only_columns": ["ctid_fivetran_id"],
                       "catalogs": ["polaris"]}
    r = semantic_context(d, "salesforce.contact")
    s = r["subjects"][0]
    assert s["enabled_in_fivetran"] is False
    assert s["lake"]["identifier_fields"] == ["ctid_fivetran_id"]
    # Provenance comes from the object's own Fivetran origin, without dbt.
    assert r["provenance"][0]["connection_id"] == "conn_sf_001"
    assert r["provenance"][0]["disabled_tables"] == 1
    cav = next(c for c in r["trust"]["caveats"] if c["kind"] == "disabled_table")
    assert "lake data last written 2026-05-01T15:00:03Z" in cav["message"]
    assert r["trust"]["level"] == "watch"


def test_no_match_and_payload_bounds(doc):
    r = semantic_context(doc, "zzz qqq")
    assert r["subjects"] == [] and "message" in r and r["trust"]["level"] == "unknown"
    r = semantic_context(doc, "dim_account", max_columns=1)
    assert len(r["subjects"][0]["columns"]) == 1 and r["subjects"][0]["columns_truncated"] == 3
    assert len(json.dumps(semantic_context(doc, "active_rate"))) < 20_000


def test_list_semantic_metrics_pagination_and_filters(doc):
    r = list_semantic_metrics(doc, limit=2)
    assert r["count"] == 3 and r["returned"] == 2 and r["next_offset"] == 2
    rows = {m["name"]: m for m in list_semantic_metrics(doc)["metrics"]}
    assert rows["active_rate"]["measures"] == ["active_accounts", "total_accounts"]
    assert rows["active_accounts"]["has_filter"] is True
    assert rows["total_accounts"]["synonyms"] == ["account count", "number of customers"]
    trusted = list_semantic_metrics(doc, trust_level="nope")
    assert trusted["count"] == 0
    assert list_semantic_metrics(doc, search="active")["metrics"][0]["name"].startswith("active")


def test_search_and_fetch_shapes(doc, tmp_path):
    results = search_documents(doc, "active rate", base_url="https://mcp.example.run.app")
    assert results[0]["id"] == "metric:active_rate"
    assert results[0]["url"] == "https://mcp.example.run.app/semantic/context?query=metric:active_rate"
    settings = Settings(metadata_storage_backend="local", metadata_local_path=str(tmp_path),
                        mcp_public_url="https://mcp.example.run.app/mcp")
    get_storage(settings).write_snapshot(doc)
    fetched = tools.fetch("metric:active_rate", settings=settings)
    assert set(fetched) == {"id", "title", "text", "url", "metadata"}
    assert fetched["title"] == "metric: active_rate"
    assert json.loads(fetched["text"])["subjects"][0]["type"] == "ratio"
    assert tools.search("active", settings=settings)["results"]
    assert "error" in tools.get_semantic_context("x", kind="bogus", settings=settings)
    assert "error" in tools.get_semantic_context("  ", settings=settings)


def test_rest_and_cli_surfaces(doc, tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from typer.testing import CliRunner

    from metadata_service import cli
    from metadata_service.api.main import app
    from metadata_service.config import get_settings

    monkeypatch.setenv("METADATA_LOCAL_PATH", str(tmp_path))
    monkeypatch.setenv("METADATA_STORAGE_BACKEND", "local")
    monkeypatch.delenv("METADATA_API_KEY", raising=False)
    get_settings.cache_clear()
    try:
        get_storage(get_settings()).write_snapshot(doc)
        client = TestClient(app)
        r = client.get("/semantic/context", params={"query": "active_rate"})
        assert r.status_code == 200 and r.json()["resolved_as"] == "metric"
        assert client.get("/semantic/context", params={"query": "x", "kind": "bad"}).status_code == 400
        assert client.get("/semantic/metrics", params={"limit": 1}).json()["returned"] == 1
        out = CliRunner().invoke(cli.app, ["semantic-context", "dim_account", "--max-columns", "2"])
        assert out.exit_code == 0, out.output
        assert json.loads(out.output)["subjects"][0]["name"] == "dim_account"
        out = CliRunner().invoke(cli.app, ["semantic-metrics", "--limit", "1"])
        assert out.exit_code == 0 and json.loads(out.output)["returned"] == 1
    finally:
        get_settings.cache_clear()
