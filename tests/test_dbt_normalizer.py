"""Tests for dbt manifest parsing, test extraction, and freshness."""

from __future__ import annotations


def _by_uid(items):
    return {i["unique_id"]: i for i in items}


def test_models_parsed(dbt_normalized):
    models = _by_uid(dbt_normalized["models"])
    assert "model.demo.stg_salesforce__account" in models
    stg = models["model.demo.stg_salesforce__account"]
    assert stg["schema"] == "salesforce"
    assert stg["materialized"] == "view"
    assert stg["depends_on"] == ["source.demo.salesforce.account"]
    assert stg["latest_status"] == "success"
    # catalog enrichment adds data types
    cols = {c["name"]: c for c in stg["columns"]}
    assert cols["id"]["data_type"] == "NUMBER"
    assert cols["id"]["description"] == "Salesforce account ID"


def test_sources_and_freshness(dbt_normalized):
    sources = _by_uid(dbt_normalized["sources"])
    src = sources["source.demo.salesforce.account"]
    assert src["schema"] == "salesforce"
    assert src["identifier"] == "account"
    assert src["freshness"] is not None
    assert src["freshness_result"]["status"] == "pass"
    assert src["freshness_result"]["max_loaded_at"] == "2026-06-25T12:15:00Z"


def test_source_columns_get_data_type_from_catalog(dbt_normalized):
    """Source columns carry warehouse data types even though the manifest
    declares none — the catalog's ``sources`` node is the only place they live."""
    src = _by_uid(dbt_normalized["sources"])["source.demo.salesforce.account"]
    cols = {c["name"]: c for c in src["columns"]}
    assert cols["id"]["data_type"] == "NUMBER"
    assert cols["email"]["data_type"] == "VARCHAR"
    assert cols["id"]["description"] == "Account id from Salesforce"


def test_undocumented_catalog_columns_are_included():
    """A column present only in the catalog still surfaces, so raw sources with
    no per-column docs (the norm for Fivetran-loaded tables) are not empty."""
    from metadata_service.normalizers.dbt_normalizer import DbtNormalizer

    out = DbtNormalizer().normalize({
        "artifacts": {
            "manifest": {"sources": {"source.d.s.t": {
                "resource_type": "source", "name": "t", "source_name": "s",
                "columns": {"documented": {"description": "d"}},
            }}},
            "catalog": {"sources": {"source.d.s.t": {"columns": {
                "DOCUMENTED": {"type": "NUMBER"},
                "UNDOCUMENTED": {"type": "VARCHAR", "comment": "from warehouse"},
            }}}},
        }
    })
    cols = {c["name"]: c for c in out["sources"][0]["columns"]}
    # case-insensitive merge: no duplicate entry for the documented column
    assert set(cols) == {"documented", "UNDOCUMENTED"}
    assert cols["documented"]["data_type"] == "NUMBER"
    assert cols["UNDOCUMENTED"]["data_type"] == "VARCHAR"
    assert cols["UNDOCUMENTED"]["description"] == "from warehouse"


def test_tests_extracted_with_type_and_status(dbt_normalized):
    tests = _by_uid(dbt_normalized["tests"])
    nn = tests["test.demo.not_null_stg_salesforce__account_id"]
    assert nn["test_type"] == "not_null"
    assert nn["attached_node"] == "model.demo.stg_salesforce__account"
    assert nn["attached_column"] == "id"
    assert nn["latest_status"] == "pass"

    # Real dbt: a firing warn-severity test has status "warn" (not "fail"),
    # and manifests carry uppercase severities.
    firing = tests["test.demo.accepted_values_stg_salesforce__account_status"]
    assert firing["test_type"] == "accepted_values"
    assert firing["latest_status"] == "warn"
    assert firing["failures"] == 3
    assert firing["severity"] == "WARN"


def test_tests_attached_to_models(dbt_normalized):
    models = _by_uid(dbt_normalized["models"])
    stg_tests = models["model.demo.stg_salesforce__account"]["tests"]
    types = {t["test_type"] for t in stg_tests}
    assert {"not_null", "unique", "accepted_values"} <= types


def test_lineage_edges(dbt_normalized):
    edges = {(e["from_unique_id"], e["to_unique_id"]): e["edge_type"] for e in dbt_normalized["lineage_edges"]}
    assert edges[("source.demo.salesforce.account", "model.demo.stg_salesforce__account")] == "source->model"
    assert edges[("model.demo.stg_salesforce__account", "model.demo.dim_account")] == "model->model"
    assert edges[("model.demo.dim_account", "exposure.demo.account_dashboard")] == "model->exposure"


def test_defensive_parsing_tolerates_missing_artifacts():
    from metadata_service.normalizers import DbtNormalizer

    out = DbtNormalizer().normalize({"artifacts": {}})
    assert out["models"] == []
    assert out["sources"] == []
    assert out["tests"] == []
    assert out["lineage_edges"] == []
