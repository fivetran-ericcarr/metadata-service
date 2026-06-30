"""Tests for combined matching, the warehouse object model, storage, and API."""

from __future__ import annotations

from fastapi.testclient import TestClient

from metadata_service.config import Settings
from metadata_service.pipeline import build_and_store, build_metadata
from metadata_service.storage.local_storage import LocalStorage

from .conftest import object_by_table


def test_matching_fivetran_table_to_dbt_source(built_doc):
    account = object_by_table(built_doc, "salesforce", "account")
    assert account["match_confidence"] == "exact_schema_table"
    assert account["dbt"]["source_unique_id"] == "source.demo.salesforce.account"
    # downstream models via lineage
    assert set(account["dbt"]["model_unique_ids"]) == {
        "model.demo.stg_salesforce__account",
        "model.demo.dim_account",
    }


def test_warehouse_object_columns_and_object_id(built_doc):
    account = object_by_table(built_doc, "salesforce", "account")
    assert account["object_id"] == "warehouse://unknown/salesforce/account"

    cols = {c["name"]: c for c in account["columns"]}
    assert cols["id"]["is_primary_key"] is True
    assert set(cols["id"]["dbt_tests"]) >= {"not_null", "unique"}
    assert cols["id"]["dbt_description"] == "Account id from Salesforce"
    assert cols["email"]["hashed"] is True


def test_dq_summary(built_doc):
    account = object_by_table(built_doc, "salesforce", "account")
    summary = account["dq_summary"]
    assert summary["has_primary_key"] is True
    assert summary["has_primary_key_tests"] is True
    assert summary["has_freshness_check"] is True
    assert summary["failing_tests_count"] == 1  # accepted_values failed
    assert summary["risk_level"] == "high"


def test_unmatched_table_flagged(built_doc):
    contact = object_by_table(built_doc, "salesforce", "contact")
    assert contact["match_confidence"] == "unmatched"
    assert contact["dbt"]["source_unique_id"] is None
    risks = [r for r in built_doc["dq_recommendations"]
             if r.get("risk") == "missing_dbt_coverage" and r["object_id"] == contact["object_id"]]
    assert risks and risks[0]["severity"] == "medium"


def test_exposures_attached_and_impact_risk(built_doc):
    account = object_by_table(built_doc, "salesforce", "account")
    exposures = account["dbt"]["exposures"]
    assert any(e["name"] == "account_dashboard" and e["type"] == "dashboard" for e in exposures)
    # account has a failing test AND feeds an exposure -> business-impact risk
    recs = [r for r in built_doc["dq_recommendations"]
            if r["object_id"] == account["object_id"] and r.get("risk") == "impacts_exposure"]
    assert recs and recs[0]["severity"] == "high"
    assert recs[0]["details"]["exposures"][0]["name"] == "account_dashboard"


def test_unmatched_object_has_no_exposures(built_doc):
    contact = object_by_table(built_doc, "salesforce", "contact")
    assert contact["dbt"]["exposures"] == []


def test_configured_alias_match(settings, fivetran_normalized, dbt_normalized):
    """An aliases map activates the configured_alias tier for non-matching names."""
    from metadata_service.normalizers import CombinedNormalizer

    aliases = {"salesforce.contact": "salesforce.account"}
    doc = CombinedNormalizer(settings, aliases=aliases).build(fivetran_normalized, dbt_normalized)
    contact = object_by_table(doc, "salesforce", "contact")
    assert contact["match_confidence"] == "configured_alias"
    assert contact["dbt"]["source_unique_id"] == "source.demo.salesforce.account"


def test_build_from_fixtures_conforms_to_contract(settings, fixtures_dir):
    doc = build_metadata(settings, fixtures_dir=str(fixtures_dir))
    assert doc["version"] == "1.0"
    for key in ("generated_at", "sources", "warehouse_objects", "dq_recommendations", "schema_drift", "errors"):
        assert key in doc
    assert {"fivetran", "dbt"} <= set(doc["sources"])
    assert len(doc["warehouse_objects"]) == 2

    # validate against the pydantic contract model
    from metadata_service.models.normalized import NormalizedMetadata

    NormalizedMetadata.model_validate(doc)


def test_local_storage_write_read_roundtrip(tmp_path, settings, fixtures_dir):
    storage = LocalStorage(str(tmp_path))
    doc = build_metadata(settings, fixtures_dir=str(fixtures_dir))
    uri = storage.write_snapshot(doc, snapshot_name="2026-06-25T12-00-00Z")
    assert uri.endswith(".json")

    latest = storage.read_latest()
    assert latest is not None
    assert latest["version"] == doc["version"]
    assert len(storage.list_snapshots()) == 1


def test_build_and_store_then_api(tmp_path, fixtures_dir, monkeypatch):
    # Point storage at a temp dir and seed a snapshot via the pipeline.
    test_settings = Settings(
        metadata_storage_backend="local",
        metadata_local_path=str(tmp_path),
    )
    monkeypatch.setattr("metadata_service.api.routes.get_settings", lambda: test_settings)

    build_and_store(test_settings, fixtures_dir=str(fixtures_dir))

    from metadata_service.api.main import create_app

    client = TestClient(create_app())

    assert client.get("/health").json() == {"status": "ok"}

    latest = client.get("/metadata/latest")
    assert latest.status_code == 200
    assert latest.json()["version"] == "1.0"

    objects = client.get("/metadata/warehouse-objects", params={"schema": "salesforce", "table": "account"})
    assert objects.json()["count"] == 1
