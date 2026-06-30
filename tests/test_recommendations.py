"""Tests for DQ recommendation generation."""

from __future__ import annotations

from datetime import datetime, timezone

from metadata_service.dq.recommendations import recommend_for_object

from .conftest import object_by_table


def _recs_for(doc, object_id):
    return [r for r in doc["dq_recommendations"] if r["object_id"] == object_id]


def test_primary_key_recommends_not_null_and_unique(built_doc):
    contact = object_by_table(built_doc, "salesforce", "contact")
    recs = _recs_for(built_doc, contact["object_id"])
    pk_tests = {(r["test_name"], r["target"].get("column")) for r in recs if r["recommendation_type"] == "dbt_test"}
    assert ("not_null", "id") in pk_tests
    assert ("unique", "id") in pk_tests
    for r in recs:
        if r.get("test_name") in {"not_null", "unique"} and r["target"].get("column") == "id":
            assert r["confidence"] == "high"
            assert r["source"] == "fivetran_metadata"


def test_covered_primary_key_is_not_recommended_again(built_doc):
    account = object_by_table(built_doc, "salesforce", "account")
    recs = _recs_for(built_doc, account["object_id"])
    # account.id already has not_null + unique in dbt, so no PK test recs for it
    pk_recs = [r for r in recs if r.get("test_name") in {"not_null", "unique"} and r["target"].get("column") == "id"]
    assert pk_recs == []


def test_relationship_heuristic_for_id_columns(built_doc):
    account = object_by_table(built_doc, "salesforce", "account")
    recs = _recs_for(built_doc, account["object_id"])
    rel = [r for r in recs if r.get("test_name") == "relationships" and r["target"].get("column") == "owner_id"]
    assert rel and rel[0]["confidence"] == "heuristic"


def test_hashed_column_signal(built_doc):
    account = object_by_table(built_doc, "salesforce", "account")
    recs = _recs_for(built_doc, account["object_id"])
    signals = [r for r in recs if r.get("signal") == "hashed_column" and r["target"].get("column") == "email"]
    assert signals
    assert "Verify" in signals[0]["recommended_action"]


def test_failing_tests_risk(built_doc):
    account = object_by_table(built_doc, "salesforce", "account")
    recs = _recs_for(built_doc, account["object_id"])
    assert any(r.get("risk") == "failing_dbt_tests" and r["severity"] == "high" for r in recs)


def test_accepted_values_heuristic_on_categorical_column():
    obj = {
        "object_id": "warehouse://unknown/s/t",
        "schema": "s",
        "name": "t",
        "origin": {"enabled": True},
        "match_confidence": "exact_schema_table",
        "dbt": {"source_unique_id": "source.x", "tests": [], "freshness": {"status": "pass"}},
        "columns": [
            {"name": "status", "source_name": "Status", "is_primary_key": False, "hashed": False, "dbt_tests": []}
        ],
    }
    recs = recommend_for_object(obj)
    av = [r for r in recs if r.get("test_name") == "accepted_values"]
    assert av and av[0]["confidence"] == "heuristic"


def _obj(columns, *, match="exact_schema_table", dbt=None):
    return {
        "object_id": "warehouse://unknown/s/t", "schema": "s", "name": "t",
        "origin": {"enabled": True}, "match_confidence": match,
        "dbt": dbt if dbt is not None else {"source_unique_id": "source.x", "tests": [], "freshness": {"status": "pass"}},
        "columns": columns,
    }


def test_potential_pii_signal_when_not_hashed():
    recs = recommend_for_object(_obj([
        {"name": "customer_email", "source_name": "Email", "hashed": False, "dbt_tests": []},
    ]))
    pii = [r for r in recs if r.get("signal") == "potential_pii"]
    assert pii and pii[0]["target"]["column"] == "customer_email"
    assert pii[0]["confidence"] == "heuristic"


def test_pii_not_flagged_when_hashed():
    recs = recommend_for_object(_obj([
        {"name": "ssn", "source_name": "SSN", "hashed": True, "dbt_tests": []},
    ]))
    assert not any(r.get("signal") == "potential_pii" for r in recs)
    assert any(r.get("signal") == "hashed_column" for r in recs)


def test_boolean_accepted_values():
    recs = recommend_for_object(_obj([
        {"name": "is_active", "source_name": "IsActive", "hashed": False, "dbt_tests": []},
    ]))
    av = [r for r in recs if r.get("test_name") == "accepted_values"]
    assert av and av[0]["target"].get("values") == [True, False]


def test_natural_key_uniqueness():
    recs = recommend_for_object(_obj([
        {"name": "email", "source_name": "Email", "hashed": False, "is_primary_key": False, "dbt_tests": []},
    ]))
    uniq = [r for r in recs if r.get("test_name") == "unique" and r["target"].get("column") == "email"]
    assert uniq and uniq[0]["confidence"] == "heuristic"


def test_untested_matched_object_risk():
    recs = recommend_for_object(_obj([], dbt={"source_unique_id": "source.x", "model_unique_ids": ["m"], "tests": []}))
    assert any(r.get("risk") == "untested_dbt_object" and r["severity"] == "medium" for r in recs)


def test_matched_object_with_tests_not_flagged_untested():
    dbt = {"source_unique_id": "source.x", "model_unique_ids": ["m"],
           "tests": [{"test_type": "not_null", "status": "success"}], "freshness": {"status": "pass"}}
    recs = recommend_for_object(_obj([], dbt=dbt))
    assert not any(r.get("risk") == "untested_dbt_object" for r in recs)


def test_governance_risks_missing_contract_and_unowned():
    obj = _obj([], dbt={
        "source_unique_id": "source.x", "model_unique_ids": ["m"], "tests": [{"status": "success"}],
        "freshness": {"status": "pass"},
        "governance": {"has_enforced_contract": False, "owners": [], "groups": [],
                       "uncontracted_public_models": ["m"]},
    })
    recs = recommend_for_object(obj)
    risks = {r["risk"]: r for r in recs if r["recommendation_type"] == "risk"}
    assert risks["missing_model_contract"]["severity"] == "high"  # public + no contract
    assert "unowned_object" in risks


def test_governed_object_has_no_governance_risk():
    obj = _obj([], dbt={
        "source_unique_id": "source.x", "model_unique_ids": ["m"], "tests": [{"status": "success"}],
        "freshness": {"status": "pass"},
        "governance": {"has_enforced_contract": True, "owners": ["Team"], "groups": ["analytics"],
                       "uncontracted_public_models": []},
    })
    risks = {r.get("risk") for r in recommend_for_object(obj)}
    assert "missing_model_contract" not in risks
    assert "unowned_object" not in risks


def test_stale_sync_risk():
    obj = {
        "object_id": "warehouse://unknown/s/t",
        "schema": "s",
        "name": "t",
        "match_confidence": "exact_schema_table",
        "origin": {"enabled": True, "last_successful_sync": "2026-06-20T00:00:00Z"},
        "dbt": {"source_unique_id": "source.x", "tests": [], "freshness": {"status": "pass"}},
        "columns": [],
    }
    now = datetime(2026, 6, 25, tzinfo=timezone.utc)
    recs = recommend_for_object(obj, stale_threshold_hours=24, now=now)
    assert any(r.get("risk") == "stale_fivetran_sync" and r["severity"] == "high" for r in recs)


def test_fresh_sync_not_flagged():
    obj = {
        "object_id": "warehouse://unknown/s/t",
        "schema": "s",
        "name": "t",
        "match_confidence": "exact_schema_table",
        "origin": {"enabled": True, "last_successful_sync": "2026-06-25T11:00:00Z"},
        "dbt": {"source_unique_id": "source.x", "tests": [], "freshness": {"status": "pass"}},
        "columns": [],
    }
    now = datetime(2026, 6, 25, 12, tzinfo=timezone.utc)
    recs = recommend_for_object(obj, stale_threshold_hours=24, now=now)
    assert not any(r.get("risk") == "stale_fivetran_sync" for r in recs)
