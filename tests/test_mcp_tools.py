"""Tests for the agent-facing MCP tools (discovery, summary, rec filtering)."""

from __future__ import annotations

import pytest

from metadata_service.config import Settings
from metadata_service.mcp import tools
from metadata_service.pipeline import build_and_store

from .conftest import FIXTURES


@pytest.fixture()
def seeded_settings(tmp_path):
    """Build a real snapshot from fixtures into a temp local store."""
    settings = Settings(metadata_storage_backend="local", metadata_local_path=str(tmp_path))
    build_and_store(settings, fixtures_dir=str(FIXTURES))
    return settings


def test_get_dq_summary(seeded_settings):
    s = tools.get_dq_summary(settings=seeded_settings)
    assert s["object_count"] == 2
    assert s["matched"] == 1            # salesforce.account
    assert s["unmatched"] == 1          # salesforce.contact
    assert s["objects_missing_dbt_coverage"] == 1
    assert s["objects_with_failing_tests"] == 1   # account has a failing accepted_values test
    assert s["recommendations"]["total"] > 0
    assert "missing_dbt_coverage" in s["recommendations"]["by_risk"]


def test_list_warehouse_objects_compact_and_filterable(seeded_settings):
    full = tools.list_warehouse_objects(settings=seeded_settings)
    assert full["count"] == 2
    # rows are compact: no heavy columns/tests payload
    assert "columns" not in full["objects"][0]
    assert set(full["objects"][0]) >= {"object_id", "risk_level", "has_dbt_coverage"}

    missing = tools.list_warehouse_objects(missing_coverage=True, settings=seeded_settings)
    assert missing["count"] == 1
    assert missing["objects"][0]["name"] == "contact"
    assert missing["objects"][0]["has_dbt_coverage"] is False

    # failing_tests is a deterministic discriminator (account has a failing test).
    failing = tools.list_warehouse_objects(failing_tests=True, settings=seeded_settings)
    assert [o["name"] for o in failing["objects"]] == ["account"]


def test_get_dq_recommendations_cross_object_filters(seeded_settings):
    risks = tools.get_dq_recommendations(recommendation_type="risk", settings=seeded_settings)
    assert risks["count"] >= 1
    assert all(r["recommendation_type"] == "risk" for r in risks["recommendations"])

    missing = tools.get_dq_recommendations(risk="missing_dbt_coverage", settings=seeded_settings)
    assert missing["count"] == 1
    assert missing["recommendations"][0]["target"]["table"] == "contact"

    # per-object still works, and limit caps the returned list
    capped = tools.get_dq_recommendations(schema="salesforce", table="account", limit=1, settings=seeded_settings)
    assert capped["returned"] == 1 and capped["count"] >= 1


def test_get_warehouse_object_detail_still_works(seeded_settings):
    o = tools.get_warehouse_object("salesforce", "account", settings=seeded_settings)
    assert o["name"] == "account"
    assert o["dbt"]["source_unique_id"]
