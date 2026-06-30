"""Tests for warehouse PK enrichment (pure function + factory gating)."""

from __future__ import annotations

from metadata_service.config import Settings
from metadata_service.warehouse import apply_primary_keys, get_warehouse_reader


def _fivetran_norm():
    return {
        "connections": [{
            "connection_id": "c1",
            "tables": [{
                "destination_schema": "github", "destination_table": "issue",
                "columns": [
                    {"destination_name": "id", "is_primary_key": False, "key_constraint": None},
                    {"destination_name": "title", "is_primary_key": False, "key_constraint": None},
                ],
            }, {
                "destination_schema": "github", "destination_table": "branch_commit_relation",
                "columns": [
                    {"destination_name": "branch_name", "is_primary_key": False},
                    {"destination_name": "commit_sha", "is_primary_key": False},
                ],
            }],
        }],
    }


def test_apply_primary_keys_sets_flags_and_provenance():
    norm = _fivetran_norm()
    pk_map = {
        ("github", "issue"): ["id"],
        ("github", "branch_commit_relation"): ["branch_name", "commit_sha"],  # composite
    }
    updated = apply_primary_keys(norm, pk_map)
    assert updated == 3

    issue = norm["connections"][0]["tables"][0]
    id_col = next(c for c in issue["columns"] if c["destination_name"] == "id")
    assert id_col["is_primary_key"] is True
    assert id_col["key_constraint"] == "primary_key"
    assert id_col["key_source"] == "fivetran_platform"
    assert all(not c["is_primary_key"] for c in issue["columns"] if c["destination_name"] == "title")

    bcr = norm["connections"][0]["tables"][1]
    assert sum(c["is_primary_key"] for c in bcr["columns"]) == 2  # composite PK


def test_apply_primary_keys_case_insensitive_and_noop_on_miss():
    norm = _fivetran_norm()
    assert apply_primary_keys(norm, {("GITHUB", "ISSUE"): ["ID"]}) == 1   # case-insensitive
    assert apply_primary_keys(norm, {("github", "unknown"): ["x"]}) == 0  # no matching table


def test_reader_factory_disabled_by_default():
    # No WAREHOUSE_* configured -> reader is None (feature is opt-in).
    assert get_warehouse_reader(Settings(warehouse_type="warehouse")) is None
    # Snowflake type but missing account/db -> still None.
    assert get_warehouse_reader(Settings(warehouse_type="snowflake")) is None


def test_reader_factory_enabled_when_configured():
    s = Settings(
        warehouse_type="snowflake", warehouse_account="ab123", warehouse_database="DB",
        warehouse_user="u", warehouse_role="r", warehouse_name="wh",
        warehouse_password="p",
    )
    assert s.warehouse_reader_enabled() is True
    # Factory returns a reader object (constructed; no connection made yet).
    reader = get_warehouse_reader(s)
    assert reader is not None and hasattr(reader, "read_primary_keys")
