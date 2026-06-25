"""Tests for Fivetran schema + column normalization."""

from __future__ import annotations


def _account_table(fivetran_normalized):
    conn = fivetran_normalized["connections"][0]
    for table in conn["tables"]:
        if table["source_table"] == "Account":
            return table
    raise AssertionError("Account table not found")


def test_connection_normalized(fivetran_normalized):
    conns = fivetran_normalized["connections"]
    assert len(conns) == 1
    conn = conns[0]
    assert conn["connection_id"] == "conn_sf_001"
    assert conn["connector_service"] == "salesforce"
    assert conn["setup_state"] == "connected"
    assert conn["sync_state"] == "scheduled"
    assert conn["last_successful_sync"] == "2026-06-25T12:34:56Z"
    assert conn["schema_change_handling"] == "ALLOW_ALL"


def test_tables_preserve_source_and_destination_names(fivetran_normalized):
    table = _account_table(fivetran_normalized)
    assert table["source_schema"] == "salesforce"
    assert table["source_table"] == "Account"
    assert table["destination_schema"] == "salesforce"
    assert table["destination_table"] == "account"
    assert table["enabled"] is True


def test_columns_merge_schema_config_and_columns_endpoint(fivetran_normalized):
    table = _account_table(fivetran_normalized)
    cols = {c["source_name"]: c for c in table["columns"]}

    # Every schema-config column survives.
    assert set(cols) == {"Id", "Name", "Email", "Status", "OwnerId"}

    # is_primary_key / hashed come from the columns endpoint and override defaults.
    assert cols["Id"]["is_primary_key"] is True
    assert cols["Id"]["destination_name"] == "id"
    assert cols["Email"]["hashed"] is True

    # Columns absent from the endpoint default to non-PK / non-hashed.
    assert cols["Name"]["is_primary_key"] is False
    assert cols["Name"]["hashed"] is False


def test_errors_passthrough():
    from metadata_service.normalizers import FivetranNormalizer

    raw = {"extracted_at": "t", "connections": [], "errors": [{"error_type": "X"}]}
    out = FivetranNormalizer().normalize(raw)
    assert out["errors"] == [{"error_type": "X"}]
