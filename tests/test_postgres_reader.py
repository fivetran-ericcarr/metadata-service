"""Tests for the Postgres warehouse reader and the shared fivetran_metadata query.

All fixtures are local; nothing here opens a real database connection.
"""

from __future__ import annotations

import pytest

from metadata_service.config import Settings
from metadata_service.exceptions import MetadataServiceError
from metadata_service.warehouse import (
    build_primary_key_sql,
    describe_destination,
    get_warehouse_reader,
    rows_to_pk_map,
)

# A realistic Postgres destination payload: non-secret coordinates in cleartext
# (host/port/database/user), the password masked by Fivetran's "******" sentinel.
POSTGRES_DESTINATION = {
    "id": "capgemini_test_group",
    "group_id": "capgemini_test_group",
    "service": "postgres_warehouse",
    "region": "AWS_US_EAST_1",
    "setup_status": "connected",
    "config": {
        "host": "capgemini-test.cxxxxxxxxx.us-east-1.rds.amazonaws.com",
        "port": "5432",
        "database": "dq_test",
        "user": "fivetran_dq",
        "password": "******",
    },
}


def _creds(**overrides) -> Settings:
    base = dict(warehouse_user="u", warehouse_password="p")
    base.update(overrides)
    return Settings(**base)


# -- destination parsing ---------------------------------------------------
def test_describe_postgres_destination_resolves_dialect_and_port():
    info = describe_destination(POSTGRES_DESTINATION)
    assert info.service == "postgres_warehouse"
    assert info.warehouse_type == "postgres"
    assert info.database == "dq_test"
    assert info.host == "capgemini-test.cxxxxxxxxx.us-east-1.rds.amazonaws.com"
    assert info.port == 5432
    assert info.supports_information_schema() is True


def test_port_is_none_when_absent_or_non_numeric():
    assert describe_destination({"id": "d", "service": "postgres_warehouse", "config": {}}).port is None
    info = describe_destination(
        {"id": "d", "service": "postgres_warehouse", "config": {"port": "not-a-number"}}
    )
    assert info.port is None


# -- factory -----------------------------------------------------------------
def test_factory_auto_detects_postgres_without_explicit_settings():
    """Neither WAREHOUSE_HOST/DATABASE nor WAREHOUSE_TYPE is set: all three come
    from the destination, while the credential still has to be supplied."""
    info = describe_destination(POSTGRES_DESTINATION)
    reader = get_warehouse_reader(_creds(), info)
    assert reader is not None
    assert hasattr(reader, "read_column_schema")
    assert reader._database == "dq_test"
    assert reader._host == "capgemini-test.cxxxxxxxxx.us-east-1.rds.amazonaws.com"
    assert reader._port == 5432


def test_factory_prefers_explicit_settings_over_destination():
    info = describe_destination(POSTGRES_DESTINATION)
    reader = get_warehouse_reader(
        _creds(warehouse_host="override-host", warehouse_database="OTHER", warehouse_port=5433),
        info,
    )
    assert reader._host == "override-host"
    assert reader._database == "OTHER"
    assert reader._port == 5433


def test_factory_defaults_to_5432_when_destination_has_no_port():
    info = describe_destination(
        {"id": "d", "service": "postgres_warehouse", "config": {"host": "h", "database": "db"}}
    )
    reader = get_warehouse_reader(_creds(), info)
    assert reader._port == 5432


def test_factory_declines_without_credentials():
    info = describe_destination(POSTGRES_DESTINATION)
    assert get_warehouse_reader(Settings(), info) is None


# -- constructor validation ---------------------------------------------------
def test_reader_rejects_missing_host_or_database():
    from metadata_service.warehouse.postgres_reader import PostgresMetadataReader

    info = describe_destination({"id": "d", "service": "postgres_warehouse", "config": {}})
    with pytest.raises(MetadataServiceError, match="host and database"):
        PostgresMetadataReader(_creds(), destination=info)


def test_reader_rejects_missing_password():
    """Unlike Snowflake, there is no key-pair auth path here — a Postgres
    warehouse destination must authenticate with a password."""
    from metadata_service.warehouse.postgres_reader import PostgresMetadataReader

    info = describe_destination(POSTGRES_DESTINATION)
    settings = Settings(warehouse_user="u", warehouse_private_key_path="/tmp/key.pem")
    with pytest.raises(MetadataServiceError, match="WAREHOUSE_PASSWORD"):
        PostgresMetadataReader(settings, destination=info)


def test_reader_rejects_a_non_identifier_metadata_schema():
    from metadata_service.warehouse.postgres_reader import PostgresMetadataReader

    info = describe_destination(POSTGRES_DESTINATION)
    settings = _creds(warehouse_metadata_schema="fivetran; drop table x")
    with pytest.raises(MetadataServiceError, match="WAREHOUSE_METADATA_SCHEMA"):
        PostgresMetadataReader(settings, destination=info)


# -- queries -------------------------------------------------------------
class _FakeCursor:
    def __init__(self, rows):
        self.rows, self.sql, self.params = rows, None, None

    def execute(self, sql, params=None):
        self.sql, self.params = sql, params

    def fetchall(self):
        return self.rows

    def close(self):
        pass


def _postgres_reader(rows, settings=None, destination=None):
    from metadata_service.warehouse.postgres_reader import PostgresMetadataReader

    info = destination if destination is not None else describe_destination(POSTGRES_DESTINATION)
    reader = PostgresMetadataReader(settings or _creds(), destination=info)
    cursor = _FakeCursor(rows)
    reader._conn = type("FakeConn", (), {"cursor": lambda self: cursor})()
    return reader, cursor


def test_read_column_schema_targets_unqualified_information_schema():
    """Postgres has no cross-database dot notation — unlike Snowflake, the query
    must NOT be database-qualified; the connection is already scoped by dbname."""
    reader, cursor = _postgres_reader([("public", "account", "id", "integer", None, "NO")])
    out = reader.read_column_schema(["public"])
    assert out[("public", "account", "id")] == {
        "data_type": "integer", "max_length": None, "nullable": False,
    }
    assert cursor.sql.strip().startswith("select")
    assert "from information_schema.COLUMNS" in cursor.sql
    assert "dq_test" not in cursor.sql  # no database prefix
    assert cursor.params == ["PUBLIC"]


def test_read_primary_keys_uses_the_metadata_schema_without_a_database_prefix():
    reader, cursor = _postgres_reader([("public", "account", "id")])
    pk_map = reader.read_primary_keys(["conn1"])
    assert pk_map[("public", "account")] == ["id"]
    assert "fivetran_metadata.SOURCE_COLUMN" in cursor.sql
    # unlike the Snowflake reader's "<database>.fivetran_metadata", Postgres
    # addresses the schema directly since the connection is already scoped
    assert "dq_test.fivetran_metadata" not in cursor.sql
    assert cursor.params == ["conn1"]


def test_read_primary_keys_respects_a_custom_metadata_schema():
    info = describe_destination(POSTGRES_DESTINATION)
    reader, cursor = _postgres_reader(
        [], settings=_creds(warehouse_metadata_schema="custom_schema"), destination=info
    )
    reader.read_primary_keys()
    assert "custom_schema.SOURCE_COLUMN" in cursor.sql


# -- shared fivetran_metadata query builder --------------------------------
def test_build_primary_key_sql_binds_connection_ids():
    sql, params = build_primary_key_sql("fivetran_metadata", ["c1", "c2"])
    assert "fivetran_metadata.SOURCE_COLUMN" in sql
    assert "in (%s, %s)" in sql
    assert params == ["c1", "c2"]


def test_build_primary_key_sql_omits_the_filter_without_connection_ids():
    sql, params = build_primary_key_sql("fivetran_metadata", None)
    assert "connection_id" not in sql
    assert params == []


def test_rows_to_pk_map_lowercases_keys_but_not_column_names():
    out = rows_to_pk_map([("GITHUB", "ISSUE", "ID"), ("github", "issue", "REPO_ID"),
                          (None, "orphan", "x")])
    assert out[("github", "issue")] == ["ID", "REPO_ID"]
    assert len(out) == 1  # case-insensitive merge onto one key
