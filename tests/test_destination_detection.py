"""Tests for Fivetran destination detection and INFORMATION_SCHEMA enrichment.

All fixtures are local; nothing here touches a live API or a warehouse.
"""

from __future__ import annotations

import httpx
import pytest

from metadata_service.clients.fivetran_client import FivetranClient
from metadata_service.config import Settings
from metadata_service.warehouse import (
    apply_column_schema,
    build_column_schema_sql,
    describe_destination,
    destination_from_dict,
    get_warehouse_reader,
    resolve_warehouse_type,
    rows_to_column_map,
)

# A realistic Snowflake destination payload: non-secret coordinates in cleartext,
# every password-format field replaced by Fivetran's "******" sentinel.
SNOWFLAKE_DESTINATION = {
    "id": "bestowing_leaft",
    "group_id": "bestowing_leaft",
    "service": "snowflake",
    "region": "GCP_US_EAST4",
    "setup_status": "connected",
    "config": {
        "host": "A3209653506471-SALES_ENG_DEMO.snowflakecomputing.com",
        "database": "CENSUS",
        "user": "FIVETRAN_CENSUS_DEMO",
        "role": "CENSUS_DEMO_ROLE",
        "private_key": "******",
        "passphrase": "******",
    },
}


def _client(handler) -> FivetranClient:
    settings = Settings(fivetran_api_key="k", fivetran_api_secret="s")
    http = httpx.Client(transport=httpx.MockTransport(handler),
                        base_url="https://api.fivetran.com/v1")
    return FivetranClient(settings, client=http)


def _creds(**overrides) -> Settings:
    base = dict(warehouse_user="u", warehouse_password="p")
    base.update(overrides)
    return Settings(**base)


# -- describe_destination -------------------------------------------------
def test_describe_snowflake_destination():
    info = describe_destination(SNOWFLAKE_DESTINATION)
    assert info.service == "snowflake"
    assert info.warehouse_type == "snowflake"
    assert info.database == "CENSUS"
    assert info.host == "A3209653506471-SALES_ENG_DEMO.snowflakecomputing.com"
    assert info.region == "GCP_US_EAST4"
    assert info.supports_information_schema() is True


def test_describe_never_leaks_config_or_redacted_values():
    """as_dict() is what reaches a snapshot: it must carry no config at all, and
    no field may hold Fivetran's "******" masking sentinel."""
    payload = {"id": "d", "service": "snowflake",
               "config": {"database": "******", "host": "******", "password": "******"}}
    data = describe_destination(payload).as_dict()
    assert "config" not in data
    assert "******" not in [v for v in data.values() if isinstance(v, str)]
    # A redacted database is treated as absent, not carried through as "******".
    assert data["database"] is None


@pytest.mark.parametrize(
    "service,expected_type,config,expected_database",
    [
        ("big_query", "bigquery", {"project_id": "my-gcp-project"}, "my-gcp-project"),
        ("databricks", "databricks", {"catalog": "yds_aws_sandbox"}, "yds_aws_sandbox"),
        ("redshift", "redshift", {"database": "dev"}, "dev"),
        ("postgres_warehouse", "postgres", {"database": "wh"}, "wh"),
        ("azure_sql_managed_db_warehouse", "sql_server", {"database": "wh"}, "wh"),
        ("managed_data_lake", "managed_data_lake", {}, None),
    ],
)
def test_service_maps_to_dialect_and_container(service, expected_type, config, expected_database):
    """Each dialect keeps its container name under a different config key —
    BigQuery's is the GCP project, Databricks' is the Unity Catalog catalog."""
    info = describe_destination({"id": "d", "service": service, "config": config})
    assert info.warehouse_type == expected_type
    assert info.database == expected_database


def test_unknown_service_keeps_service_name_for_diagnosis():
    info = describe_destination({"id": "d", "service": "some_new_warehouse", "config": {}})
    assert info.service == "some_new_warehouse"
    assert info.warehouse_type is None
    assert info.supports_information_schema() is False


def test_describe_tolerates_empty_and_malformed_payloads():
    assert describe_destination(None) is None
    assert describe_destination({}) is None
    # config present but not a dict must not raise
    assert describe_destination({"id": "d", "service": "snowflake", "config": "nope"}).database is None


def test_destination_round_trips_through_dict():
    info = describe_destination(SNOWFLAKE_DESTINATION)
    assert destination_from_dict(info.as_dict()) == info
    assert destination_from_dict(None) is None


# -- client lookup --------------------------------------------------------
def test_find_destination_for_group_uses_direct_get():
    """Destination ids equal group ids in practice, so the common case is one
    request — no account-wide list walk."""
    paths = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return httpx.Response(200, json={"data": SNOWFLAKE_DESTINATION})

    dest = _client(handler).find_destination_for_group("bestowing_leaft")
    assert dest["service"] == "snowflake"
    assert paths == ["/v1/destinations/bestowing_leaft"]


def test_find_destination_falls_back_to_list_on_404():
    """Fivetran documents id and group_id as distinct and offers no group_id
    filter, so a miss on the direct GET must scan and re-fetch by id (the list
    payload carries no config)."""
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/destinations/grp1":
            return httpx.Response(404, json={"code": "NotFound"})
        if request.url.path == "/v1/destinations":
            return httpx.Response(200, json={"data": {"items": [
                {"id": "other", "group_id": "grp2", "service": "big_query"},
                {"id": "real", "group_id": "grp1", "service": "snowflake"},
            ]}})
        return httpx.Response(200, json={"data": SNOWFLAKE_DESTINATION})

    dest = _client(handler).find_destination_for_group("grp1")
    assert dest["config"]["database"] == "CENSUS"


def test_find_destination_refuses_to_guess_between_several():
    """A group with more than one destination has no single right answer;
    picking one would silently point the reader at the wrong database."""
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/destinations/grp1":
            return httpx.Response(404, json={})
        return httpx.Response(200, json={"data": {"items": [
            {"id": "a", "group_id": "grp1", "service": "snowflake"},
            {"id": "b", "group_id": "grp1", "service": "redshift"},
        ]}})

    assert _client(handler).find_destination_for_group("grp1") is None


def test_find_destination_returns_none_without_group():
    def handler(request):  # pragma: no cover - must never be called
        raise AssertionError("no request expected")

    assert _client(handler).find_destination_for_group("") is None


# -- dialect resolution + factory ----------------------------------------
def test_placeholder_warehouse_type_defers_to_detection():
    """WAREHOUSE_TYPE ships as the placeholder "warehouse"; an operator who never
    edited .env must get auto-detection, not a permanently disabled reader."""
    info = describe_destination(SNOWFLAKE_DESTINATION)
    assert resolve_warehouse_type(Settings(warehouse_type="warehouse"), info) == "snowflake"
    assert resolve_warehouse_type(Settings(warehouse_type="warehouse"), None) is None


def test_explicit_warehouse_type_overrides_detection():
    info = describe_destination({"id": "d", "service": "big_query", "config": {}})
    assert resolve_warehouse_type(Settings(warehouse_type="snowflake"), info) == "snowflake"


def test_factory_auto_detects_snowflake_without_explicit_settings():
    """Neither WAREHOUSE_TYPE nor WAREHOUSE_DATABASE is set: both come from the
    destination, while the credential still has to be supplied."""
    info = describe_destination(SNOWFLAKE_DESTINATION)
    reader = get_warehouse_reader(_creds(), info)
    assert reader is not None
    assert hasattr(reader, "read_column_schema")
    assert reader._database == "CENSUS"
    # account derived from the destination host, not guessed
    assert reader._account == "a3209653506471-sales_eng_demo"


def test_factory_prefers_explicit_settings_over_destination():
    info = describe_destination(SNOWFLAKE_DESTINATION)
    reader = get_warehouse_reader(_creds(warehouse_account="acct", warehouse_database="OTHER"), info)
    assert reader._database == "OTHER"
    assert reader._account == "acct"


def test_factory_declines_without_credentials():
    """Detection alone is not enough: Fivetran masks the credential, so a reader
    can never be built from the destination payload by itself."""
    info = describe_destination(SNOWFLAKE_DESTINATION)
    assert get_warehouse_reader(Settings(), info) is None


@pytest.mark.parametrize("service", ["managed_data_lake", "big_query", "databricks"])
def test_factory_declines_unsupported_dialects(service):
    """MDLS has no SQL information schema at all; BigQuery and Databricks have
    one but not an ANSI-compatible one. Neither may fall through to a reader."""
    info = describe_destination({"id": "d", "service": service, "config": {}})
    assert get_warehouse_reader(_creds(), info) is None


# -- ANSI query + row mapping --------------------------------------------
def test_column_schema_sql_binds_schemas_and_matches_case_insensitively():
    sql, params = build_column_schema_sql("DB.INFORMATION_SCHEMA", ["salesforce", "github"])
    assert "from DB.INFORMATION_SCHEMA.COLUMNS" in sql
    assert "CHARACTER_MAXIMUM_LENGTH" in sql and "IS_NULLABLE" in sql
    assert "upper(TABLE_SCHEMA) in (%s, %s)" in sql
    # schema names are bound, never interpolated, and upper-cased on both sides
    assert params == ["SALESFORCE", "GITHUB"]


def test_column_schema_sql_excludes_system_schemas_when_unscoped():
    sql, params = build_column_schema_sql("DB.INFORMATION_SCHEMA", None)
    assert "not in" in sql
    assert "INFORMATION_SCHEMA" in params and "PG_CATALOG" in params


def test_column_schema_sql_honors_driver_placeholder():
    sql, _ = build_column_schema_sql("DB.INFORMATION_SCHEMA", ["s"], placeholder="?")
    assert "in (?)" in sql and "%s" not in sql


def test_rows_to_column_map_normalizes_types():
    rows = [
        ("SALESFORCE", "ACCOUNT", "NAME", "TEXT", 255, "YES"),
        ("SALESFORCE", "ACCOUNT", "ID", "NUMBER", None, "NO"),
        ("SALESFORCE", "ACCOUNT", "FLAG", "BOOLEAN", "12", True),
        (None, "ACCOUNT", "ORPHAN", "TEXT", 1, "YES"),  # unkeyed row is dropped
    ]
    out = rows_to_column_map(rows)
    assert out[("salesforce", "account", "name")] == {
        "data_type": "TEXT", "max_length": 255, "nullable": True,
    }
    assert out[("salesforce", "account", "id")]["max_length"] is None
    assert out[("salesforce", "account", "id")]["nullable"] is False
    # string lengths coerce to int; a driver-native bool passes through
    assert out[("salesforce", "account", "flag")]["max_length"] == 12
    assert out[("salesforce", "account", "flag")]["nullable"] is True
    assert len(out) == 3


def test_rows_to_column_map_leaves_unrecognized_nullable_none():
    """Better an explicit None than guessing a nullability the warehouse did not
    state — a wrong `nullable` drives a wrong not_null recommendation."""
    out = rows_to_column_map([("s", "t", "c", "TEXT", None, "MAYBE")])
    assert out[("s", "t", "c")]["nullable"] is None


# -- merge onto normalized columns ---------------------------------------
def _fivetran_norm():
    return {"connections": [{"connection_id": "c1", "tables": [{
        "destination_schema": "salesforce", "destination_table": "account",
        "columns": [{"destination_name": "id"}, {"destination_name": "name"},
                    {"destination_name": "untracked"}],
    }]}]}


def test_apply_column_schema_merges_and_stamps_provenance():
    norm = _fivetran_norm()
    updated = apply_column_schema(norm, {
        ("SALESFORCE", "ACCOUNT", "ID"): {"data_type": "NUMBER", "max_length": None, "nullable": False},
        ("salesforce", "account", "name"): {"data_type": "TEXT", "max_length": 255, "nullable": True},
    })
    assert updated == 2  # case-insensitive on all three key parts

    cols = {c["destination_name"]: c for c in norm["connections"][0]["tables"][0]["columns"]}
    assert cols["id"]["data_type"] == "NUMBER"
    assert cols["id"]["nullable"] is False
    assert cols["name"]["max_length"] == 255
    assert cols["name"]["schema_source"] == "information_schema"
    # a column absent from the information schema is left untouched, not zeroed
    assert "data_type" not in cols["untracked"]


def test_apply_column_schema_noop_on_unknown_table():
    norm = _fivetran_norm()
    assert apply_column_schema(norm, {("other", "table", "id"): {"data_type": "TEXT"}}) == 0


# -- Snowflake reader -----------------------------------------------------
class _FakeCursor:
    def __init__(self, rows):
        self.rows, self.sql, self.params = rows, None, None

    def execute(self, sql, params=None):
        self.sql, self.params = sql, params

    def fetchall(self):
        return self.rows

    def close(self):
        pass


def _snowflake_reader(rows, settings=None, destination=None):
    from metadata_service.warehouse.snowflake_reader import SnowflakeMetadataReader

    reader = SnowflakeMetadataReader(settings or _creds(warehouse_account="a",
                                                       warehouse_database="DB"),
                                     destination=destination)
    cursor = _FakeCursor(rows)
    reader._conn = type("FakeConn", (), {"cursor": lambda self: cursor})()
    return reader, cursor


def test_snowflake_read_column_schema_targets_the_database_information_schema():
    reader, cursor = _snowflake_reader([("SALESFORCE", "ACCOUNT", "NAME", "TEXT", 255, "YES")])
    out = reader.read_column_schema(["salesforce"])
    assert out[("salesforce", "account", "name")]["max_length"] == 255
    assert "DB.INFORMATION_SCHEMA.COLUMNS" in cursor.sql
    assert cursor.params == ["SALESFORCE"]


def test_snowflake_reader_uses_detected_database_when_unset():
    """The information schema is per-database on Snowflake, so an auto-detected
    destination must retarget it — not just the fivetran_metadata join."""
    info = describe_destination(SNOWFLAKE_DESTINATION)
    reader, cursor = _snowflake_reader([], settings=_creds(), destination=info)
    reader.read_column_schema(None)
    assert "CENSUS.INFORMATION_SCHEMA.COLUMNS" in cursor.sql
    assert "CENSUS.fivetran_metadata" in reader._fqn


def test_snowflake_reader_rejects_a_non_identifier_database():
    """The database is interpolated as an identifier, so it must be validated."""
    from metadata_service.exceptions import MetadataServiceError
    from metadata_service.warehouse.snowflake_reader import SnowflakeMetadataReader

    info = describe_destination(
        {"id": "d", "service": "snowflake", "config": {"database": "DB; drop table x"}}
    )
    with pytest.raises(MetadataServiceError, match="WAREHOUSE_DATABASE"):
        SnowflakeMetadataReader(_creds(), destination=info)


# -- end-to-end surface on warehouse objects ------------------------------
def test_warehouse_objects_carry_the_destination_database(built_doc):
    """The destinations endpoint exposes the database, so `database` is populated
    — but object_id deliberately keeps its `unknown` segment so a stable public
    identifier is not rewritten under existing consumers."""
    obj = built_doc["warehouse_objects"][0]
    assert obj["database"] == "ANALYTICS"
    assert obj["object_id"].startswith("warehouse://unknown/")


def test_data_type_falls_back_to_the_dbt_catalog_without_a_warehouse_read(built_doc):
    """Fixtures mode never reaches a warehouse, so `data_type` comes from the dbt
    catalog and says so, while max_length/nullable — which only the information
    schema can supply — stay explicitly null rather than absent."""
    col = built_doc["warehouse_objects"][0]["columns"][0]
    assert col["data_type"] == "NUMBER"
    assert col["data_type_source"] == "dbt_catalog"
    for field in ("max_length", "nullable", "schema_source"):
        assert field in col and col[field] is None


def test_columns_uncovered_by_either_source_still_carry_every_key(built_doc):
    """A missing key would read as a schema regression to a consumer."""
    cols = {c["name"]: c for c in built_doc["warehouse_objects"][0]["columns"]}
    uncovered = cols["name"]  # in neither the dbt catalog nor the manifest
    for field in ("data_type", "data_type_source", "max_length", "nullable",
                  "schema_source"):
        assert field in uncovered and uncovered[field] is None


def test_information_schema_wins_over_the_dbt_catalog():
    """The warehouse describes the column as it is now; the catalog is a snapshot
    from the last docs generate, so it must not override a live read."""
    from metadata_service.normalizers.combined_normalizer import _resolve_data_type

    warehouse_col = {"data_type": "VARCHAR(16777216)", "schema_source": "information_schema"}
    assert _resolve_data_type(warehouse_col, "TEXT") == (
        "VARCHAR(16777216)", "information_schema")
    # reader did not match this column -> the catalog fills in, and says so
    assert _resolve_data_type({}, "TEXT") == ("TEXT", "dbt_catalog")
    # neither source covers it
    assert _resolve_data_type({}, None) == (None, None)


def test_derived_unique_reports_its_provenance(built_doc):
    """`unique` is derived, never a warehouse constraint — Fivetran-replicated
    tables carry none and Snowflake does not enforce UNIQUE."""
    cols = {c["name"]: c for c in built_doc["warehouse_objects"][0]["columns"]}
    assert cols["id"]["unique"] is True
    assert cols["id"]["unique_source"] == "primary_key"
    assert cols["name"]["unique"] is False
    assert cols["name"]["unique_source"] is None


def test_derive_unique_rules():
    from metadata_service.normalizers.combined_normalizer import _derive_unique

    assert _derive_unique(True, []) == (True, "primary_key")
    # a PK outranks an attached test as the provenance
    assert _derive_unique(True, ["unique"]) == (True, "primary_key")
    assert _derive_unique(False, ["not_null", "unique"]) == (True, "dbt_test")
    assert _derive_unique(False, ["dbt_utils.unique_combination_of_columns"]) == (False, None)
    assert _derive_unique(False, ["not_null"]) == (False, None)


# -- pipeline wiring ------------------------------------------------------
def test_enrichment_passes_the_detected_destination_to_the_factory(monkeypatch):
    """End-to-end wiring: the destination normalized off the raw payload must
    reach the reader factory, and the column read must be bounded to the
    destination schemas actually in the snapshot."""
    from metadata_service import pipeline
    from metadata_service.normalizers import FivetranNormalizer

    seen = {}

    class StubReader:
        def read_primary_keys(self, connection_ids=None):
            seen["connection_ids"] = connection_ids
            return {("salesforce", "account"): ["id"]}

        def read_column_schema(self, schemas=None):
            seen["schemas"] = schemas
            return {("salesforce", "account", "id"):
                    {"data_type": "NUMBER", "max_length": None, "nullable": False}}

        def close(self):
            seen["closed"] = True

    def fake_factory(settings, destination=None):
        seen["destination"] = destination
        return StubReader()

    monkeypatch.setattr("metadata_service.warehouse.get_warehouse_reader", fake_factory)

    norm = FivetranNormalizer().normalize({
        "destination": SNOWFLAKE_DESTINATION,
        "connections": [{
            "detail": {"id": "c1", "service": "salesforce"},
            "schemas": {"schemas": {"salesforce": {"tables": {"account": {
                "columns": {"Id": {"name_in_destination": "id", "enabled": True}}}}}}},
            "columns": {},
        }],
    })
    assert pipeline._enrich_from_warehouse(Settings(), norm) == "ran"

    assert seen["destination"].warehouse_type == "snowflake"
    assert seen["destination"].database == "CENSUS"
    assert seen["connection_ids"] == ["c1"]
    assert seen["schemas"] == ["salesforce"]
    assert seen["closed"] is True

    col = norm["connections"][0]["tables"][0]["columns"][0]
    assert col["is_primary_key"] is True and col["key_source"] == "fivetran_platform"
    assert col["data_type"] == "NUMBER" and col["nullable"] is False


def test_column_read_failure_keeps_authoritative_primary_keys(monkeypatch):
    """A role with SELECT on fivetran_metadata but not INFORMATION_SCHEMA must
    not lose its PKs as collateral."""
    from metadata_service import pipeline
    from metadata_service.normalizers import FivetranNormalizer

    class HalfBrokenReader:
        def read_primary_keys(self, connection_ids=None):
            return {("salesforce", "account"): ["id"]}

        def read_column_schema(self, schemas=None):
            raise RuntimeError("permission denied on INFORMATION_SCHEMA")

        def close(self):
            pass

    monkeypatch.setattr("metadata_service.warehouse.get_warehouse_reader",
                        lambda settings, destination=None: HalfBrokenReader())

    norm = FivetranNormalizer().normalize({
        "destination": SNOWFLAKE_DESTINATION,
        "connections": [{
            "detail": {"id": "c1"},
            "schemas": {"schemas": {"salesforce": {"tables": {"account": {
                "columns": {"Id": {"name_in_destination": "id", "enabled": True}}}}}}},
            "columns": {},
        }],
    })
    assert pipeline._enrich_from_warehouse(Settings(), norm) == "ran"
    col = norm["connections"][0]["tables"][0]["columns"][0]
    assert col["is_primary_key"] is True
    assert col.get("data_type") is None


def test_enrichment_unavailable_without_a_destination_or_settings(monkeypatch):
    from metadata_service import pipeline

    norm = {"destination": None, "connections": []}
    assert pipeline._enrich_from_warehouse(Settings(), norm) == "unavailable"


def test_warehouse_read_overrides_the_catalog_in_the_built_document(fivetran_raw, dbt_raw):
    """End-to-end precedence: the fixture's dbt catalog says NUMBER, the warehouse
    says NUMBER(38,0). The built object must carry the warehouse's answer, and the
    catalog must still cover a column the warehouse read missed."""
    from metadata_service.normalizers import CombinedNormalizer, DbtNormalizer, FivetranNormalizer
    from metadata_service.warehouse import apply_column_schema

    fivetran_norm = FivetranNormalizer().normalize(fivetran_raw)
    applied = apply_column_schema(fivetran_norm, {
        ("salesforce", "account", "id"):
            {"data_type": "NUMBER(38,0)", "max_length": None, "nullable": False},
    })
    assert applied == 1

    doc = CombinedNormalizer(Settings()).build(
        fivetran_norm, DbtNormalizer().normalize(dbt_raw), {})
    cols = {c["name"]: c for c in doc["warehouse_objects"][0]["columns"]}

    assert cols["id"]["data_type"] == "NUMBER(38,0)"
    assert cols["id"]["data_type_source"] == "information_schema"
    assert cols["id"]["nullable"] is False
    assert cols["id"]["schema_source"] == "information_schema"

    # untouched by the warehouse read -> catalog fills data_type, but the two
    # fields only the information schema can supply stay null
    assert cols["email"]["data_type"] == "VARCHAR"
    assert cols["email"]["data_type_source"] == "dbt_catalog"
    assert cols["email"]["max_length"] is None
    assert cols["email"]["schema_source"] is None
