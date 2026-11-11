"""Lake catalog readers: destination detection, Iceberg parsing, Polaris REST,
Glue, the factory, and fault-isolated enrichment in the pipeline.

Everything is mocked (httpx.MockTransport, a fake Glue client); nothing here
touches a live catalog.
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone

import httpx
import pytest

from metadata_service import pipeline
from metadata_service.catalog import (
    CatalogReadResult,
    apply_catalog_tables,
    parse_table_metadata,
    physical_locations,
    plan_catalog_readers,
    requested_catalogs,
)
from metadata_service.catalog.glue import GlueCatalogReader, parse_glue_table
from metadata_service.catalog.polaris import PolarisAuthError, PolarisCatalogReader
from metadata_service.config import Settings
from metadata_service.warehouse import describe_destination, destination_from_dict

from .conftest import FIXTURES

# Shape of GET /v1/destinations/{id} for a Managed Data Lake destination
# (taken from a live MDLS destination, identifiers anonymized). Every secret is
# masked by Fivetran.
MDLS_DESTINATION = {
    "id": "lake_dest",
    "group_id": "lake_dest",
    "service": "managed_data_lake",
    "region": "AWS_US_WEST_2",
    "setup_status": "connected",
    "config": {
        "auth_type": "OAUTH2",
        "should_maintain_tables_in_glue": True,
        "catalog": "demo_uc_catalog",
        "should_maintain_tables_in_databricks": True,
        "fivetran_role_arn": "arn:aws:iam::000000000000:role/demo-lake-role",
        "oauth2_secret": "******",
        "polaris_catalog_configuration": {
            "polaris_server_endpoint": "https://demo-host.us-west-2.aws.polaris.fivetran.com/api/catalog",
            "polaris_catalog": "lake_dest",
            "client_id": "democlientid",
            "client_secret": "********",
        },
        "server_host_name": "dbc-demo.cloud.databricks.com",
        "bucket": "demo-lake-bucket",
        "should_write_delta": True,
        "storage_provider": "AWS",
        "region": "us-west-2",
    },
}

ICEBERG_METADATA = {
    "format-version": 2,
    "table-uuid": "4e3c-uuid",
    "location": "s3://demo-lake-bucket/salesforce/account",
    "last-updated-ms": 1762848000000,
    "current-schema-id": 1,
    "schemas": [
        {"schema-id": 0, "type": "struct", "fields": [{"id": 1, "name": "id", "required": True, "type": "string"}]},
        {"schema-id": 1, "type": "struct", "identifier-field-ids": [1], "fields": [
            {"id": 1, "name": "id", "required": True, "type": "string"},
            {"id": 2, "name": "name", "required": False, "type": "string"},
            {"id": 3, "name": "email", "required": False, "type": "string"},
            {"id": 4, "name": "annual_revenue", "required": False, "type": "decimal(18,2)"},
            {"id": 5, "name": "tags", "required": False,
             "type": {"type": "list", "element-id": 9, "element": "string", "element-required": False}},
            {"id": 6, "name": "_fivetran_synced", "required": False, "type": "timestamptz"},
            {"id": 7, "name": "hash", "required": False, "type": "fixed[16]"},
        ]},
    ],
    "default-spec-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": [
        {"source-id": 6, "field-id": 1000, "name": "_fivetran_synced_day", "transform": "day"}]}],
    "current-snapshot-id": 77,
    "snapshots": [
        {"snapshot-id": 76, "timestamp-ms": 1762800000000, "summary": {"operation": "append"}},
        {"snapshot-id": 77, "timestamp-ms": 1762848000000,
         "summary": {"operation": "overwrite", "total-records": "1250"}},
    ],
}


# -- destination ------------------------------------------------------------
def test_describe_mdls_destination_derives_catalogs_without_secrets():
    info = describe_destination(MDLS_DESTINATION)
    assert info.warehouse_type == "managed_data_lake" and info.is_lake()
    lake = info.lake
    kinds = [c["type"] for c in lake["catalogs"]]
    assert kinds == ["polaris", "glue", "unity"]
    polaris = lake["catalogs"][0]
    assert polaris["endpoint"].endswith("/api/catalog")
    assert polaris["catalog"] == "lake_dest" and polaris["client_id"] == "democlientid"
    assert lake["table_formats"] == ["iceberg", "delta"]
    assert lake["catalogs"][1]["region"] == "us-west-2"
    serialized = json.dumps(info.as_dict())
    assert "******" not in serialized and "role_arn" not in serialized
    # Round-trips through the snapshot form.
    assert destination_from_dict(info.as_dict()).lake == lake


def test_warehouse_destination_serialization_unchanged():
    info = describe_destination(json.loads((FIXTURES / "fivetran_destination.json").read_text()))
    assert info.lake is None and "lake" not in info.as_dict()


# -- iceberg parsing ----------------------------------------------------------
def test_parse_iceberg_metadata():
    t = parse_table_metadata(ICEBERG_METADATA, catalog="polaris", namespace="salesforce", table="account")
    assert t["table_format"] == "iceberg" and t["format_version"] == 2
    assert t["primary_keys"] == ["id"]
    assert t["columns"]["id"] == {"name": "id", "data_type": "string", "nullable": False,
                                  "max_length": None, "field_id": 1, "doc": None}
    assert t["columns"]["annual_revenue"]["data_type"] == "decimal(18,2)"
    assert t["columns"]["tags"]["data_type"] == "list<string>"
    assert t["columns"]["hash"]["max_length"] == 16
    assert t["partition_spec"] == [{"source_column": "_fivetran_synced", "transform": "day",
                                    "name": "_fivetran_synced_day"}]
    assert t["current_snapshot_id"] == 77 and t["total_records"] == 1250
    assert t["snapshot_operation"] == "overwrite"
    assert t["last_updated"] == "2025-11-11T08:00:00Z"


# -- polaris ------------------------------------------------------------------
def _polaris(handler) -> PolarisCatalogReader:
    return PolarisCatalogReader(
        endpoint="https://polaris.example/api/catalog", catalog="lake_dest",
        client_id="cid", client_secret="TOP-SECRET",
        client=httpx.Client(transport=httpx.MockTransport(handler)))


def _polaris_handler(calls, *, tables=None, fail=()):
    tables = tables if tables is not None else {("salesforce", "account"): ICEBERG_METADATA}

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        path = request.url.path
        if path.endswith("/v1/oauth/tokens"):
            body = request.content.decode()
            assert "grant_type=client_credentials" in body and "scope=PRINCIPAL_ROLE%3AALL" in body
            return httpx.Response(200, json={"access_token": "tok", "expires_in": 3600})
        assert request.headers["authorization"] == "Bearer tok"
        if path.endswith("/v1/config"):
            assert request.url.params["warehouse"] == "lake_dest"
            return httpx.Response(200, json={"defaults": {}, "overrides": {"prefix": "lake_dest_prefix"}})
        parts = path.split("/")
        ns, table = parts[-3], parts[-1]
        assert "/v1/lake_dest_prefix/namespaces/" in path
        if (ns, table) in fail:
            return httpx.Response(500, json={"error": "boom"})
        meta = tables.get((ns, table))
        if meta is None:
            return httpx.Response(404, json={"error": {"type": "NoSuchTableException"}})
        return httpx.Response(200, json={"metadata-location": "s3://x/metadata/v3.json", "metadata": meta})

    return handler


def test_polaris_reads_tables_and_isolates_failures():
    calls: list = []
    reader = _polaris(_polaris_handler(calls, fail={("salesforce", "contact")}))
    result = reader.read_tables([("salesforce", "account"), ("salesforce", "contact"),
                                 ("salesforce", "missing")])
    assert set(result.tables) == {("salesforce", "account")}
    assert result.missing == [("salesforce", "missing")]
    assert len(result.errors) == 1 and result.errors[0]["table"] == "contact"
    assert result.errors[0]["error_message"] == "HTTP 500 loading table"
    # One token exchange and one config call for the whole build.
    assert sum(1 for _, p in calls if p.endswith("/oauth/tokens")) == 1
    assert sum(1 for _, p in calls if p.endswith("/v1/config")) == 1


def test_polaris_auth_failure_raises_without_secret():
    reader = _polaris(lambda req: httpx.Response(401, json={"error": "bad client TOP-SECRET"}))
    with pytest.raises(PolarisAuthError) as exc:
        reader.read_tables([("salesforce", "account")])
    assert "TOP-SECRET" not in str(exc.value) and "401" in str(exc.value)


def test_polaris_refreshes_expired_token_once():
    state = {"loads": 0, "tokens": 0}

    def handler(request):
        if request.url.path.endswith("/oauth/tokens"):
            state["tokens"] += 1
            return httpx.Response(200, json={"access_token": "tok"})
        if request.url.path.endswith("/v1/config"):
            return httpx.Response(200, json={"overrides": {}})
        state["loads"] += 1
        if state["loads"] == 1:
            return httpx.Response(401)
        return httpx.Response(200, json={"metadata": ICEBERG_METADATA})

    result = _polaris(handler).read_tables([("salesforce", "account")])
    assert ("salesforce", "account") in result.tables and state["tokens"] == 2


# -- glue ---------------------------------------------------------------------
GLUE_TABLE = {
    "Name": "account",
    "DatabaseName": "salesforce",
    "UpdateTime": datetime(2026, 11, 10, 8, 0, tzinfo=timezone.utc),
    "Parameters": {"table_type": "ICEBERG", "metadata_location": "s3://x/metadata/v3.json"},
    "StorageDescriptor": {
        "Location": "s3://demo-lake-bucket/salesforce/account",
        "Columns": [
            {"Name": "id", "Type": "string", "Parameters": {"iceberg.field.optional": "false"}},
            {"Name": "name", "Type": "varchar(255)", "Parameters": {"iceberg.field.optional": "true"}},
            {"Name": "email", "Type": "string"},
        ],
    },
}


class FakeGlue:
    def __init__(self, databases):
        self.databases = databases

    def get_paginator(self, op):
        assert op == "get_tables"
        glue = self

        class P:
            def paginate(self, DatabaseName, **kw):
                if DatabaseName not in glue.databases:
                    raise type("EntityNotFoundException", (Exception,), {})("nope")
                if glue.databases[DatabaseName] == "deny":
                    raise type("AccessDeniedException", (Exception,), {})("denied")
                yield {"TableList": glue.databases[DatabaseName]}

        return P()


def test_parse_glue_table():
    t = parse_glue_table(GLUE_TABLE, namespace="salesforce")
    assert t["table_format"] == "iceberg" and t["primary_keys"] is None
    assert t["columns"]["id"]["nullable"] is False
    assert t["columns"]["name"]["max_length"] == 255 and t["columns"]["name"]["nullable"] is True
    assert t["columns"]["email"]["nullable"] is None
    assert t["last_updated"] == "2026-11-10T08:00:00Z"


def test_glue_reader_isolates_databases():
    reader = GlueCatalogReader(client=FakeGlue({"salesforce": [GLUE_TABLE], "hr": "deny"}))
    result = reader.read_tables([("SALESFORCE", "account"), ("salesforce", "contact"),
                                 ("hr", "employee"), ("gone", "x")])
    assert set(result.tables) == {("salesforce", "account")}
    assert ("salesforce", "contact") in result.missing and ("gone", "x") in result.missing
    assert [e["schema"] for e in result.errors] == ["hr"]


# -- enrichment ---------------------------------------------------------------
def _norm():
    return {"connections": [{"connection_id": "c1", "tables": [{
        "destination_schema": "salesforce", "destination_table": "account", "enabled": True,
        "columns": [{"destination_name": "id", "is_primary_key": False},
                    {"destination_name": "name"},
                    {"destination_name": "email", "data_type": "VARCHAR", "schema_source": "information_schema"}],
    }]}]}


def test_apply_catalog_tables_precedence_and_pks():
    norm = _norm()
    polaris = parse_table_metadata(ICEBERG_METADATA, catalog="polaris", namespace="salesforce", table="account")
    glue = parse_glue_table(GLUE_TABLE, namespace="salesforce")
    c1 = apply_catalog_tables(norm, "polaris", {("salesforce", "account"): polaris})
    c2 = apply_catalog_tables(norm, "glue", {("salesforce", "account"): glue})
    table = norm["connections"][0]["tables"][0]
    cols = {c["destination_name"]: c for c in table["columns"]}
    assert c1 == {"tables": 1, "columns": 2, "primary_keys": 1}
    assert c2["columns"] == 0  # polaris already described them
    assert cols["id"]["is_primary_key"] and cols["id"]["key_source"] == "catalog:polaris"
    assert cols["name"]["schema_source"] == "catalog:polaris" and cols["name"]["nullable"] is True
    # An INFORMATION_SCHEMA value is never overwritten.
    assert cols["email"]["schema_source"] == "information_schema"
    assert table["lake"]["catalogs"] == ["polaris", "glue"]
    assert table["lake"]["partition_spec"][0]["transform"] == "day"


# -- factory ------------------------------------------------------------------
def _lake_dest():
    return describe_destination(MDLS_DESTINATION)


def test_factory_auto_routes_lake_destination(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    s = Settings(polaris_client_secret="x")
    assert requested_catalogs(s, _lake_dest()) == ["polaris", "glue", "unity"]
    plans = {p.catalog_type: p for p in plan_catalog_readers(s, _lake_dest())}
    assert isinstance(plans["polaris"].reader, PolarisCatalogReader)
    assert plans["polaris"].reader.endpoint.endswith("/api/catalog")
    assert plans["unity"].reader is None and plans["unity"].outcome == "not_implemented"
    assert isinstance(plans["glue"].reader, GlueCatalogReader)


def test_factory_glue_without_aws_identity(monkeypatch, tmp_path):
    for var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "none"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "none"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    plans = {p.catalog_type: p for p in plan_catalog_readers(Settings(), _lake_dest())}
    assert plans["glue"].outcome == "not_configured" and "AWS" in plans["glue"].reason


def test_factory_reports_missing_secret_and_respects_overrides():
    plans = {p.catalog_type: p for p in plan_catalog_readers(Settings(), _lake_dest())}
    assert plans["polaris"].outcome == "not_configured"
    assert "POLARIS_CLIENT_SECRET" in plans["polaris"].reason
    assert requested_catalogs(Settings(lake_catalogs="none"), _lake_dest()) == []
    warehouse = describe_destination(json.loads((FIXTURES / "fivetran_destination.json").read_text()))
    assert requested_catalogs(Settings(), warehouse) == []
    assert requested_catalogs(Settings(lake_catalogs="glue,polaris"), warehouse) == ["polaris", "glue"]
    s = Settings(polaris_client_secret="x", polaris_endpoint="https://other/api/catalog",
                 polaris_catalog="override")
    reader = plan_catalog_readers(s, _lake_dest())[0].reader
    assert reader.endpoint == "https://other/api/catalog" and reader.catalog == "override"


def test_physical_locations_per_catalog():
    locs = physical_locations(_lake_dest().as_dict(), "salesforce", "account")
    by = {loc["catalog"]: loc for loc in locs}
    assert by["polaris"]["fqn"] == "lake_dest.salesforce.account"
    assert by["glue"]["fqn"] == "awsdatacatalog.salesforce.account"
    assert by["unity"]["fqn"] == "demo_uc_catalog.salesforce.account" and by["unity"]["format"] == "delta"
    sf = describe_destination(json.loads((FIXTURES / "fivetran_destination.json").read_text())).as_dict()
    assert physical_locations(sf, "salesforce", "account")[0]["fqn"] == "ANALYTICS.salesforce.account"


# -- pipeline -----------------------------------------------------------------
@pytest.fixture()
def lake_fixtures(tmp_path):
    out = tmp_path / "ft"
    out.mkdir()
    for name in ("fivetran_connections.json", "fivetran_schema_config.json", "fivetran_columns.json"):
        shutil.copy(FIXTURES / name, out / name)
    (out / "fivetran_destination.json").write_text(json.dumps(MDLS_DESTINATION))
    return out


class _Reader:
    def __init__(self, kind, tables=None, raise_exc=None):
        self.catalog_type, self._tables, self._raise = kind, tables or {}, raise_exc
        self.closed = False

    def read_tables(self, wanted):
        if self._raise:
            raise self._raise
        return CatalogReadResult(tables=self._tables, errors=[
            {"source": "catalog", "catalog": self.catalog_type, "schema": "salesforce",
             "table": "contact", "error_type": "HTTPStatusError", "error_message": "HTTP 403 loading table"}])

    def close(self):
        self.closed = True


def test_pipeline_catalog_enrichment_is_fault_isolated(monkeypatch, lake_fixtures):
    from metadata_service.catalog.factory import PlannedReader

    polaris = _Reader("polaris", raise_exc=RuntimeError("polaris down"))
    glue = _Reader("glue", tables={("salesforce", "account"): parse_glue_table(GLUE_TABLE, namespace="salesforce")})
    monkeypatch.setattr("metadata_service.catalog.plan_catalog_readers", lambda s, d: [
        PlannedReader("polaris", polaris), PlannedReader("glue", glue),
        PlannedReader("unity", outcome="not_implemented")])
    doc = pipeline.build_metadata(Settings(), fivetran_fixtures_dir=str(lake_fixtures),
                                  include_dbt=False, include_activations=False)
    scope = doc["build_scope"]
    assert scope["catalog_enrichment"] == {"polaris": "failed", "glue": "ran", "unity": "not_implemented"}
    assert scope["column_schema_enrichment"] == "unavailable"  # no information schema on a lake
    assert polaris.closed and glue.closed
    account = next(o for o in doc["warehouse_objects"] if o["name"] == "account")
    assert account["lake"]["catalogs"] == ["glue"] and account["lake"]["table_format"] == "iceberg"
    id_col = next(c for c in account["columns"] if c["name"] == "id")
    assert id_col["data_type"] == "string" and id_col["data_type_source"] == "catalog:glue"
    assert id_col["nullable"] is False
    errors = [e for e in doc["errors"] if e.get("source") == "catalog"]
    assert {e["catalog"] for e in errors} == {"polaris", "glue"}
    assert doc["sources"]["fivetran"]["destination"]["lake"]["catalogs"][0]["type"] == "polaris"


def test_warehouse_build_scope_has_no_catalog_key(monkeypatch, tmp_path):
    out = tmp_path / "ft"
    out.mkdir()
    for name in ("fivetran_connections.json", "fivetran_schema_config.json", "fivetran_columns.json",
                 "fivetran_destination.json"):
        shutil.copy(FIXTURES / name, out / name)
    doc = pipeline.build_metadata(Settings(), fivetran_fixtures_dir=str(out),
                                  include_dbt=False, include_activations=False)
    assert "catalog_enrichment" not in doc["build_scope"]
