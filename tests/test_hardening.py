"""Tests for the serving/storage/pipeline hardening: API auth, atomic writes,
the refresh lock, extractor fail-fast, drift scoping, and normalizer robustness."""

from __future__ import annotations

import threading

import pytest
from fastapi.testclient import TestClient

from metadata_service.config import Settings
from metadata_service.dq.drift import detect_drift
from metadata_service.exceptions import (
    FivetranError,
    FivetranRateLimitError,
    RefreshInProgressError,
)
from metadata_service.pipeline import _REFRESH_LOCK, build_and_store

from .conftest import FIXTURES


# -- H1: API key auth --------------------------------------------------------
def _client(monkeypatch, tmp_path, api_key=None) -> TestClient:
    settings = Settings(metadata_storage_backend="local", metadata_local_path=str(tmp_path),
                        metadata_api_key=api_key)
    monkeypatch.setattr("metadata_service.api.routes.get_settings", lambda: settings)
    build_and_store(settings, fixtures_dir=str(FIXTURES))

    from metadata_service.api.main import create_app

    return TestClient(create_app())


def test_api_open_when_no_key_configured(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    assert client.get("/metadata/latest").status_code == 200


def test_api_requires_key_when_configured(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path, api_key="s3cret")
    assert client.get("/metadata/latest").status_code == 401
    assert client.get("/metadata/latest", headers={"X-API-Key": "wrong"}).status_code == 401
    assert client.get("/metadata/latest", headers={"X-API-Key": "s3cret"}).status_code == 200
    assert client.get("/metadata/latest", headers={"Authorization": "Bearer s3cret"}).status_code == 200
    # health stays open for probes
    assert client.get("/health").status_code == 200


def test_refresh_500_is_generic(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)

    def boom(*a, **k):
        raise RuntimeError("snowflake account xy12345 unreachable at /Users/eric/secret")

    monkeypatch.setattr("metadata_service.api.routes.build_and_store", boom)
    resp = client.post("/metadata/refresh", json={})
    assert resp.status_code == 500
    assert "xy12345" not in resp.text and "/Users/" not in resp.text


# -- H2: atomic writes + refresh lock ----------------------------------------
def test_atomic_write_leaves_no_temp_files(tmp_path):
    from metadata_service.storage.local_storage import LocalStorage

    storage = LocalStorage(str(tmp_path))
    storage.write_snapshot({"generated_at": "2026-07-06T00:00:00Z", "version": "1.0"})
    leftovers = [p for p in tmp_path.rglob("*") if p.is_file() and p.suffix == ".tmp"]
    assert leftovers == []
    assert storage.read_latest()["version"] == "1.0"


def test_concurrent_refresh_raises(tmp_path):
    settings = Settings(metadata_storage_backend="local", metadata_local_path=str(tmp_path))
    assert _REFRESH_LOCK.acquire(blocking=False)  # simulate a build in flight
    try:
        with pytest.raises(RefreshInProgressError):
            build_and_store(settings, fixtures_dir=str(FIXTURES))
    finally:
        _REFRESH_LOCK.release()
    # and the lock is released after a normal run
    build_and_store(settings, fixtures_dir=str(FIXTURES))
    assert not _REFRESH_LOCK.locked()


def test_refresh_conflict_maps_to_409(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)

    def busy(*a, **k):
        raise RefreshInProgressError("busy")

    monkeypatch.setattr("metadata_service.api.routes.build_and_store", busy)
    assert client.post("/metadata/refresh", json={}).status_code == 409


# -- H3: extractor fails fast on unrecoverable errors -------------------------
class _RateLimitedClient:
    def list_connections(self, group_id=None):
        return [{"id": "c1"}]

    def get_connection(self, cid):
        return {"id": cid, "service": "s", "status": {}}

    def get_connection_schemas(self, cid):
        return {"schemas": {"s": {"enabled": True, "tables": {"t": {"enabled": True}}}}}

    def get_table_columns(self, cid, schema, table):
        raise FivetranRateLimitError("429 after retries")

    def get_connector_type(self, service):
        return {}


def test_extractor_reraises_rate_limit():
    from metadata_service.extractors.fivetran_extractor import FivetranExtractor

    with pytest.raises(FivetranRateLimitError):
        FivetranExtractor(_RateLimitedClient()).extract()


def test_extractor_still_captures_ordinary_errors():
    from metadata_service.extractors.fivetran_extractor import FivetranExtractor

    class OrdinaryFailure(_RateLimitedClient):
        def get_table_columns(self, cid, schema, table):
            raise FivetranError("400 bad table")

    raw = FivetranExtractor(OrdinaryFailure()).extract()
    assert len(raw["connections"]) == 1
    assert any(e["error_type"] == "FivetranError" for e in raw["errors"])


def test_extractor_tolerates_null_schema_config():
    from metadata_service.extractors.fivetran_extractor import FivetranExtractor

    class NullSchema(_RateLimitedClient):
        def get_connection_schemas(self, cid):
            return {"schemas": {"good": {"enabled": True, "tables": {"t": None}},
                                "bad": None}}

        def get_table_columns(self, cid, schema, table):
            return {"columns": {}}

    raw = FivetranExtractor(NullSchema()).extract()
    assert len(raw["connections"]) == 1  # no AttributeError crash


# -- H5: drift only compares like-for-like builds ------------------------------
def _doc(scope, objects):
    return {"build_scope": scope,
            "warehouse_objects": [{"object_id": o, "origin": {}, "columns": [], "dbt": {}} for o in objects]}


def test_drift_skipped_across_different_scopes():
    full = _doc({"group_id": None, "include_fivetran": True}, ["a", "b", "c"])
    scoped = _doc({"group_id": "g1", "include_fivetran": True}, ["a"])
    assert detect_drift(full, scoped) == []  # NOT two removed_table records


def test_drift_still_runs_for_matching_scopes():
    scope = {"group_id": "g1", "include_fivetran": True}
    prev = _doc(scope, ["a", "b"])
    latest = _doc(scope, ["a"])
    records = detect_drift(prev, latest)
    assert [r["change_type"] for r in records] == ["removed_table"]


# -- H7: one malformed table degrades, not crashes -----------------------------
def test_malformed_table_lands_in_errors_not_crash(settings, dbt_normalized):
    from metadata_service.normalizers import CombinedNormalizer

    fivetran_norm = {
        "extracted_at": "2026-07-06T00:00:00Z",
        "connections": [{
            "connection_id": "c1", "connector_service": "s",
            "tables": [
                {"destination_schema": "s", "destination_table": "good", "columns": []},
                # columns as a non-list crashes _build_columns without the guard
                {"destination_schema": "s", "destination_table": "bad", "columns": 42},
            ],
        }],
        "errors": [],
    }
    doc = CombinedNormalizer(settings).build(fivetran_norm, dbt_normalized)
    names = [o["name"] for o in doc["warehouse_objects"]]
    assert "good" in names and "bad" not in names
    combined_errors = [e for e in doc["errors"] if e.get("source") == "combined"]
    assert combined_errors and combined_errors[0]["table"] == "bad"


def test_parse_dt_handles_epoch_timestamps():
    from datetime import datetime, timezone

    from metadata_service.dq.recommendations import _is_stale, _parse_dt

    # Epoch numbers are real connector output; they must parse (not silently
    # read as "not stale" — a fail-open hole in a fail-closed gate).
    now = datetime(2024, 7, 2, tzinfo=timezone.utc)  # ~2 weeks after the epoch below
    assert _is_stale(1718000000, 24, now=now) is True          # epoch seconds (int)
    assert _is_stale(1718000000.0, 24, now=now) is True        # epoch seconds (float)
    assert _is_stale(1718000000000, 24, now=now) is True       # epoch millis
    # A recent epoch is not stale.
    recent = int(now.timestamp()) - 3600
    assert _is_stale(recent, 24, now=now) is False
    # Genuinely unparseable values still degrade to "not stale" without crashing.
    assert _parse_dt("not-a-date") is None
    assert _is_stale("not-a-date", 24, now=now) is False
