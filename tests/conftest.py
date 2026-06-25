"""Shared pytest fixtures. All tests use local JSON fixtures, never live APIs."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from metadata_service.config import Settings
from metadata_service.normalizers import (
    CombinedNormalizer,
    DbtNormalizer,
    FivetranNormalizer,
)
from metadata_service.pipeline import _load_fixture_payloads

FIXTURES = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.fixture()
def fixtures_dir() -> Path:
    return FIXTURES


@pytest.fixture()
def settings() -> Settings:
    # Construct directly so .env / real credentials never leak into tests.
    return Settings(
        metadata_storage_backend="local",
        metadata_local_path="./_test_snapshots_unused",
        warehouse_type="warehouse",
        stale_sync_threshold_hours=24,
    )


@pytest.fixture()
def raw_payloads():
    return _load_fixture_payloads(FIXTURES)


@pytest.fixture()
def fivetran_raw(raw_payloads):
    return raw_payloads[0]


@pytest.fixture()
def dbt_raw(raw_payloads):
    return raw_payloads[1]


@pytest.fixture()
def fivetran_normalized(fivetran_raw):
    return FivetranNormalizer().normalize(fivetran_raw)


@pytest.fixture()
def dbt_normalized(dbt_raw):
    return DbtNormalizer().normalize(dbt_raw)


@pytest.fixture()
def built_doc(settings, fivetran_normalized, dbt_normalized):
    return CombinedNormalizer(settings).build(fivetran_normalized, dbt_normalized)


def object_by_table(doc: dict, schema: str, table: str) -> dict:
    for obj in doc["warehouse_objects"]:
        if obj["schema"] == schema and obj["name"] == table:
            return obj
    raise AssertionError(f"object {schema}.{table} not found")
