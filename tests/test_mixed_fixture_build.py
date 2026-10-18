"""Mixed builds: Fivetran from fixtures, dbt + Activations + warehouse live.

Every "live" call is stubbed; nothing here touches a real API or warehouse.
"""

from __future__ import annotations

import json
import shutil

import pytest

from metadata_service import pipeline
from metadata_service.config import Settings

from .conftest import FIXTURES

FIVETRAN_FILES = (
    "fivetran_connections.json",
    "fivetran_schema_config.json",
    "fivetran_columns.json",
    "fivetran_destination.json",
)


@pytest.fixture()
def fivetran_only_dir(tmp_path):
    """A directory holding ONLY the Fivetran fixture files (no dbt/activations),
    the shape a generator for simulated connections would emit."""
    out = tmp_path / "fivetran_fixtures"
    out.mkdir()
    for name in FIVETRAN_FILES:
        shutil.copy(FIXTURES / name, out / name)
    return out


@pytest.fixture()
def live_stubs(monkeypatch, dbt_raw):
    """Stub every live extractor, recording which ones the build invoked."""
    calls: dict = {}

    def fake_fivetran(*args, **kwargs):  # pragma: no cover - must not be called
        calls["fivetran"] = True
        raise AssertionError("live Fivetran extraction must not run in mixed mode")

    def fake_dbt(settings, *, project_id=None, job_id=None):
        calls["dbt"] = {"project_id": project_id, "job_id": job_id}
        return dbt_raw

    def fake_enrich(settings, fivetran_norm):
        calls["enrich_destination"] = fivetran_norm.get("destination")
        return {"pk_enrichment": "ran", "column_schema_enrichment": "ran"}

    def fake_activations(settings):
        calls["activations"] = True
        return pipeline._empty_activations()

    monkeypatch.setattr(pipeline, "_extract_fivetran", fake_fivetran)
    monkeypatch.setattr(pipeline, "_extract_dbt", fake_dbt)
    monkeypatch.setattr(pipeline, "_enrich_from_warehouse", fake_enrich)
    monkeypatch.setattr(pipeline, "_extract_activations", fake_activations)
    return calls


def _dbt_settings(**overrides) -> Settings:
    base = dict(
        metadata_storage_backend="local",
        warehouse_type="warehouse",
        stale_sync_threshold_hours=24,
        dbt_account_id="1",
        dbt_service_token="t",
    )
    base.update(overrides)
    return Settings(**base)


def test_mixed_build_takes_fivetran_from_fixtures_and_dbt_live(fivetran_only_dir, live_stubs):
    doc = pipeline.build_metadata(
        _dbt_settings(), fivetran_fixtures_dir=str(fivetran_only_dir), dbt_project_id=7
    )

    assert "fivetran" not in live_stubs
    assert live_stubs["dbt"] == {"project_id": 7, "job_id": None}
    # Warehouse enrichment runs (it is skipped in the fully offline mode), and
    # is pointed at the destination the fixture describes.
    assert live_stubs["enrich_destination"]["database"] == "ANALYTICS"

    scope = doc["build_scope"]
    assert scope["fivetran_fixtures"] is True
    assert scope["fixtures"] is False
    assert scope["include_dbt"] is True
    assert scope["pk_enrichment"] == "ran"
    assert scope["column_schema_enrichment"] == "ran"

    # Fixture connections joined against the (live-shaped) dbt payload.
    names = {(o["schema"], o["name"]) for o in doc["warehouse_objects"]}
    assert ("salesforce", "account") in names
    assert any(r.get("source") == "fivetran_metadata" for r in doc["dq_recommendations"])


def test_live_fivetran_build_reports_fivetran_fixtures_false(live_stubs, monkeypatch):
    monkeypatch.setattr(pipeline, "_extract_fivetran",
                        lambda *a, **k: pipeline._empty_fivetran())
    doc = pipeline.build_metadata(_dbt_settings(), include_activations=False)
    assert doc["build_scope"]["fivetran_fixtures"] is False


def test_mixed_and_live_builds_do_not_drift_compare(fivetran_only_dir, live_stubs, monkeypatch):
    from metadata_service.dq.drift import detect_drift

    mixed = pipeline.build_metadata(_dbt_settings(), fivetran_fixtures_dir=str(fivetran_only_dir))
    monkeypatch.setattr(pipeline, "_extract_fivetran",
                        lambda *a, **k: pipeline._load_fivetran_fixtures(fivetran_only_dir))
    live = pipeline.build_metadata(_dbt_settings())

    records = detect_drift(mixed, live)
    assert [r["change_type"] for r in records] == ["comparison_skipped"]


def test_mixed_build_requires_the_connections_fixture(tmp_path, live_stubs):
    with pytest.raises(FileNotFoundError, match="fivetran_connections.json"):
        pipeline.build_metadata(_dbt_settings(), fivetran_fixtures_dir=str(tmp_path))


def test_fixtures_dir_and_fivetran_fixtures_dir_are_mutually_exclusive(fivetran_only_dir):
    with pytest.raises(ValueError, match="mutually exclusive"):
        pipeline.build_metadata(
            _dbt_settings(), fixtures_dir=str(FIXTURES), fivetran_fixtures_dir=str(fivetran_only_dir)
        )


def test_fivetran_fixtures_dir_conflicts_with_no_fivetran(fivetran_only_dir):
    with pytest.raises(ValueError, match="include_fivetran"):
        pipeline.build_metadata(
            _dbt_settings(), include_fivetran=False, fivetran_fixtures_dir=str(fivetran_only_dir)
        )


def test_full_fixture_payloads_unchanged_by_refactor():
    """The offline loader still assembles the same Fivetran payload it always did."""
    fivetran_raw, _, _ = pipeline._load_fixture_payloads(FIXTURES)
    assert fivetran_raw == pipeline._load_fivetran_fixtures(FIXTURES)
    assert [c["detail"]["id"] for c in fivetran_raw["connections"]] == ["conn_sf_001"]
    assert fivetran_raw["destination"]["service"] == "snowflake"


# -- CLI ---------------------------------------------------------------------------
def _invoke(args, tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from metadata_service import cli
    from metadata_service.config import get_settings

    monkeypatch.setenv("METADATA_STORAGE_BACKEND", "local")
    monkeypatch.setenv("METADATA_LOCAL_PATH", str(tmp_path / "snaps"))
    monkeypatch.setenv("DBT_ACCOUNT_ID", "1")
    monkeypatch.setenv("DBT_SERVICE_TOKEN", "t")
    get_settings.cache_clear()
    try:
        return CliRunner().invoke(cli.app, args)
    finally:
        get_settings.cache_clear()


def test_cli_mixed_build_persists_latest_by_default(fivetran_only_dir, live_stubs, tmp_path, monkeypatch):
    result = _invoke(["build", "--fivetran-fixtures-dir", str(fivetran_only_dir)], tmp_path, monkeypatch)
    assert result.exit_code == 0, result.output
    latest = tmp_path / "snaps" / "latest.json"
    assert latest.exists()
    assert json.loads(latest.read_text())["build_scope"]["fivetran_fixtures"] is True


def test_cli_rejects_both_fixture_flags(fivetran_only_dir, tmp_path, monkeypatch):
    result = _invoke(["build", "--fixtures-dir", str(FIXTURES),
                      "--fivetran-fixtures-dir", str(fivetran_only_dir)], tmp_path, monkeypatch)
    assert result.exit_code == 2
    assert "mutually exclusive" in result.output
