"""Make the example package importable and build a real snapshot from the
metadata-service fixtures — the example's tests double as a consumer-side
contract guard: if the snapshot shape drifts, these fail."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_EXAMPLE_ROOT = Path(__file__).resolve().parents[1]
_REPO_ROOT = _EXAMPLE_ROOT.parents[1]
sys.path.insert(0, str(_EXAMPLE_ROOT))

FIXTURES = _REPO_ROOT / "tests" / "fixtures"


@pytest.fixture(scope="session")
def snapshot_path(tmp_path_factory) -> Path:
    """A real latest.json produced by the metadata-service pipeline (offline)."""
    from metadata_service.config import Settings
    from metadata_service.pipeline import build_and_store

    root = tmp_path_factory.mktemp("dqmw_snapshots")
    settings = Settings(metadata_storage_backend="local", metadata_local_path=str(root))
    build_and_store(settings, fixtures_dir=str(FIXTURES))
    return root / "latest.json"


@pytest.fixture()
def snapshot(snapshot_path) -> dict:
    from dq_middleware.snapshot import load_snapshot

    return load_snapshot(str(snapshot_path))


@pytest.fixture()
def policy_path() -> Path:
    return _EXAMPLE_ROOT / "policy.toml"
