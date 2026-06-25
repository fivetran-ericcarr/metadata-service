"""End-to-end orchestration: extract -> normalize -> combine -> (store).

Shared by the CLI, REST API, and MCP server so they all build snapshots the
same way. Supports an offline ``fixtures_dir`` mode that loads raw payloads from
JSON fixtures instead of calling live APIs.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from .clients import DbtClient, FivetranClient
from .config import Settings
from .extractors import DbtExtractor, FivetranExtractor
from .normalizers import CombinedNormalizer, DbtNormalizer, FivetranNormalizer
from .storage.base import get_storage

logger = logging.getLogger(__name__)


def build_metadata(
    settings: Settings,
    *,
    group_id: str | None = None,
    include_fivetran: bool = True,
    include_dbt: bool = True,
    fixtures_dir: str | None = None,
    aliases: dict | None = None,
    connected_only: bool = False,
    skip_paused: bool = False,
) -> dict:
    """Run extraction + normalization and return the normalized document."""
    if fixtures_dir:
        fivetran_raw, dbt_raw = _load_fixture_payloads(Path(fixtures_dir))
    else:
        fivetran_raw = (
            _extract_fivetran(settings, group_id, connected_only=connected_only, skip_paused=skip_paused)
            if include_fivetran
            else _empty_fivetran()
        )
        dbt_raw = _extract_dbt(settings) if include_dbt else _empty_dbt()

    fivetran_norm = FivetranNormalizer().normalize(fivetran_raw)
    dbt_norm = DbtNormalizer().normalize(dbt_raw)
    doc = CombinedNormalizer(settings, aliases=aliases).build(fivetran_norm, dbt_norm)
    logger.info(
        "Built metadata: %s warehouse objects, %s recommendations, %s errors",
        len(doc.get("warehouse_objects", [])),
        len(doc.get("dq_recommendations", [])),
        len(doc.get("errors", [])),
    )
    return doc


def build_and_store(
    settings: Settings,
    *,
    group_id: str | None = None,
    include_fivetran: bool = True,
    include_dbt: bool = True,
    fixtures_dir: str | None = None,
    aliases: dict | None = None,
    connected_only: bool = False,
    skip_paused: bool = False,
) -> dict:
    """Build metadata, attach drift vs. the previous snapshot, persist, and return
    a summary ``{status, snapshot_uri, generated_at, object_count, error_count, doc}``.
    """
    from .dq.drift import detect_drift  # local import to avoid cycles

    storage = get_storage(settings)
    previous = storage.read_latest()

    doc = build_metadata(
        settings,
        group_id=group_id,
        include_fivetran=include_fivetran,
        include_dbt=include_dbt,
        fixtures_dir=fixtures_dir,
        aliases=aliases,
        connected_only=connected_only,
        skip_paused=skip_paused,
    )
    doc["schema_drift"] = detect_drift(previous, doc)
    uri = storage.write_snapshot(doc)

    return {
        "status": "success",
        "snapshot_uri": uri,
        "generated_at": doc.get("generated_at"),
        "object_count": len(doc.get("warehouse_objects", [])),
        "error_count": len(doc.get("errors", [])),
        "doc": doc,
    }


# -- live extraction ------------------------------------------------------
def _extract_fivetran(
    settings: Settings,
    group_id: str | None,
    *,
    connected_only: bool = False,
    skip_paused: bool = False,
) -> dict:
    with FivetranClient(settings) as client:
        return FivetranExtractor(client).extract(
            group_id=group_id or settings.fivetran_group_id,
            connected_only=connected_only,
            skip_paused=skip_paused,
        )


def _extract_dbt(settings: Settings) -> dict:
    settings.require_dbt()
    with DbtClient(settings) as client:
        return DbtExtractor(client, settings.dbt_account_id or "").extract()


def _empty_fivetran() -> dict:
    return {"extracted_at": None, "source": "fivetran", "connections": [], "errors": []}


def _empty_dbt() -> dict:
    return {"extracted_at": None, "source": "dbt", "artifacts": {}, "errors": []}


# -- fixtures mode --------------------------------------------------------
def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _load_fixture_payloads(fixtures: Path) -> tuple[dict, dict]:
    """Assemble raw Fivetran + dbt payloads from fixture files (offline build)."""
    connections_fix = _load_json(fixtures / "fivetran_connections.json")
    schema_fix = _load_json(fixtures / "fivetran_schema_config.json")
    columns_fix = _load_json(fixtures / "fivetran_columns.json")

    details = connections_fix.get("connections") or []
    fivetran_connections = []
    for detail in details:
        conn_id = detail.get("id") or detail.get("connection_id")
        fivetran_connections.append(
            {
                "detail": detail,
                "schemas": schema_fix.get(conn_id, schema_fix.get("default", {})),
                "columns": columns_fix.get(conn_id, columns_fix.get("default", {})),
                "connector_type": None,
            }
        )
    fivetran_raw = {
        "extracted_at": connections_fix.get("extracted_at"),
        "source": "fivetran",
        "connections": fivetran_connections,
        "errors": [],
    }

    dbt_raw = {
        "extracted_at": None,
        "source": "dbt",
        "projects": [],
        "environments": [],
        "jobs": [],
        "runs": [],
        "artifacts": {
            "manifest": _load_json(fixtures / "dbt_manifest.json"),
            "catalog": _load_json(fixtures / "dbt_catalog.json"),
            "run_results": _load_json(fixtures / "dbt_run_results.json"),
            "sources": _load_json(fixtures / "dbt_sources.json"),
        },
        "errors": [],
    }
    return fivetran_raw, dbt_raw
