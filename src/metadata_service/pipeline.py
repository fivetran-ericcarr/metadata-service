"""End-to-end orchestration: extract -> normalize -> combine -> (store).

Shared by the CLI, REST API, and MCP server so they all build snapshots the
same way. Supports an offline ``fixtures_dir`` mode that loads raw payloads from
JSON fixtures instead of calling live APIs.
"""

from __future__ import annotations

import contextlib
import json
import logging
import threading
from pathlib import Path

from .clients import ActivationsClient, DbtClient, FivetranClient
from .config import Settings
from .extractors import DbtExtractor, FivetranExtractor
from .extractors.activations_extractor import ActivationsExtractor
from .normalizers import (
    ActivationsNormalizer,
    CombinedNormalizer,
    DbtNormalizer,
    FivetranNormalizer,
)
from .exceptions import RefreshInProgressError
from .storage.base import get_storage

logger = logging.getLogger(__name__)

# One build at a time per process: concurrent refreshes race the latest.json
# write, double-bill the source APIs, and compute drift against the same
# previous snapshot.
_REFRESH_LOCK = threading.Lock()


def _acquire_file_lock(settings: Settings):
    """Best-effort cross-process build lock: an exclusive advisory flock on a
    lockfile in the snapshot directory. Serializes builds across processes on the
    same host (the common CLI-cron + serve-api + serve-mcp deployment).

    Returns an open file handle to hold, or None when file locking is
    unavailable (non-POSIX platform, or the directory can't be created).
    Cross-HOST serialization (e.g. many S3 writers) still needs external
    coordination — this does not provide it.
    """
    try:
        import fcntl  # noqa: PLC0415 - POSIX only
    except ImportError:  # pragma: no cover - non-POSIX
        return None
    lock_dir = Path(settings.metadata_local_path or ".")
    try:
        lock_dir.mkdir(parents=True, exist_ok=True)
        fh = open(lock_dir / ".build.lock", "w")  # noqa: SIM115 - held for the build
    except OSError:  # pragma: no cover - defensive
        return None
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        fh.close()
        raise RefreshInProgressError(
            "A metadata build is already running (build lock held by another process)."
        ) from exc
    return fh


def _release_file_lock(fh) -> None:
    try:
        import fcntl  # noqa: PLC0415

        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    except (ImportError, OSError):  # pragma: no cover
        pass
    finally:
        fh.close()


@contextlib.contextmanager
def _build_lock(settings: Settings):
    """Hold the in-process lock AND a cross-process file lock for one build."""
    if not _REFRESH_LOCK.acquire(blocking=False):
        raise RefreshInProgressError("A metadata build is already running in this process.")
    try:
        file_lock = _acquire_file_lock(settings)
    except BaseException:
        _REFRESH_LOCK.release()
        raise
    try:
        yield
    finally:
        if file_lock is not None:
            _release_file_lock(file_lock)
        _REFRESH_LOCK.release()


def build_metadata(
    settings: Settings,
    *,
    group_id: str | None = None,
    include_fivetran: bool = True,
    include_dbt: bool = True,
    include_activations: bool = True,
    fixtures_dir: str | None = None,
    aliases: dict | None = None,
    connected_only: bool = False,
    skip_paused: bool = False,
    dbt_project_id: int | None = None,
    dbt_job_id: int | None = None,
    enrich_warehouse: bool = True,
) -> dict:
    """Run extraction + normalization and return the normalized document."""
    enrichment = {"pk_enrichment": "not_requested", "column_schema_enrichment": "not_requested"}
    if fixtures_dir:
        fivetran_raw, dbt_raw, activations_raw = _load_fixture_payloads(Path(fixtures_dir))
        # Fixtures mode always loads all three payloads regardless of include_*,
        # so the *effective* coverage is whatever the fixture set provides.
        eff_fivetran, eff_dbt, eff_activations = True, True, True
    else:
        eff_fivetran = include_fivetran
        fivetran_raw = (
            _extract_fivetran(settings, group_id, connected_only=connected_only, skip_paused=skip_paused)
            if include_fivetran
            else _empty_fivetran()
        )
        # dbt degrades like activations when its credentials are absent, instead
        # of aborting the whole build — a Fivetran-only deployment must still
        # produce a snapshot.
        eff_dbt = include_dbt and settings.dbt_enabled()
        if eff_dbt:
            dbt_raw = _extract_dbt(settings, project_id=dbt_project_id, job_id=dbt_job_id)
        else:
            dbt_raw = _empty_dbt()
            if include_dbt and not settings.dbt_enabled():
                dbt_raw["errors"].append({
                    "source": "dbt", "error_type": "DbtNotConfigured",
                    "error_message": "dbt was requested but DBT_ACCOUNT_ID/DBT_SERVICE_TOKEN "
                                     "are not set; skipped (no dbt metadata in this snapshot).",
                })
        eff_activations = include_activations and settings.activations_enabled()
        activations_raw = _extract_activations(settings) if eff_activations else _empty_activations()

    fivetran_norm = FivetranNormalizer().normalize(fivetran_raw)
    if enrich_warehouse and not fixtures_dir and include_fivetran:
        enrichment = _enrich_from_warehouse(settings, fivetran_norm)
    dbt_norm = DbtNormalizer().normalize(dbt_raw)
    activations_norm = ActivationsNormalizer().normalize(activations_raw)
    doc = CombinedNormalizer(settings, aliases=aliases).build(fivetran_norm, dbt_norm, activations_norm)
    # Record what this build ACTUALLY covered (effective flags, not just what was
    # requested) so drift only compares like-for-like — a scoped/partial run
    # diffed against a full baseline mass-fires removed_table, and a build where a
    # source silently degraded must not diff as if that source were present.
    doc["build_scope"] = {
        "group_id": group_id,
        "include_fivetran": eff_fivetran,
        "include_dbt": eff_dbt,
        "include_activations": eff_activations,
        "connected_only": connected_only,
        "skip_paused": skip_paused,
        "dbt_project_id": dbt_project_id,
        "dbt_job_id": dbt_job_id,
        "fixtures": bool(fixtures_dir),
        "pk_enrichment": enrichment["pk_enrichment"],
        "column_schema_enrichment": enrichment["column_schema_enrichment"],
    }
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
    include_activations: bool = True,
    fixtures_dir: str | None = None,
    aliases: dict | None = None,
    connected_only: bool = False,
    skip_paused: bool = False,
    dbt_project_id: int | None = None,
    dbt_job_id: int | None = None,
    enrich_warehouse: bool = True,
    write_latest: bool = True,
) -> dict:
    """Build metadata, attach drift vs. the previous snapshot, persist, and return
    a summary ``{status, snapshot_uri, generated_at, object_count, error_count, doc}``.

    ``write_latest=False`` runs the full build (including drift vs. the current
    baseline) but does NOT persist — ``snapshot_uri`` is None and the baseline
    the MCP/REST readers serve is untouched.

    Raises :class:`RefreshInProgressError` if another build is already running in
    this process (REST maps it to 409).
    """
    from .dq.drift import detect_drift  # local import to avoid cycles

    with _build_lock(settings):
        storage = get_storage(settings)
        previous = storage.read_latest()

        doc = build_metadata(
            settings,
            group_id=group_id,
            include_fivetran=include_fivetran,
            include_dbt=include_dbt,
            include_activations=include_activations,
            fixtures_dir=fixtures_dir,
            aliases=aliases,
            connected_only=connected_only,
            skip_paused=skip_paused,
            dbt_project_id=dbt_project_id,
            dbt_job_id=dbt_job_id,
            enrich_warehouse=enrich_warehouse,
        )
        doc["schema_drift"] = detect_drift(previous, doc)
        status = _assess_build_health(doc, previous)
        uri = None
        if write_latest:
            # A degraded build (errors coinciding with a collapsed inventory) must
            # not become the served baseline — publishing an empty/partial snapshot
            # over a good one mass-fires false removed_table drift and feeds
            # downstream gates an empty inventory. Keep a forensic history file,
            # but leave latest.json pointing at the last good snapshot.
            promote = status != "degraded"
            uri = storage.write_snapshot(doc, update_latest=promote)
            if not promote:
                logger.error(
                    "Build DEGRADED (%s errors, %s objects vs %s previously) — wrote history "
                    "snapshot %s but left latest.json unchanged.",
                    len(doc.get("errors", [])), len(doc.get("warehouse_objects", [])),
                    len((previous or {}).get("warehouse_objects", [])), uri,
                )
        else:
            logger.info("Snapshot NOT persisted (write_latest=False); latest.json unchanged.")

        return {
            "status": status,
            "latest_updated": bool(uri) and status != "degraded",
            "snapshot_uri": uri,
            "generated_at": doc.get("generated_at"),
            "object_count": len(doc.get("warehouse_objects", [])),
            "error_count": len(doc.get("errors", [])),
            "doc": doc,
        }


# Fraction of the previous snapshot's object inventory a build must retain to be
# trusted as the new baseline when it also recorded errors.
_MIN_INVENTORY_RATIO = 0.5


def _assess_build_health(doc: dict, previous: dict | None) -> str:
    """Classify a build: ``success`` (clean), ``partial`` (errored but inventory
    intact), or ``degraded`` (errored AND lost most/all of its objects — do not
    promote to latest). A clean build is always ``success``; errors alone never
    block promotion, only errors *with* a collapsed inventory do."""
    errors = doc.get("errors") or []
    if not errors:
        return "success"
    obj_count = len(doc.get("warehouse_objects") or [])
    if obj_count == 0:
        return "degraded"  # never let an errored, empty snapshot become the baseline
    prev_count = len((previous or {}).get("warehouse_objects") or [])
    if prev_count and obj_count < prev_count * _MIN_INVENTORY_RATIO:
        return "degraded"  # errored build lost >half its inventory — untrustworthy
    return "partial"


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


def _enrich_from_warehouse(settings: Settings, fivetran_norm: dict) -> dict:
    """Enrich normalized columns from the destination warehouse (best-effort).

    Two reads against the same reader object, each hitting a DIFFERENT
    Snowflake object and so failing independently in practice: PK flags from
    the Fivetran Platform Connector's ``fivetran_metadata`` schema (which only
    exists once that connector has been added and synced at least once — a
    separate setup step from granting warehouse credentials), and
    data_type/max_length/nullable from the destination's own
    INFORMATION_SCHEMA (which needs no Platform Connector at all — any role
    with SELECT on the database can read it). A role/setup that satisfies one
    and not the other is the common case, not the exception, so each read gets
    its own guard and its own outcome rather than one failure masking a read
    that would otherwise have succeeded.

    Returns ``{"pk_enrichment": ..., "column_schema_enrichment": ...}`` for
    build_scope, each ``not_requested`` (caller didn't run this function at
    all), ``unavailable`` (no reader configured/supported), ``ran``, or
    ``failed``. Keeping them as two keys (not collapsed into one) matters for
    drift: two builds where PK enrichment succeeded in one and failed in the
    other must not be diffed as like-for-like just because their column-schema
    outcome happens to match.

    The reader construction is inside the guard too: a misconfigured reader
    (e.g. a bad WAREHOUSE_DATABASE) must not abort the whole build — enrichment
    is best-effort.
    """
    from .warehouse import (
        apply_column_schema,
        apply_primary_keys,
        destination_from_dict,
        get_warehouse_reader,
    )

    reader = None
    try:
        destination = destination_from_dict(fivetran_norm.get("destination"))
        reader = get_warehouse_reader(settings, destination)
        if reader is None:
            return {"pk_enrichment": "unavailable", "column_schema_enrichment": "unavailable"}

        pk_outcome = "failed"
        try:
            connection_ids = [c.get("connection_id") for c in fivetran_norm.get("connections", [])
                              if c.get("connection_id")]
            pk_map = reader.read_primary_keys(connection_ids or None)
            updated = apply_primary_keys(fivetran_norm, pk_map)
            logger.info("Enriched %s primary-key columns from fivetran_metadata", updated)
            pk_outcome = "ran"
        except Exception as exc:
            # The reader was EXPLICITLY configured, so a silent skip means PKs
            # quietly stop being authoritative — log at ERROR, not a warning
            # nobody reads. Does NOT skip the column-schema read below: that
            # one has no dependency on fivetran_metadata existing at all.
            logger.error("Warehouse PK enrichment FAILED (reader is configured): %s", exc)

        column_outcome = "failed"
        try:
            schemas = _destination_schemas(fivetran_norm)
            column_map = reader.read_column_schema(schemas or None)
            enriched = apply_column_schema(fivetran_norm, column_map)
            logger.info("Enriched %s columns with data_type/max_length/nullable from "
                        "INFORMATION_SCHEMA", enriched)
            column_outcome = "ran"
        except Exception as exc:
            logger.error("Column-schema enrichment FAILED (PK enrichment unaffected): %s", exc)

        return {"pk_enrichment": pk_outcome, "column_schema_enrichment": column_outcome}
    except Exception as exc:  # reader construction itself blew up
        logger.error("Warehouse reader construction FAILED (reader is configured): %s", exc)
        return {"pk_enrichment": "failed", "column_schema_enrichment": "failed"}
    finally:
        if reader is not None:
            try:
                reader.close()
            except Exception:  # pragma: no cover
                pass


def _destination_schemas(fivetran_norm: dict) -> list[str]:
    """Distinct destination schemas in the snapshot, to bound the column read."""
    schemas = {
        (table.get("destination_schema") or "")
        for conn in fivetran_norm.get("connections", []) or []
        for table in conn.get("tables", []) or []
    }
    return sorted(s for s in schemas if s)


def _extract_activations(settings: Settings) -> dict:
    with ActivationsClient(settings) as client:
        return ActivationsExtractor(client).extract(source_database=settings.warehouse_database)


def _extract_dbt(settings: Settings, *, project_id: int | None = None, job_id: int | None = None) -> dict:
    settings.require_dbt()
    with DbtClient(settings) as client:
        return DbtExtractor(client, settings.dbt_account_id or "").extract(
            project_id=project_id, job_id=job_id
        )


def _empty_fivetran() -> dict:
    return {"extracted_at": None, "source": "fivetran", "destination": None,
            "connections": [], "errors": []}


def _empty_dbt() -> dict:
    return {"extracted_at": None, "source": "dbt", "artifacts": {}, "errors": []}


def _empty_activations() -> dict:
    return {"extracted_at": None, "source": "activations", "syncs": [],
            "sources": {}, "destinations": {}, "errors": []}


# -- fixtures mode --------------------------------------------------------
def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _load_fixture_payloads(fixtures: Path) -> tuple[dict, dict, dict]:
    """Assemble raw Fivetran + dbt + activations payloads from fixture files (offline build)."""
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
        # Optional: lets an offline build exercise destination detection and the
        # `database` field without reaching the API.
        "destination": _load_json(fixtures / "fivetran_destination.json") or None,
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

    activations_fix = _load_json(fixtures / "activations_syncs.json")
    activations_raw = activations_fix or _empty_activations()
    return fivetran_raw, dbt_raw, activations_raw
