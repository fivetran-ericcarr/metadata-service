"""Shared constants and helpers used across the metadata pipeline."""

from __future__ import annotations

from datetime import datetime, timezone

SCHEMA_VERSION = "1.0"

# Confidence levels for DQ recommendations.
CONFIDENCE_HIGH = "high"
CONFIDENCE_MEDIUM = "medium"
CONFIDENCE_HEURISTIC = "heuristic"

# Match confidence levels for joining Fivetran and dbt objects.
MATCH_EXACT_SCHEMA_TABLE = "exact_schema_table"
MATCH_CASE_INSENSITIVE = "case_insensitive_schema_table"
MATCH_CONFIGURED_ALIAS = "configured_alias"
MATCH_UNMATCHED = "unmatched"


def utcnow_iso() -> str:
    """UTC timestamp in ISO-8601 / Zulu form, e.g. ``2026-06-25T12:34:56Z``."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def snapshot_timestamp() -> str:
    """Filesystem-safe UTC timestamp, e.g. ``2026-06-25T12-34-56-123456Z``.

    Microsecond resolution so two builds in the same wall-clock second don't
    generate the same filename and silently overwrite one snapshot's history.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S-%fZ")


def build_object_id(
    database: str | None,
    schema: str | None,
    table: str | None,
) -> str:
    """Build a stable, warehouse-agnostic object id.

    Format: ``warehouse://database/schema/table``. Missing parts become
    ``unknown``. Lower-cased so Fivetran (which usually lower-cases destination
    identifiers) and dbt relations join deterministically.

    The ``warehouse://`` scheme is fixed and deliberately warehouse-agnostic
    (per ARTIFACTS.md): deriving it from WAREHOUSE_TYPE would rewrite every id
    when the Snowflake reader is enabled, mass-firing removed_table/new_table
    drift and orphaning any persisted references (waiver targets, tickets).
    """
    db = (database or "unknown").lower()
    sc = (schema or "unknown").lower()
    tb = (table or "unknown").lower()
    return f"warehouse://{db}/{sc}/{tb}"
