"""Data Quality recommendation rules.

Recommendations are generated deterministically from joined Fivetran + dbt
metadata. Explicit recommendations (high/medium confidence) are kept separate
from heuristic ones (confidence == "heuristic").

Each recommendation is a dict with a ``recommendation_type`` of:
  - ``dbt_test``  : suggest a dbt test (has ``test_name`` + ``target``)
  - ``risk``      : a data-quality risk (has ``risk`` + ``severity``)
  - ``signal``    : an informational signal (has ``signal`` + ``recommended_action``)
"""

from __future__ import annotations

from datetime import datetime, timezone

from ..models.common import CONFIDENCE_HEURISTIC, CONFIDENCE_HIGH, CONFIDENCE_MEDIUM

_FAILING_STATUSES = {"fail", "error", "runtime error"}
_CATEGORICAL_TOKENS = {"status", "type", "category", "kind", "state", "stage"}


def generate_recommendations(warehouse_objects: list[dict], *, stale_threshold_hours: int = 24,
                             now: datetime | None = None) -> list[dict]:
    """Generate the flat ``dq_recommendations`` list across all objects."""
    recs: list[dict] = []
    for obj in warehouse_objects:
        recs.extend(recommend_for_object(obj, stale_threshold_hours=stale_threshold_hours, now=now))
    return recs


def recommend_for_object(obj: dict, *, stale_threshold_hours: int = 24,
                         now: datetime | None = None) -> list[dict]:
    """Return all recommendations/risks/signals for a single warehouse object."""
    object_id = obj.get("object_id")
    schema = obj.get("schema")
    table = obj.get("name")
    target_base = {"schema": schema, "table": table}
    columns = obj.get("columns") or []
    dbt_section = obj.get("dbt") or {}
    recs: list[dict] = []

    pk_columns = [c for c in columns if c.get("is_primary_key")]
    composite_pk = len(pk_columns) > 1

    # --- Primary keys -> not_null + unique --------------------------------
    for col in pk_columns:
        existing = {t.lower() for t in (col.get("dbt_tests") or [])}
        col_target = {**target_base, "column": col.get("name")}
        if "not_null" not in existing:
            recs.append(_dbt_test(object_id, "not_null", col_target, CONFIDENCE_HIGH,
                                  "Fivetran marks this column as a primary key."))
        if not composite_pk and "unique" not in existing:
            recs.append(_dbt_test(object_id, "unique", col_target, CONFIDENCE_HIGH,
                                  "Fivetran marks this column as a primary key."))

    if composite_pk:
        recs.append(_dbt_test(
            object_id, "dbt_utils.unique_combination_of_columns",
            {**target_base, "columns": [c.get("name") for c in pk_columns]},
            CONFIDENCE_HIGH, "Fivetran reports a composite primary key on this table."))

    # --- Freshness --------------------------------------------------------
    if dbt_section.get("source_unique_id"):
        freshness = dbt_section.get("freshness")
        if not freshness or freshness.get("status") is None:
            recs.append({
                "object_id": object_id,
                "recommendation_type": "dbt_test",
                "test_name": "source_freshness",
                "target": dict(target_base),
                "reason": "Fivetran-originated dbt source has no freshness check configured.",
                "confidence": CONFIDENCE_MEDIUM,
                "source": "fivetran_metadata",
            })

    # --- Per-column heuristics + signals ----------------------------------
    for col in columns:
        name = (col.get("name") or "")
        lowered = name.lower()
        existing = {t.lower() for t in (col.get("dbt_tests") or [])}
        col_target = {**target_base, "column": name}

        # Accepted values (heuristic)
        if _looks_categorical(lowered) and "accepted_values" not in existing:
            recs.append({
                "object_id": object_id,
                "recommendation_type": "dbt_test",
                "test_name": "accepted_values",
                "target": dict(col_target),
                "reason": "Column name suggests a categorical field.",
                "confidence": CONFIDENCE_HEURISTIC,
                "source": "heuristic",
            })

        # Relationships (heuristic) for *_id non-PK columns
        if lowered.endswith("_id") and not col.get("is_primary_key") and "relationships" not in existing:
            recs.append({
                "object_id": object_id,
                "recommendation_type": "dbt_test",
                "test_name": "relationships",
                "target": dict(col_target),
                "reason": "Column ends with '_id' and may reference another table.",
                "confidence": CONFIDENCE_HEURISTIC,
                "source": "heuristic",
            })

        # Hashed / sensitive columns
        if col.get("hashed"):
            recs.append({
                "object_id": object_id,
                "recommendation_type": "signal",
                "signal": "hashed_column",
                "target": dict(col_target),
                "recommended_action": "Verify downstream models do not expect the raw value.",
            })

    # --- Object-level risks ----------------------------------------------
    match_confidence = obj.get("match_confidence")
    if match_confidence in (None, "unmatched") and obj.get("origin", {}).get("enabled", True):
        recs.append({
            "object_id": object_id,
            "recommendation_type": "risk",
            "risk": "missing_dbt_coverage",
            "severity": "medium",
            "reason": "Table is enabled in Fivetran but no dbt source or model match exists.",
            "target": dict(target_base),
        })

    if _has_failing_tests(dbt_section):
        recs.append({
            "object_id": object_id,
            "recommendation_type": "risk",
            "risk": "failing_dbt_tests",
            "severity": "high",
            "reason": "One or more dbt tests are failing for this object.",
            "target": dict(target_base),
        })

    last_sync = (obj.get("origin") or {}).get("last_successful_sync")
    if _is_stale(last_sync, stale_threshold_hours, now=now):
        recs.append({
            "object_id": object_id,
            "recommendation_type": "risk",
            "risk": "stale_fivetran_sync",
            "severity": "high",
            "reason": f"Last successful Fivetran sync is older than {stale_threshold_hours}h.",
            "target": dict(target_base),
        })

    return recs


# -- helpers --------------------------------------------------------------
def _dbt_test(object_id, test_name, target, confidence, reason) -> dict:
    return {
        "object_id": object_id,
        "recommendation_type": "dbt_test",
        "test_name": test_name,
        "target": target,
        "reason": reason,
        "confidence": confidence,
        "source": "fivetran_metadata",
    }


def _looks_categorical(lowered_name: str) -> bool:
    tokens = set(lowered_name.replace("-", "_").split("_"))
    return bool(tokens & _CATEGORICAL_TOKENS)


def _has_failing_tests(dbt_section: dict) -> bool:
    for test in dbt_section.get("tests") or []:
        status = (test.get("status") or "").lower()
        if status in _FAILING_STATUSES:
            return True
    freshness = dbt_section.get("freshness") or {}
    return (freshness.get("status") or "").lower() in _FAILING_STATUSES


def _is_stale(last_sync: str | None, threshold_hours: int, now: datetime | None = None) -> bool:
    if not last_sync:
        return False
    parsed = _parse_dt(last_sync)
    if parsed is None:
        return False
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    age_hours = (now - parsed).total_seconds() / 3600.0
    return age_hours > threshold_hours


def _parse_dt(value: str) -> datetime | None:
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt
