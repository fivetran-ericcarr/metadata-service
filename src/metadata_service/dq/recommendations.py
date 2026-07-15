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
from .status import FAILING_STATUSES as _FAILING_STATUSES

_CATEGORICAL_TOKENS = {"status", "type", "category", "kind", "state", "stage"}
_BOOLEAN_PREFIXES = ("is_", "has_")
# Distinctive substrings that suggest a column holds personal / sensitive data.
_PII_TOKENS = (
    "email", "ssn", "social_security", "phone", "birth", "dob", "passport",
    "credit_card", "card_number", "ip_address", "routing_number", "account_number",
    "salary", "postal_code", "zipcode", "national_id", "tax_id", "drivers_license",
)
# Column names that commonly act as natural keys (worth a uniqueness test).
_NATURAL_KEY_NAMES = {"email", "username", "slug", "uuid", "guid", "external_id"}


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

    if composite_pk and not _has_combination_test(dbt_section):
        recs.append(_dbt_test(
            object_id, "dbt_utils.unique_combination_of_columns",
            {**target_base, "columns": [c.get("name") for c in pk_columns]},
            CONFIDENCE_HIGH, "Fivetran reports a composite primary key on this table."))

    # --- Ambiguous key columns (PK *or* FK; SaaS/SDK connectors don't disambiguate)
    for col in columns:
        if col.get("key_constraint") == "primary_or_foreign_key" and not col.get("is_primary_key"):
            existing = {t.lower() for t in (col.get("dbt_tests") or [])}
            if "not_null" not in existing:
                recs.append(_dbt_test(
                    object_id, "not_null", {**target_base, "column": col.get("name")},
                    CONFIDENCE_MEDIUM,
                    "Fivetran locks this column as a primary or foreign key (cannot disambiguate)."))

    # --- Freshness --------------------------------------------------------
    if dbt_section.get("source_unique_id"):
        freshness = dbt_section.get("freshness")
        # "configured: True" means freshness IS set up but the sources.json
        # artifact was missing this run — recommending it again would be false.
        if not freshness or (freshness.get("status") is None and not freshness.get("configured")):
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

        # Accepted values (heuristic): categorical-named, else boolean-style.
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
        elif lowered.startswith(_BOOLEAN_PREFIXES) and "accepted_values" not in existing:
            recs.append({
                "object_id": object_id,
                "recommendation_type": "dbt_test",
                "test_name": "accepted_values",
                "target": {**col_target, "values": [True, False]},
                "reason": "Column name suggests a boolean flag.",
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

        # Natural-key uniqueness (heuristic) — skip hashed (can't reason about it).
        if (lowered in _NATURAL_KEY_NAMES and not col.get("is_primary_key")
                and not col.get("hashed") and "unique" not in existing):
            recs.append({
                "object_id": object_id,
                "recommendation_type": "dbt_test",
                "test_name": "unique",
                "target": dict(col_target),
                "reason": "Column name suggests a natural key.",
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

        # Potential PII (heuristic) — name-based, only when NOT already hashed.
        elif any(tok in lowered for tok in _PII_TOKENS):
            recs.append({
                "object_id": object_id,
                "recommendation_type": "signal",
                "signal": "potential_pii",
                "target": dict(col_target),
                "recommended_action": "Column name suggests PII; review for masking/hashing and access controls.",
                "confidence": CONFIDENCE_HEURISTIC,
            })

    # --- Object-level risks ----------------------------------------------
    match_confidence = obj.get("match_confidence")
    is_matched = match_confidence not in (None, "unmatched")
    if not is_matched and obj.get("origin", {}).get("enabled", True):
        recs.append({
            "object_id": object_id,
            "recommendation_type": "risk",
            "risk": "missing_dbt_coverage",
            "severity": "medium",
            "reason": "Table is enabled in Fivetran but no dbt source or model match exists.",
            "target": dict(target_base),
        })

    # Matched to dbt but carrying no tests at all — a real coverage gap distinct
    # from a fully unmatched table.
    has_dbt = dbt_section.get("source_unique_id") or dbt_section.get("model_unique_ids")
    if is_matched and has_dbt and not (dbt_section.get("tests") or []):
        recs.append({
            "object_id": object_id,
            "recommendation_type": "risk",
            "risk": "untested_dbt_object",
            "severity": "medium",
            "reason": "Object is modeled in dbt but has no tests.",
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
    stale = _is_stale(last_sync, stale_threshold_hours, now=now)
    if stale:
        recs.append({
            "object_id": object_id,
            "recommendation_type": "risk",
            "risk": "stale_fivetran_sync",
            "severity": "high",
            "reason": f"Last successful Fivetran sync is older than {stale_threshold_hours}h.",
            "target": dict(target_base),
        })

    # Business impact: a DQ problem on this object reaches downstream consumers.
    exposures = dbt_section.get("exposures") or []
    if exposures and (_has_failing_tests(dbt_section) or stale or not is_matched):
        recs.append({
            "object_id": object_id,
            "recommendation_type": "risk",
            "risk": "impacts_exposure",
            "severity": "high",
            "reason": "A data-quality problem on this object affects downstream exposures.",
            "target": dict(target_base),
            "details": {"exposures": [
                {"name": e.get("name"), "type": e.get("type"), "maturity": e.get("maturity")}
                for e in exposures
            ]},
        })

    # Metric trust: a DQ problem on this object reaches a governed Semantic Layer metric.
    metrics = dbt_section.get("metrics") or []
    if metrics and (_has_failing_tests(dbt_section) or stale or not is_matched):
        recs.append({
            "object_id": object_id,
            "recommendation_type": "risk",
            "risk": "metric_at_risk",
            "severity": "high",
            "reason": "A data-quality problem on this object affects governed metrics.",
            "target": dict(target_base),
            "details": {"metrics": [{"name": m.get("name"), "type": m.get("type")} for m in metrics]},
        })

    # Governance gaps on matched, modeled objects.
    governance = dbt_section.get("governance") or {}
    if is_matched and has_dbt:
        if not governance.get("has_enforced_contract"):
            recs.append({
                "object_id": object_id,
                "recommendation_type": "risk",
                "risk": "missing_model_contract",
                "severity": "high" if governance.get("uncontracted_public_models") else "medium",
                "reason": "Downstream dbt model(s) have no enforced contract"
                          + (" and are publicly accessible." if governance.get("uncontracted_public_models") else "."),
                "target": dict(target_base),
            })
        if not governance.get("owners") and not governance.get("groups"):
            recs.append({
                "object_id": object_id,
                "recommendation_type": "risk",
                "risk": "unowned_object",
                "severity": "medium",
                "reason": "Object is modeled in dbt but has no owner or group assigned.",
                "target": dict(target_base),
            })

    return recs


def activation_risk(obj: dict, activations: list[dict]) -> dict | None:
    """Risk when a warehouse object feeds a reverse-ETL activation that the gate
    is *not* clearing. Bad data pushed back to an operational system corrupts a
    system of record, so this is the highest-consequence blast radius we track.
    """
    blocking = [a for a in activations if a.get("readiness_verdict") in ("block", "warn")]
    if not blocking:
        return None
    worst = "block" if any(a.get("readiness_verdict") == "block" for a in blocking) else "warn"
    return {
        "object_id": obj.get("object_id"),
        "recommendation_type": "risk",
        "risk": "activates_bad_data",
        "severity": "high" if worst == "block" else "medium",
        "reason": ("This object feeds a reverse-ETL activation the readiness gate is blocking; "
                   "syncing would push unvalidated data into an operational system."
                   if worst == "block" else
                   "This object feeds a reverse-ETL activation with unresolved governance gaps."),
        "target": {"schema": obj.get("schema"), "table": obj.get("name")},
        "details": {"activations": [
            {"sync_id": a.get("sync_id"), "label": a.get("label"),
             "destination_name": a.get("destination_name"),
             "destination_object": a.get("destination_object"),
             "verdict": a.get("readiness_verdict")}
            for a in blocking
        ]},
    }


# -- helpers --------------------------------------------------------------
def _has_combination_test(dbt_section: dict) -> bool:
    """True when a unique_combination_of_columns test already exists on the
    object. These tests have no attached_column, so per-column existence checks
    can't see them — without this, the composite-PK rec refires forever."""
    for test in dbt_section.get("tests") or []:
        blob = f"{test.get('test_type') or ''} {test.get('name') or ''}".lower()
        if "unique_combination_of_columns" in blob:
            return True
    return False


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


def _parse_dt(value) -> datetime | None:
    # Connectors occasionally report timestamps as epoch numbers rather than
    # ISO-8601 strings. datetime.fromisoformat(str(epoch)) raises, so these used
    # to silently parse to None and read as "not stale" — a fail-open hole.
    # Handle numeric epochs (int/float, or a bare-digit string) explicitly.
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _from_epoch(float(value))
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return _from_epoch(float(raw))
    except ValueError:
        pass  # not a bare number — fall through to ISO parsing
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _from_epoch(epoch: float) -> datetime | None:
    # Values too large to be plausible seconds are milliseconds (1e11 s ~ year
    # 5138). Reject anything that still can't be represented as a UTC datetime.
    if abs(epoch) > 1e11:
        epoch /= 1000.0
    try:
        return datetime.fromtimestamp(epoch, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None
