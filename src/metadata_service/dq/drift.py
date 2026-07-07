"""Schema / metadata drift detection between two normalized snapshots."""

from __future__ import annotations

import logging

from ..models.common import utcnow_iso

logger = logging.getLogger(__name__)

SEVERITY = {
    "new_table": "low",
    "removed_table": "high",
    "disabled_table": "high",
    "new_column": "medium",
    "removed_column": "high",
    "disabled_column": "high",
    "primary_key_changed": "high",
    "hashing_changed": "high",
    "destination_name_changed": "high",
    "dbt_test_added": "low",
    "dbt_test_removed": "high",
    "dbt_test_status_changed": "high",
    "freshness_status_changed": "high",
}


def detect_drift(previous: dict | None, latest: dict | None) -> list[dict]:
    """Compare two normalized snapshots and return a list of drift records.

    Only like-for-like builds are compared: a scoped/partial run (different
    group, ``--no-fivetran``, different filters) diffed against a full baseline
    would mass-fire high-severity ``removed_table`` records for everything the
    scope excluded, so scope mismatches skip drift instead.
    """
    if not previous or not latest:
        return []

    prev_scope = previous.get("build_scope")
    latest_scope = latest.get("build_scope")
    if prev_scope != latest_scope:
        logger.info(
            "Drift skipped: build scopes differ (previous=%s, latest=%s); "
            "comparing them would report scope changes as schema drift.",
            prev_scope, latest_scope,
        )
        return []

    prev_objs = _index_objects(previous)
    latest_objs = _index_objects(latest)
    records: list[dict] = []

    for object_id in latest_objs.keys() - prev_objs.keys():
        records.append(_record(object_id, "new_table", {}))
    for object_id in prev_objs.keys() - latest_objs.keys():
        records.append(_record(object_id, "removed_table", {}))

    for object_id in latest_objs.keys() & prev_objs.keys():
        records.extend(_compare_object(object_id, prev_objs[object_id], latest_objs[object_id]))

    return records


def _compare_object(object_id: str, prev: dict, latest: dict) -> list[dict]:
    records: list[dict] = []

    prev_enabled = (prev.get("origin") or {}).get("enabled", True)
    latest_enabled = (latest.get("origin") or {}).get("enabled", True)
    if prev_enabled and not latest_enabled:
        records.append(_record(object_id, "disabled_table", {}))

    records.extend(_compare_columns(object_id, prev, latest))
    records.extend(_compare_tests(object_id, prev, latest))
    records.extend(_compare_freshness(object_id, prev, latest))
    return records


def _compare_columns(object_id: str, prev: dict, latest: dict) -> list[dict]:
    records: list[dict] = []
    prev_cols = {c.get("source_name"): c for c in (prev.get("columns") or []) if c.get("source_name")}
    latest_cols = {c.get("source_name"): c for c in (latest.get("columns") or []) if c.get("source_name")}

    for src_name in latest_cols.keys() - prev_cols.keys():
        records.append(_record(object_id, "new_column", {"column": latest_cols[src_name].get("name")}))
    for src_name in prev_cols.keys() - latest_cols.keys():
        records.append(_record(object_id, "removed_column", {"column": prev_cols[src_name].get("name")}))

    for src_name in latest_cols.keys() & prev_cols.keys():
        p, c = prev_cols[src_name], latest_cols[src_name]
        if p.get("enabled", True) and not c.get("enabled", True):
            records.append(_record(object_id, "disabled_column", {"column": c.get("name")}))
        if bool(p.get("is_primary_key")) != bool(c.get("is_primary_key")):
            records.append(_record(object_id, "primary_key_changed",
                                   {"column": c.get("name"), "from": p.get("is_primary_key"), "to": c.get("is_primary_key")}))
        if bool(p.get("hashed")) != bool(c.get("hashed")):
            records.append(_record(object_id, "hashing_changed",
                                   {"column": c.get("name"), "from": p.get("hashed"), "to": c.get("hashed")}))
        if p.get("name") != c.get("name"):
            records.append(_record(object_id, "destination_name_changed",
                                   {"source_name": src_name, "from": p.get("name"), "to": c.get("name")}))
    return records


def _compare_tests(object_id: str, prev: dict, latest: dict) -> list[dict]:
    records: list[dict] = []
    prev_tests = {t.get("unique_id"): t for t in (prev.get("dbt") or {}).get("tests") or [] if t.get("unique_id")}
    latest_tests = {t.get("unique_id"): t for t in (latest.get("dbt") or {}).get("tests") or [] if t.get("unique_id")}

    for uid in latest_tests.keys() - prev_tests.keys():
        records.append(_record(object_id, "dbt_test_added", {"test": latest_tests[uid].get("name")}))
    for uid in prev_tests.keys() - latest_tests.keys():
        records.append(_record(object_id, "dbt_test_removed", {"test": prev_tests[uid].get("name")}))
    for uid in latest_tests.keys() & prev_tests.keys():
        p_status = prev_tests[uid].get("status")
        c_status = latest_tests[uid].get("status")
        if p_status != c_status:
            records.append(_record(object_id, "dbt_test_status_changed",
                                   {"test": latest_tests[uid].get("name"), "from": p_status, "to": c_status},
                                   severity=_status_change_severity(c_status)))
    return records


def _compare_freshness(object_id: str, prev: dict, latest: dict) -> list[dict]:
    p = ((prev.get("dbt") or {}).get("freshness") or {}).get("status")
    c = ((latest.get("dbt") or {}).get("freshness") or {}).get("status")
    if p != c:
        return [_record(object_id, "freshness_status_changed", {"from": p, "to": c},
                        severity=_status_change_severity(c))]
    return []


def _status_change_severity(new_status) -> str:
    """A status change is only high-severity when it changed INTO a bad state —
    fail -> pass is an improvement, not a high-severity alert."""
    return "high" if (str(new_status or "")).lower() in {"fail", "error", "warn", "runtime error"} else "low"


def _index_objects(doc: dict) -> dict[str, dict]:
    return {obj.get("object_id"): obj for obj in doc.get("warehouse_objects") or [] if obj.get("object_id")}


def _record(object_id: str, change_type: str, details: dict, severity: str | None = None) -> dict:
    return {
        "detected_at": utcnow_iso(),
        "object_id": object_id,
        "change_type": change_type,
        "severity": severity or SEVERITY.get(change_type, "medium"),
        "details": details,
    }
