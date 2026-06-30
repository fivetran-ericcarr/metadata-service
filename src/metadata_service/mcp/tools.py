"""MCP tool implementations as plain functions.

These contain the actual logic and are SDK-independent (so they are unit
testable and reusable). ``server.py`` binds them to the MCP protocol.

Tools are intentionally narrow and task-focused; no raw arbitrary-API tool is
exposed.
"""

from __future__ import annotations

from ..config import Settings, get_settings
from ..pipeline import build_and_store
from ..storage.base import get_storage


def _latest(settings: Settings) -> dict:
    latest = get_storage(settings).read_latest()
    if latest is None:
        raise RuntimeError("No metadata snapshot found. Call refresh_metadata first.")
    return latest


def refresh_metadata(
    fivetran_group_id: str | None = None,
    include_fivetran: bool = True,
    include_dbt: bool = True,
    settings: Settings | None = None,
) -> dict:
    settings = settings or get_settings()
    result = build_and_store(
        settings,
        group_id=fivetran_group_id,
        include_fivetran=include_fivetran,
        include_dbt=include_dbt,
    )
    return {
        "status": result["status"],
        "snapshot_uri": result["snapshot_uri"],
        "generated_at": result["generated_at"],
    }


def get_latest_metadata(scope: str = "all", settings: Settings | None = None) -> dict:
    settings = settings or get_settings()
    doc = _latest(settings)
    if scope == "fivetran":
        return doc.get("sources", {}).get("fivetran", {})
    if scope == "dbt":
        return doc.get("sources", {}).get("dbt", {})
    if scope == "warehouse_objects":
        return {"warehouse_objects": doc.get("warehouse_objects", [])}
    return doc


def get_warehouse_object(schema: str, table: str, settings: Settings | None = None) -> dict:
    settings = settings or get_settings()
    doc = _latest(settings)
    for obj in doc.get("warehouse_objects", []):
        if (obj.get("schema") or "").lower() == schema.lower() and (obj.get("name") or "").lower() == table.lower():
            return obj
    return {"found": False, "message": f"No warehouse object for schema={schema!r} table={table!r}."}


def get_dq_recommendations(
    schema: str | None = None,
    table: str | None = None,
    recommendation_type: str | None = None,
    confidence: str | None = None,
    risk: str | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """DQ recommendations, filterable per-object or across the whole snapshot.

    Filters (all optional, AND-combined): ``schema``/``table`` (target object),
    ``recommendation_type`` (dbt_test|risk|signal), ``confidence``
    (high|medium|heuristic), ``risk`` (e.g. missing_dbt_coverage). ``limit`` caps
    the returned list.
    """
    settings = settings or get_settings()
    doc = _latest(settings)
    recs = doc.get("dq_recommendations", [])
    if schema:
        recs = [r for r in recs if (r.get("target", {}).get("schema") or "").lower() == schema.lower()]
    if table:
        recs = [r for r in recs if (r.get("target", {}).get("table") or "").lower() == table.lower()]
    if recommendation_type:
        recs = [r for r in recs if r.get("recommendation_type") == recommendation_type]
    if confidence:
        recs = [r for r in recs if r.get("confidence") == confidence]
    if risk:
        recs = [r for r in recs if r.get("risk") == risk]
    total = len(recs)
    if limit is not None:
        recs = recs[:limit]
    return {"count": total, "returned": len(recs), "recommendations": recs}


def _stale_object_ids(doc: dict) -> set[str]:
    return {
        r.get("object_id")
        for r in doc.get("dq_recommendations", [])
        if r.get("risk") == "stale_fivetran_sync"
    }


def list_warehouse_objects(
    schema: str | None = None,
    risk_level: str | None = None,
    missing_coverage: bool | None = None,
    failing_tests: bool | None = None,
    stale: bool | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict:
    """Compact, filterable index of warehouse objects for agent triage.

    Returns small rows (no columns/tests payload). Filters (AND-combined):
    ``schema``, ``risk_level`` (low|medium|high), ``missing_coverage`` (unmatched
    to dbt), ``failing_tests`` (>0 failing), ``stale`` (Fivetran sync past
    threshold). Use ``get_warehouse_object`` for the full detail of one object.
    """
    settings = settings or get_settings()
    doc = _latest(settings)
    stale_ids = _stale_object_ids(doc)
    rows: list[dict] = []
    for o in doc.get("warehouse_objects", []):
        summary = o.get("dq_summary", {})
        dbt = o.get("dbt", {})
        has_coverage = bool(dbt.get("source_unique_id") or dbt.get("model_unique_ids"))
        is_failing = (summary.get("failing_tests_count") or 0) > 0
        is_missing = o.get("match_confidence") == "unmatched"
        is_stale = o.get("object_id") in stale_ids

        if schema and (o.get("schema") or "").lower() != schema.lower():
            continue
        if risk_level and summary.get("risk_level") != risk_level:
            continue
        if missing_coverage is not None and is_missing != missing_coverage:
            continue
        if failing_tests is not None and is_failing != failing_tests:
            continue
        if stale is not None and is_stale != stale:
            continue

        rows.append({
            "object_id": o.get("object_id"),
            "schema": o.get("schema"),
            "name": o.get("name"),
            "match_confidence": o.get("match_confidence"),
            "risk_level": summary.get("risk_level"),
            "has_dbt_coverage": has_coverage,
            "has_freshness_check": summary.get("has_freshness_check", False),
            "failing_tests_count": summary.get("failing_tests_count", 0),
            "recommended_tests_count": summary.get("recommended_tests_count", 0),
            "is_stale": is_stale,
        })
    total = len(rows)
    if limit is not None:
        rows = rows[:limit]
    return {"count": total, "returned": len(rows), "objects": rows}


def get_impact(schema: str, table: str, settings: Settings | None = None) -> dict:
    """Blast radius for an object: downstream dbt models and the exposures
    (dashboards/ML/apps) that depend on it. Answers 'what breaks if this is wrong?'."""
    settings = settings or get_settings()
    obj = get_warehouse_object(schema, table, settings=settings)
    if not obj.get("name"):
        return obj
    dbt = obj.get("dbt", {})
    return {
        "object_id": obj.get("object_id"),
        "schema": obj.get("schema"),
        "name": obj.get("name"),
        "downstream_models": dbt.get("model_unique_ids", []),
        "exposures": dbt.get("exposures", []),
        "has_failing_tests": (obj.get("dq_summary", {}) or {}).get("failing_tests_count", 0) > 0,
    }


def list_metrics(settings: Settings | None = None) -> dict:
    """Semantic Layer metrics with a trust level derived from the DQ posture of
    their upstream objects (trusted | watch | at_risk | unknown)."""
    settings = settings or get_settings()
    mq = _latest(settings).get("metric_quality", [])
    return {"count": len(mq), "metrics": [
        {"metric": m["metric"], "label": m.get("label"), "type": m.get("type"),
         "trust_level": m.get("trust_level"), "upstream_object_count": m.get("upstream_object_count")}
        for m in mq
    ]}


def get_metric_quality(metric: str, settings: Settings | None = None) -> dict:
    """Full trust detail for one governed metric: upstream objects + failing tests."""
    settings = settings or get_settings()
    for m in _latest(settings).get("metric_quality", []):
        if (m.get("metric") or "").lower() == metric.lower():
            return m
    return {"found": False, "message": f"No metric named {metric!r}."}


def get_dq_summary(settings: Settings | None = None) -> dict:
    """Account-level DQ rollup — the orienting call an agent makes first."""
    settings = settings or get_settings()
    doc = _latest(settings)
    objs = doc.get("warehouse_objects", [])
    recs = doc.get("dq_recommendations", [])
    drift = doc.get("schema_drift", [])
    stale_ids = _stale_object_ids(doc)

    risk_levels = {"low": 0, "medium": 0, "high": 0}
    matched = failing = missing = with_freshness = 0
    for o in objs:
        s = o.get("dq_summary", {})
        risk_levels[s.get("risk_level", "low")] = risk_levels.get(s.get("risk_level", "low"), 0) + 1
        if o.get("match_confidence") != "unmatched":
            matched += 1
        else:
            missing += 1
        if (s.get("failing_tests_count") or 0) > 0:
            failing += 1
        if s.get("has_freshness_check"):
            with_freshness += 1

    def tally(items, key):
        out: dict = {}
        for it in items:
            v = it.get(key)
            if v is not None:
                out[v] = out.get(v, 0) + 1
        return out

    return {
        "generated_at": doc.get("generated_at"),
        "object_count": len(objs),
        "matched": matched,
        "unmatched": len(objs) - matched,
        "risk_levels": risk_levels,
        "objects_with_failing_tests": failing,
        "objects_missing_dbt_coverage": missing,
        "objects_stale": len(stale_ids),
        "objects_with_freshness": with_freshness,
        "recommendations": {
            "total": len(recs),
            "by_type": tally(recs, "recommendation_type"),
            "by_confidence": tally(recs, "confidence"),
            "by_risk": tally(recs, "risk"),
        },
        "drift": {"total": len(drift), "by_severity": tally(drift, "severity")},
    }


def get_schema_drift(
    schema: str | None = None,
    table: str | None = None,
    severity: str | None = None,
    settings: Settings | None = None,
) -> dict:
    settings = settings or get_settings()
    doc = _latest(settings)
    drift = doc.get("schema_drift", [])
    if schema or table:
        target = f"/{(schema or '').lower()}/{(table or '').lower()}"
        drift = [d for d in drift if target in (d.get("object_id") or "").lower()]
    if severity:
        drift = [d for d in drift if d.get("severity") == severity]
    return {"count": len(drift), "drift": drift}
