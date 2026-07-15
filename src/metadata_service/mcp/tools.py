"""MCP tool implementations as plain functions.

These contain the actual logic and are SDK-independent (so they are unit
testable and reusable). ``server.py`` binds them to the MCP protocol.

Tools are intentionally narrow and task-focused; no raw arbitrary-API tool is
exposed.
"""

from __future__ import annotations

import logging

from ..config import Settings, get_settings
from ..models.common import SCHEMA_VERSION
from ..pipeline import build_and_store
from ..storage.base import get_storage

logger = logging.getLogger(__name__)


def _latest(settings: Settings) -> dict:
    latest = get_storage(settings).read_latest()
    if latest is None:
        raise RuntimeError("No metadata snapshot found. Call refresh_metadata first.")
    version = latest.get("version")
    if version != SCHEMA_VERSION:
        logger.warning("Snapshot schema version %r differs from the reader's %r; "
                       "fields added since then read as empty defaults.", version, SCHEMA_VERSION)
    return latest


def refresh_metadata(
    fivetran_group_id: str | None = None,
    include_fivetran: bool = True,
    include_dbt: bool = True,
    include_activations: bool = True,
    dbt_project_id: int | None = None,
    connected_only: bool = False,
    skip_paused: bool = False,
    settings: Settings | None = None,
) -> dict:
    """Rebuild the snapshot. Accepts the same scoping as the CLI build — an
    agent-triggered refresh should produce the SAME snapshot as the scheduled
    one, or drift/scope semantics fall apart."""
    settings = settings or get_settings()
    result = build_and_store(
        settings,
        group_id=fivetran_group_id,
        include_fivetran=include_fivetran,
        include_dbt=include_dbt,
        include_activations=include_activations,
        dbt_project_id=dbt_project_id,
        connected_only=connected_only,
        skip_paused=skip_paused,
    )
    return {
        "status": result["status"],
        "snapshot_uri": result["snapshot_uri"],
        "generated_at": result["generated_at"],
    }


def get_latest_metadata(scope: str = "all", settings: Settings | None = None) -> dict:
    """Snapshot access by scope. ``all`` returns the joined document with the
    raw per-source payloads replaced by counts (they dominate the ~1 MB size and
    blow agent context); use ``fivetran``/``dbt`` for a raw section or ``full``
    for the verbatim document."""
    settings = settings or get_settings()
    doc = _latest(settings)
    if scope == "fivetran":
        return doc.get("sources", {}).get("fivetran", {})
    if scope == "dbt":
        return doc.get("sources", {}).get("dbt", {})
    if scope == "warehouse_objects":
        return {"warehouse_objects": doc.get("warehouse_objects", [])}
    if scope == "full":
        return doc
    slim = {k: v for k, v in doc.items() if k != "sources"}
    sources = doc.get("sources", {})
    slim["sources"] = {
        name: {"extracted_at": (section or {}).get("extracted_at"),
               "sizes": {k: len(v) for k, v in (section or {}).items() if isinstance(v, list)},
               "hint": f"use get_latest_metadata(scope='{name}') for the raw section"}
        for name, section in sources.items()
    }
    return slim


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
    offset: int = 0,
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
    recs = recs[offset:]
    if limit is not None:
        recs = recs[:limit]
    return {"count": total, "returned": len(recs), "offset": offset, "recommendations": recs}


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
    warn_test_failures: bool | None = None,
    stale: bool | None = None,
    limit: int | None = None,
    offset: int = 0,
    settings: Settings | None = None,
) -> dict:
    """Compact, filterable index of warehouse objects for agent triage.

    Returns small rows (no columns/tests payload). Filters (AND-combined):
    ``schema``, ``risk_level`` (low|medium|high), ``missing_coverage`` (unmatched
    to dbt), ``failing_tests`` (>0 failing), ``warn_test_failures`` (warn-severity
    tests firing — the run is green but rows are failing), ``stale`` (Fivetran
    sync past threshold). Use ``get_warehouse_object`` for the full detail.
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
        is_warn_firing = (summary.get("warn_tests_with_failures_count") or 0) > 0
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
        if warn_test_failures is not None and is_warn_firing != warn_test_failures:
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
            "warn_tests_with_failures_count": summary.get("warn_tests_with_failures_count", 0),
            "recommended_tests_count": summary.get("recommended_tests_count", 0),
            "is_stale": is_stale,
        })
    total = len(rows)
    rows = rows[offset:]
    if limit is not None:
        rows = rows[:limit]
    return {"count": total, "returned": len(rows), "offset": offset, "objects": rows}


def get_impact(schema: str, table: str, settings: Settings | None = None) -> dict:
    """Blast radius for an object: downstream dbt models, the exposures
    (dashboards/ML/apps) that depend on it, and any reverse-ETL activations it
    feeds into operational systems. Answers 'what breaks if this is wrong?'."""
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
        "activations": obj.get("activations", []),
        "has_failing_tests": (obj.get("dq_summary", {}) or {}).get("failing_tests_count", 0) > 0,
    }


def get_column_impact(schema: str, table: str, column: str, settings: Settings | None = None) -> dict:
    """Column-level blast radius: starting from a Fivetran destination column, the
    downstream model columns it feeds (via parsed SQL lineage), plus the metrics and
    exposures of the affected models. 'If this column changes, what breaks?'"""
    settings = settings or get_settings()
    from ..dq.column_lineage import downstream_columns

    doc = _latest(settings)
    obj = get_warehouse_object(schema, table, settings=settings)
    if not obj.get("name"):
        return obj
    source_uid = (obj.get("dbt") or {}).get("source_unique_id")
    if not source_uid:
        return {"found": False, "message": f"{schema}.{table} is not matched to a dbt source."}

    edges = doc.get("sources", {}).get("dbt", {}).get("column_lineage_edges", [])
    affected = downstream_columns(edges, source_uid, column)
    affected_models = sorted({a["unique_id"] for a in affected})

    # metrics/exposures fed by the affected models
    metrics = [m for m in doc.get("sources", {}).get("dbt", {}).get("metrics", [])
               if set(m.get("model_unique_ids") or []) & set(affected_models)]
    exposures = [e for e in doc.get("sources", {}).get("dbt", {}).get("exposures", [])
                 if set(e.get("depends_on") or []) & set(affected_models)]

    # reverse-ETL destination fields fed by an affected column of an activation's source model
    affected_cols_by_model: dict[str, set] = {}
    # Seed the start node itself: downstream_columns returns only nodes reachable
    # via edges, never the origin, so an activation reading this dbt source
    # DIRECTLY (source_node == source_uid) would otherwise report no blast radius
    # for a column it pushes verbatim to the operational system.
    affected_cols_by_model.setdefault(source_uid, set()).add((column or "").lower())
    for a in affected:
        affected_cols_by_model.setdefault(a["unique_id"], set()).add((a.get("column") or "").lower())
    activation_fields: list[dict] = []
    for sync in doc.get("activations", {}).get("syncs", []):
        src_node = (sync.get("readiness") or {}).get("source_node_unique_id")
        cols = affected_cols_by_model.get(src_node)
        if not cols:
            continue
        for m in sync.get("mappings") or []:
            if (m.get("source_column") or "").lower() in cols:
                activation_fields.append({
                    "sync_id": sync.get("sync_id"),
                    "destination_name": sync.get("destination_name"),
                    "destination_object": sync.get("destination_object"),
                    "destination_field": m.get("destination_field"),
                    "source_column": m.get("source_column"),
                    "readiness_verdict": (sync.get("readiness") or {}).get("verdict"),
                })
    return {
        "object_id": obj.get("object_id"),
        "column": column,
        "affected_columns": affected,
        "affected_model_count": len(affected_models),
        "metrics": [{"name": m.get("name"), "type": m.get("type")} for m in metrics],
        "exposures": [{"name": e.get("name"), "type": e.get("type")} for e in exposures],
        "activation_fields": activation_fields,
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


def list_activations(
    verdict: str | None = None,
    settings: Settings | None = None,
) -> dict:
    """Reverse-ETL activations with their readiness verdict (allow|warn|block|
    unknown). Answers 'what data are we pushing back into operational systems,
    and is any of it unsafe?'. Filter by ``verdict``."""
    settings = settings or get_settings()
    doc = _latest(settings)
    syncs = doc.get("activations", {}).get("syncs", [])
    rows = []
    for s in syncs:
        r = s.get("readiness") or {}
        if verdict and r.get("verdict") != verdict:
            continue
        rows.append({
            "sync_id": s.get("sync_id"),
            "label": s.get("label"),
            "paused": s.get("paused"),
            "source_object": s.get("source_object"),
            "destination_name": s.get("destination_name"),
            "destination_type": s.get("destination_type"),
            "destination_object": s.get("destination_object"),
            "verdict": r.get("verdict"),
        })
    return {"count": len(rows), "summary": doc.get("activations", {}).get("summary", {}),
            "activations": rows}


def get_activation_readiness(sync_id: str | int | None = None, label: str | None = None,
                             schema: str | None = None, table: str | None = None,
                             settings: Settings | None = None) -> dict:
    """Full readiness detail for one activation: verdict + the upstream reasons
    (failing tests, warn-severity failures, staleness, missing contract) and the
    field mappings pushed to the destination. Address by ``sync_id``, ``label``,
    or the warehouse ``schema``+``table`` the sync reads. 'Safe to sync to prod?'"""
    settings = settings or get_settings()
    syncs = _latest(settings).get("activations", {}).get("syncs", [])
    matches: list[dict] = []
    for s in syncs:
        if sync_id is not None and str(s.get("sync_id")) == str(sync_id):
            return s
        if label and (s.get("label") or "").lower() == label.lower():
            return s
        if table:
            obj = s.get("source_object") or {}
            if ((obj.get("table_name") or "").lower() == table.lower()
                    and (not schema or (obj.get("table_schema") or "").lower() == schema.lower())):
                matches.append(s)
    if len(matches) == 1:
        return matches[0]
    if matches:
        return {"found": False,
                "message": f"{len(matches)} activations read {schema or '*'}.{table}; "
                           "disambiguate by sync_id.",
                "candidates": [{"sync_id": m.get("sync_id"), "label": m.get("label")} for m in matches]}
    return {"found": False,
            "message": f"No activation for sync_id={sync_id!r} label={label!r} "
                       f"schema={schema!r} table={table!r}."}


def get_dq_summary(settings: Settings | None = None) -> dict:
    """Account-level DQ rollup — the orienting call an agent makes first."""
    settings = settings or get_settings()
    doc = _latest(settings)
    objs = doc.get("warehouse_objects", [])
    recs = doc.get("dq_recommendations", [])
    drift = doc.get("schema_drift", [])
    stale_ids = _stale_object_ids(doc)

    risk_levels = {"low": 0, "medium": 0, "high": 0}
    matched = failing = warn_failing = missing = with_freshness = 0
    for o in objs:
        s = o.get("dq_summary", {})
        risk_levels[s.get("risk_level", "low")] = risk_levels.get(s.get("risk_level", "low"), 0) + 1
        if o.get("match_confidence") != "unmatched":
            matched += 1
        else:
            missing += 1
        if (s.get("failing_tests_count") or 0) > 0:
            failing += 1
        if (s.get("warn_tests_with_failures_count") or 0) > 0:
            warn_failing += 1
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
        "objects_with_warn_test_failures": warn_failing,
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
        "activations": {
            "total": doc.get("activations", {}).get("summary", {}).get("total", 0),
            "by_verdict": doc.get("activations", {}).get("summary", {}).get("by_verdict", {}),
        },
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
        def _matches(d: dict) -> bool:
            # object_id: warehouse://<db>/<schema>/<table>
            parts = (d.get("object_id") or "").lower().split("/")
            if len(parts) < 2:
                return False
            obj_schema, obj_table = parts[-2], parts[-1]
            if schema and obj_schema != schema.lower():
                return False
            if table and obj_table != table.lower():
                return False
            return True
        drift = [d for d in drift if _matches(d)]
    if severity:
        drift = [d for d in drift if d.get("severity") == severity]
    return {"count": len(drift), "drift": drift}
