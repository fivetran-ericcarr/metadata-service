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


def get_dq_recommendations(schema: str, table: str, settings: Settings | None = None) -> dict:
    settings = settings or get_settings()
    doc = _latest(settings)
    recs = [
        r
        for r in doc.get("dq_recommendations", [])
        if (r.get("target", {}).get("schema") or "").lower() == schema.lower()
        and (r.get("target", {}).get("table") or "").lower() == table.lower()
    ]
    return {"count": len(recs), "recommendations": recs}


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
