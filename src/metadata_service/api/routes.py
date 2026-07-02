"""REST route handlers."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..config import get_settings
from ..pipeline import build_and_store
from ..storage.base import get_storage

logger = logging.getLogger(__name__)

router = APIRouter()


class RefreshRequest(BaseModel):
    fivetran_group_id: str | None = None
    include_dbt: bool = True
    include_fivetran: bool = True


def _load_latest_or_404() -> dict:
    storage = get_storage(get_settings())
    latest = storage.read_latest()
    if latest is None:
        raise HTTPException(status_code=404, detail="No metadata snapshot found. Run a refresh/build first.")
    return latest


@router.get("/health")
def health() -> dict:
    return {"status": "ok"}


@router.post("/metadata/refresh")
def refresh_metadata(body: RefreshRequest) -> dict:
    settings = get_settings()
    try:
        result = build_and_store(
            settings,
            group_id=body.fivetran_group_id,
            include_fivetran=body.include_fivetran,
            include_dbt=body.include_dbt,
        )
    except Exception as exc:  # surface as 500 with a clear message
        logger.exception("Metadata refresh failed")
        raise HTTPException(status_code=500, detail=f"Refresh failed: {exc}") from exc
    result.pop("doc", None)  # don't return the whole doc inline
    return result


@router.get("/metadata/latest")
def metadata_latest() -> dict:
    return _load_latest_or_404()


@router.get("/metadata/fivetran")
def metadata_fivetran() -> dict:
    return _load_latest_or_404().get("sources", {}).get("fivetran", {})


@router.get("/metadata/dbt")
def metadata_dbt() -> dict:
    return _load_latest_or_404().get("sources", {}).get("dbt", {})


@router.get("/metadata/warehouse-objects")
def warehouse_objects(
    schema: str | None = Query(default=None),
    table: str | None = Query(default=None),
) -> dict:
    objects = _load_latest_or_404().get("warehouse_objects", [])
    if schema:
        objects = [o for o in objects if (o.get("schema") or "").lower() == schema.lower()]
    if table:
        objects = [o for o in objects if (o.get("name") or "").lower() == table.lower()]
    return {"count": len(objects), "warehouse_objects": objects}


@router.get("/metadata/warehouse-objects/{object_id:path}")
def warehouse_object(object_id: str) -> dict:
    objects = _load_latest_or_404().get("warehouse_objects", [])
    for obj in objects:
        if obj.get("object_id") == object_id or obj.get("object_id", "").endswith(object_id):
            return obj
    raise HTTPException(status_code=404, detail=f"Warehouse object not found: {object_id}")


@router.get("/dq/recommendations")
def dq_recommendations(
    schema: str | None = Query(default=None),
    table: str | None = Query(default=None),
) -> dict:
    recs = _load_latest_or_404().get("dq_recommendations", [])
    if schema:
        recs = [r for r in recs if (r.get("target", {}).get("schema") or "").lower() == schema.lower()]
    if table:
        recs = [r for r in recs if (r.get("target", {}).get("table") or "").lower() == table.lower()]
    return {"count": len(recs), "recommendations": recs}


@router.get("/dq/drift")
def dq_drift(severity: str | None = Query(default=None)) -> dict:
    drift = _load_latest_or_404().get("schema_drift", [])
    if severity:
        drift = [d for d in drift if d.get("severity") == severity]
    return {"count": len(drift), "drift": drift}


@router.get("/metadata/activations")
def metadata_activations(verdict: str | None = Query(default=None)) -> dict:
    activations = _load_latest_or_404().get("activations", {})
    syncs = activations.get("syncs", [])
    if verdict:
        syncs = [s for s in syncs if (s.get("readiness") or {}).get("verdict") == verdict]
    return {"count": len(syncs), "summary": activations.get("summary", {}), "activations": syncs}


@router.get("/dq/activation-readiness")
def dq_activation_readiness(
    sync_id: str | None = Query(default=None),
    label: str | None = Query(default=None),
) -> dict:
    syncs = _load_latest_or_404().get("activations", {}).get("syncs", [])
    for s in syncs:
        if sync_id is not None and str(s.get("sync_id")) == str(sync_id):
            return s
        if label and (s.get("label") or "").lower() == label.lower():
            return s
    raise HTTPException(status_code=404, detail=f"No activation for sync_id={sync_id!r} label={label!r}.")
