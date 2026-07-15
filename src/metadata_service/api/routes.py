"""REST route handlers."""

from __future__ import annotations

import logging
import secrets

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from ..config import get_settings
from ..exceptions import RefreshInProgressError
from ..pipeline import build_and_store
from ..storage.base import get_storage

logger = logging.getLogger(__name__)


async def require_api_key(request: Request) -> None:
    """Enforce METADATA_API_KEY on every route except /health.

    Accepts the key via ``X-API-Key`` or ``Authorization: Bearer <key>``. When no
    key is configured the API is open — pair that only with the loopback default
    bind (API_HOST=127.0.0.1); set a key before exposing the service remotely.
    """
    configured = get_settings().metadata_api_key
    if not configured or request.url.path == "/health":
        return
    provided = request.headers.get("x-api-key")
    if not provided:
        auth = request.headers.get("authorization") or ""
        if auth.lower().startswith("bearer "):
            provided = auth[7:]
    # Compare as bytes: secrets.compare_digest raises TypeError on non-ASCII str
    # (Starlette decodes headers as latin-1), which would surface as a 500 rather
    # than a clean 401 for a bogus key.
    if not provided or not secrets.compare_digest(provided.encode("utf-8"),
                                                  configured.encode("utf-8")):
        raise HTTPException(status_code=401, detail="Missing or invalid API key.")


router = APIRouter(dependencies=[Depends(require_api_key)])


class RefreshRequest(BaseModel):
    """Accepts the same scoping as the CLI build, so a webhook/agent-triggered
    refresh produces the SAME snapshot as the scheduled one (a broader refresh
    would clobber the baseline and manufacture false drift)."""

    fivetran_group_id: str | None = None
    include_dbt: bool = True
    include_fivetran: bool = True
    include_activations: bool = True
    dbt_project_id: int | None = None
    connected_only: bool = False
    skip_paused: bool = False


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
            include_activations=body.include_activations,
            dbt_project_id=body.dbt_project_id,
            connected_only=body.connected_only,
            skip_paused=body.skip_paused,
        )
    except RefreshInProgressError as exc:
        raise HTTPException(status_code=409, detail="A refresh is already in progress.") from exc
    except Exception as exc:
        # Log the detail server-side; don't echo internals (paths, hosts, env
        # var names) to callers.
        logger.exception("Metadata refresh failed")
        raise HTTPException(status_code=500, detail="Refresh failed; see server logs.") from exc
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
    """Exact object_id, or a slash-delimited schema/table suffix. A bare-name
    suffix used to match the first id merely ENDING in the string (asking for
    "orders" could return "stg_orders") — bare names now 404 instead."""
    objects = _load_latest_or_404().get("warehouse_objects", [])
    # Object ids are lower-cased by the producer; match case-insensitively so a
    # caller echoing warehouse casing (e.g. SALESFORCE/ACCOUNT) isn't a 404 while
    # the sibling ?schema=&table= filter finds the same object.
    needle = object_id.lower()
    for obj in objects:
        oid = (obj.get("object_id") or "").lower()
        if oid == needle or ("/" in needle and oid.endswith("/" + needle)):
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
