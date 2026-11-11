"""Choose catalog readers from the Fivetran destination (overridable)."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from ..config import Settings
from ..warehouse.destination import DestinationInfo
from .stubs import NOT_IMPLEMENTED

logger = logging.getLogger(__name__)

#: Reader precedence: an earlier catalog's column attributes win.
PRECEDENCE = ("polaris", "glue", "unity", "biglake", "onelake")


@dataclass
class PlannedReader:
    """One catalog the build should consult. ``reader`` is None when it can't
    run; ``outcome`` then says why (``not_configured`` | ``not_implemented``)."""

    catalog_type: str
    reader: object | None = None
    outcome: str | None = None
    reason: str | None = None


def _catalog_entry(destination: DestinationInfo | None, kind: str) -> dict:
    lake = (destination.lake if destination else None) or {}
    for c in lake.get("catalogs") or []:
        if c.get("type") == kind:
            return c
    return {}


def requested_catalogs(settings: Settings, destination: DestinationInfo | None) -> list[str]:
    """Catalog types to consult, in precedence order. ``[]`` = not applicable."""
    raw = (settings.lake_catalogs or "auto").strip().lower()
    if raw in ("none", "off", "false", ""):
        return []
    if raw == "auto":
        if destination is None or not destination.is_lake():
            return []
        kinds = [c.get("type") for c in (destination.lake or {}).get("catalogs") or []
                 if c.get("enabled", True)]
        if "polaris" not in kinds:
            kinds.insert(0, "polaris")  # every Managed Data Lake has one
    else:
        kinds = [k.strip() for k in raw.split(",") if k.strip()]
    ordered = [k for k in PRECEDENCE if k in kinds] + [k for k in kinds if k not in PRECEDENCE]
    return list(dict.fromkeys(ordered))


def plan_catalog_readers(settings: Settings, destination: DestinationInfo | None) -> list[PlannedReader]:
    plans: list[PlannedReader] = []
    for kind in requested_catalogs(settings, destination):
        try:
            plans.append(_plan_one(kind, settings, destination))
        except Exception as exc:  # construction must never abort the build
            logger.error("Catalog reader %s construction FAILED: %s", kind, exc)
            plans.append(PlannedReader(kind, outcome="failed", reason=type(exc).__name__))
    return plans


def _plan_one(kind: str, settings: Settings, destination: DestinationInfo | None) -> PlannedReader:
    if kind == "polaris":
        entry = _catalog_entry(destination, "polaris")
        endpoint = settings.polaris_endpoint or entry.get("endpoint")
        catalog = settings.polaris_catalog or entry.get("catalog") or \
            (destination.destination_id if destination else None)
        client_id = settings.polaris_client_id or entry.get("client_id")
        secret = settings.polaris_client_secret
        missing = [n for n, v in (("POLARIS_ENDPOINT", endpoint), ("POLARIS_CATALOG", catalog),
                                  ("POLARIS_CLIENT_ID", client_id),
                                  ("POLARIS_CLIENT_SECRET", secret)) if not v]
        if missing:
            return PlannedReader(kind, outcome="not_configured",
                                 reason="missing " + ", ".join(missing))
        from .polaris import PolarisCatalogReader

        return PlannedReader(kind, PolarisCatalogReader(
            endpoint=endpoint, catalog=catalog, client_id=client_id, client_secret=secret,
            scope=settings.polaris_scope, timeout=settings.catalog_timeout_seconds))
    if kind == "glue":
        entry = _catalog_entry(destination, "glue")
        try:
            import boto3  # noqa: PLC0415
        except ImportError:
            return PlannedReader(kind, outcome="not_configured",
                                 reason="boto3 not installed (catalog-glue extra)")
        if boto3.Session().get_credentials() is None:
            # Glue maintenance is on, but this host has no AWS identity: a
            # configuration gap, not a failed read (keeps the build `success`).
            return PlannedReader(kind, outcome="not_configured", reason="no AWS credentials in the default chain")
        from .glue import GlueCatalogReader

        lake_region = ((destination.lake or {}).get("region")) if destination else None
        region = settings.glue_region or entry.get("region") or lake_region
        return PlannedReader(kind, GlueCatalogReader(
            region=region, catalog_id=settings.glue_catalog_id or entry.get("catalog_id")))
    if kind in NOT_IMPLEMENTED:
        return PlannedReader(kind, outcome="not_implemented",
                             reason=f"{kind} reader is a documented stub (catalog/stubs.py)")
    return PlannedReader(kind, outcome="not_implemented", reason=f"unknown catalog type {kind!r}")
