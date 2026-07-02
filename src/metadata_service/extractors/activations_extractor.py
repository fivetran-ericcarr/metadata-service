"""Fivetran Activations (reverse ETL) extraction.

Pulls sync definitions (source object, destination object, field mappings) from
the Census-based Activations API. Optionally scopes to syncs whose source reads a
given warehouse database (so a shared workspace doesn't drown the snapshot).
"""

from __future__ import annotations

import logging

from ..clients.activations_client import ActivationsClient
from ..exceptions import ActivationsError
from ..models.common import utcnow_iso

logger = logging.getLogger(__name__)


class ActivationsExtractor:
    def __init__(self, client: ActivationsClient) -> None:
        self._client = client

    def extract(self, source_database: str | None = None, *, max_syncs: int = 200) -> dict:
        errors: list[dict] = []
        logger.info("Activations extraction starting (source_database=%s)", source_database or "<all>")

        try:
            sources = {s.get("id"): {"name": s.get("name"), "type": s.get("type")}
                       for s in self._client.list_sources()}
        except ActivationsError as exc:
            sources = {}
            errors.append({"source": "activations", "resource": "sources",
                           "error_type": type(exc).__name__, "error_message": str(exc)})
        try:
            destinations = {d.get("id"): {"name": d.get("name"), "type": d.get("type")}
                            for d in self._client.list_destinations()}
        except ActivationsError as exc:
            destinations = {}
            errors.append({"source": "activations", "resource": "destinations",
                           "error_type": type(exc).__name__, "error_message": str(exc)})

        kept: list[dict] = []
        try:
            summaries = self._client.list_syncs()
        except ActivationsError as exc:
            summaries = []
            errors.append({"source": "activations", "resource": "syncs",
                           "error_type": type(exc).__name__, "error_message": str(exc)})

        wanted = (source_database or "").lower()
        for sm in summaries[:max_syncs]:
            sid = sm.get("id")
            try:
                detail = self._client.get_sync(sid)
            except ActivationsError as exc:
                errors.append({"source": "activations", "sync_id": sid,
                               "error_type": type(exc).__name__, "error_message": str(exc)})
                continue
            if wanted:
                obj = (detail.get("source_attributes") or {}).get("object") or {}
                if (obj.get("table_catalog") or "").lower() != wanted:
                    continue
            kept.append(detail)

        logger.info("Activations extraction complete: %s syncs kept, %s errors", len(kept), len(errors))
        return {
            "extracted_at": utcnow_iso(),
            "source": "activations",
            "syncs": kept,
            "sources": sources,
            "destinations": destinations,
            "errors": errors,
        }
