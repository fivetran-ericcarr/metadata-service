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
            syncs = self._client.list_syncs()
        except ActivationsError as exc:
            syncs = []
            errors.append({"source": "activations", "resource": "syncs",
                           "error_type": type(exc).__name__, "error_message": str(exc)})

        # Census list responses carry the full sync payload (source/destination
        # attributes + mappings), so no per-sync detail call is needed.
        wanted = (source_database or "").lower()
        for sync in syncs:
            if wanted:
                obj = (sync.get("source_attributes") or {}).get("object") or {}
                if (obj.get("table_catalog") or "").lower() != wanted:
                    continue
            kept.append(sync)

        if len(kept) > max_syncs:
            errors.append({
                "source": "activations", "resource": "syncs",
                "error_type": "Truncated",
                "error_message": f"{len(kept)} syncs matched but only {max_syncs} were kept "
                                 f"(max_syncs); readiness was NOT evaluated for the rest.",
            })
            logger.warning("Activations truncated: %s matched, keeping %s", len(kept), max_syncs)
            kept = kept[:max_syncs]

        logger.info("Activations extraction complete: %s syncs kept, %s errors", len(kept), len(errors))
        return {
            "extracted_at": utcnow_iso(),
            "source": "activations",
            "syncs": kept,
            "sources": sources,
            "destinations": destinations,
            "errors": errors,
        }
