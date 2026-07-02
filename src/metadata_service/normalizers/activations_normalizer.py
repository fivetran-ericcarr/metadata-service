"""Normalize raw Activations (reverse ETL) sync payloads into stable records."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class ActivationsNormalizer:
    def normalize(self, raw: dict) -> dict:
        raw = raw or {}
        sources = raw.get("sources") or {}
        destinations = raw.get("destinations") or {}
        syncs = [self._sync(s, sources, destinations) for s in raw.get("syncs") or []]
        return {
            "extracted_at": raw.get("extracted_at"),
            "syncs": syncs,
            "errors": list(raw.get("errors") or []),
        }

    @staticmethod
    def _sync(sync: dict, sources: dict, destinations: dict) -> dict:
        sa = sync.get("source_attributes") or {}
        da = sync.get("destination_attributes") or {}
        obj = sa.get("object") or {}
        src_conn = sa.get("connection_id")
        dst_conn = da.get("connection_id")
        dst_obj = da.get("object")
        # destination object may be a string ("Contact") or an object.
        if isinstance(dst_obj, dict):
            dst_obj = dst_obj.get("name") or dst_obj.get("label")

        mappings = []
        for m in sync.get("mappings") or []:
            frm = m.get("from") or {}
            mappings.append({
                "source_column": frm.get("data") if frm.get("type") == "column" else None,
                "destination_field": m.get("to"),
                "is_primary_identifier": bool(m.get("is_primary_identifier")),
            })

        return {
            "sync_id": sync.get("id"),
            "label": sync.get("label"),
            "status": sync.get("status"),
            "paused": sync.get("paused"),
            "operation": sync.get("operation"),
            "source_connection_id": src_conn,
            "source_name": (sources.get(src_conn) or {}).get("name"),
            "source_object": {
                "table_catalog": obj.get("table_catalog"),
                "table_schema": obj.get("table_schema"),
                "table_name": obj.get("table_name"),
            },
            "destination_connection_id": dst_conn,
            "destination_name": (destinations.get(dst_conn) or {}).get("name"),
            "destination_type": (destinations.get(dst_conn) or {}).get("type"),
            "destination_object": dst_obj,
            "mappings": mappings,
            "last_synced_at": sync.get("updated_at"),
        }
