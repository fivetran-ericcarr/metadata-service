"""Warehouse reader interface, factory, and the (pure) PK-enrichment function."""

from __future__ import annotations

import logging
from typing import Protocol, runtime_checkable

from ..config import Settings

logger = logging.getLogger(__name__)


@runtime_checkable
class WarehouseMetadataReader(Protocol):
    """Reads metadata from the destination's ``fivetran_metadata`` schema."""

    def read_primary_keys(self, connection_ids: list[str] | None = None) -> dict[tuple[str, str], list[str]]:
        """Map ``(dest_schema_lower, dest_table_lower) -> [dest_column, ...]`` of PKs."""
        ...

    def close(self) -> None:
        ...


def get_warehouse_reader(settings: Settings) -> WarehouseMetadataReader | None:
    """Return a reader if configured + supported, else None (feature is optional)."""
    if not settings.warehouse_reader_enabled():
        return None
    wtype = (settings.warehouse_type or "").lower()
    if wtype == "snowflake":
        from .snowflake_reader import SnowflakeMetadataReader

        return SnowflakeMetadataReader(settings)
    logger.warning("Warehouse metadata reader not implemented for type %r", wtype)
    return None


def apply_primary_keys(fivetran_normalized: dict, pk_map: dict[tuple[str, str], list[str]]) -> int:
    """Override column PK flags from the authoritative Platform Connector map.

    ``pk_map`` keys are ``(dest_schema_lower, dest_table_lower)``; values are
    destination column names. Sets ``is_primary_key``/``key_constraint`` and tags
    ``key_source = "fivetran_platform"``. Returns the number of columns updated.
    """
    pk_map_lc = {(s.lower(), t.lower()): v for (s, t), v in pk_map.items()}
    updated = 0
    for conn in fivetran_normalized.get("connections", []) or []:
        for table in conn.get("tables", []) or []:
            key = ((table.get("destination_schema") or "").lower(),
                   (table.get("destination_table") or "").lower())
            pk_cols = pk_map_lc.get(key)
            if not pk_cols:
                continue
            pk_lower = {c.lower() for c in pk_cols}
            for col in table.get("columns", []) or []:
                if (col.get("destination_name") or "").lower() in pk_lower and not col.get("is_primary_key"):
                    col["is_primary_key"] = True
                    col["key_constraint"] = "primary_key"
                    col["key_source"] = "fivetran_platform"
                    updated += 1
    return updated
