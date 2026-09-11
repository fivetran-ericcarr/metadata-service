"""Warehouse reader interface, factory, and the (pure) enrichment functions."""

from __future__ import annotations

import logging
from typing import Protocol, runtime_checkable

from ..config import Settings
from .destination import (
    ANSI_INFORMATION_SCHEMA_TYPES,
    NON_ANSI_TYPES,
    NO_INFORMATION_SCHEMA_TYPES,
    DestinationInfo,
)

logger = logging.getLogger(__name__)

#: Placeholder default of ``WAREHOUSE_TYPE`` in .env.example; treated as "unset".
_PLACEHOLDER_WAREHOUSE_TYPE = "warehouse"


@runtime_checkable
class WarehouseMetadataReader(Protocol):
    """Reads metadata from the destination warehouse.

    Two distinct sources are involved: primary keys come from the Fivetran
    Platform Connector's ``fivetran_metadata`` schema, while column-level
    attributes come from the warehouse's own information schema.
    """

    def read_primary_keys(self, connection_ids: list[str] | None = None) -> dict[tuple[str, str], list[str]]:
        """Map ``(dest_schema_lower, dest_table_lower) -> [dest_column, ...]`` of PKs."""
        ...

    def read_column_schema(self, schemas: list[str] | None = None) -> dict[tuple[str, str, str], dict]:
        """Map ``(schema_lower, table_lower, column_lower) -> attributes``.

        Attributes are ``{"data_type", "max_length", "nullable"}``; any of them may
        be None when the warehouse does not report it (``max_length`` is NULL for
        every non-character type). ``schemas`` bounds the read to the destination
        schemas actually in the snapshot.
        """
        ...

    def close(self) -> None:
        ...


def resolve_warehouse_type(
    settings: Settings, destination: DestinationInfo | None = None
) -> str | None:
    """Decide the reader dialect: explicit ``WAREHOUSE_TYPE`` wins, else the
    destination's detected type.

    ``WAREHOUSE_TYPE`` ships with the placeholder default ``"warehouse"``, which
    is treated as "unset" so an operator who never edited ``.env`` gets
    auto-detection rather than a permanently disabled reader.
    """
    explicit = (settings.warehouse_type or "").strip().lower()
    if explicit and explicit != _PLACEHOLDER_WAREHOUSE_TYPE:
        if destination is not None and destination.warehouse_type and \
                destination.warehouse_type != explicit:
            logger.warning(
                "WAREHOUSE_TYPE=%s overrides the detected destination type %s (service %r).",
                explicit, destination.warehouse_type, destination.service,
            )
        return explicit
    if destination is not None and destination.warehouse_type:
        logger.info(
            "Detected destination warehouse type %s from Fivetran service %r.",
            destination.warehouse_type, destination.service,
        )
        return destination.warehouse_type
    return None


def get_warehouse_reader(
    settings: Settings, destination: DestinationInfo | None = None
) -> WarehouseMetadataReader | None:
    """Return a reader if configured + supported, else None (feature is optional).

    ``destination`` supplies the auto-detected dialect and database when
    ``WAREHOUSE_TYPE``/``WAREHOUSE_DATABASE`` are not set explicitly.
    """
    wtype = resolve_warehouse_type(settings, destination)
    if not wtype:
        return None
    if not settings.warehouse_credentials_present():
        logger.debug("Warehouse reader not configured (no WAREHOUSE_* credentials).")
        return None

    if wtype in NO_INFORMATION_SCHEMA_TYPES:
        # Not a gap to fill later: MDLS writes open table formats to object
        # storage and has no SQL information schema to read at all.
        logger.info(
            "Destination type %r has no queryable information schema; column "
            "attributes must come from the table-format catalog instead.", wtype,
        )
        return None
    if wtype in NON_ANSI_TYPES:
        logger.warning(
            "Warehouse metadata reader not implemented for type %r (its information "
            "schema is not ANSI-compatible and needs a dedicated query).", wtype,
        )
        return None

    if wtype == "snowflake":
        from .snowflake_reader import SnowflakeMetadataReader

        return SnowflakeMetadataReader(settings, destination=destination)

    if wtype in ANSI_INFORMATION_SCHEMA_TYPES:
        logger.warning(
            "Destination type %r uses the ANSI information schema but has no reader "
            "yet (needs a driver extra); skipping warehouse enrichment.", wtype,
        )
        return None

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


#: Attributes copied from the information schema onto each normalized column.
_COLUMN_SCHEMA_FIELDS = ("data_type", "max_length", "nullable")


def apply_column_schema(
    fivetran_normalized: dict, column_map: dict[tuple[str, str, str], dict]
) -> int:
    """Merge information-schema attributes onto normalized Fivetran columns.

    ``column_map`` keys are ``(schema_lower, table_lower, column_lower)``; values
    carry ``data_type``/``max_length``/``nullable``. Matching is case-insensitive
    on all three parts. Also stamps ``schema_source = "information_schema"`` so a
    consumer can tell a warehouse-authoritative value from an inferred one.
    Returns the number of columns updated.
    """
    column_map_lc = {
        (s.lower(), t.lower(), c.lower()): v for (s, t, c), v in column_map.items()
    }
    updated = 0
    for conn in fivetran_normalized.get("connections", []) or []:
        for table in conn.get("tables", []) or []:
            schema = (table.get("destination_schema") or "").lower()
            name = (table.get("destination_table") or "").lower()
            for col in table.get("columns", []) or []:
                attrs = column_map_lc.get((schema, name, (col.get("destination_name") or "").lower()))
                if not attrs:
                    continue
                for field in _COLUMN_SCHEMA_FIELDS:
                    col[field] = attrs.get(field)
                col["schema_source"] = "information_schema"
                updated += 1
    return updated
