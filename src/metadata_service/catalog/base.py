"""Lake catalog reader interface and the (pure) enrichment functions.

A catalog reader is the open-lake counterpart of a warehouse reader: where a
warehouse destination exposes column attributes through ``INFORMATION_SCHEMA``,
a Fivetran Managed Data Lake destination writes Iceberg/Delta files to object
storage and registers them in a catalog (Apache Polaris, AWS Glue, Unity
Catalog, ...). The catalog is the only place those attributes exist.

Every reader returns the same per-table record so enrichment does not care
which catalog answered::

    {
      "catalog": "polaris",            # reader type
      "namespace": "salesforce", "table": "account",
      "table_format": "iceberg",       # iceberg | delta | hive | None
      "format_version": 2,
      "location": "s3://bucket/prefix/salesforce/account",
      "partition_spec": [{"source_column": "created_date", "transform": "day", "name": "created_date_day"}],
      "last_updated": "2026-11-10T08:00:00Z",
      "current_snapshot_id": 123, "snapshot_at": "...", "snapshot_operation": "append",
      "total_records": 1000,
      "primary_keys": ["id"],          # Iceberg identifier fields; None when the catalog doesn't know
      "columns": {"id": {"name": "id", "data_type": "string", "nullable": False, "max_length": None}},
    }
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)

#: Column attributes copied from a catalog onto each normalized column; the
#: same set the warehouse readers provide.
COLUMN_FIELDS = ("data_type", "max_length", "nullable")

#: Table-level attributes kept on the normalized table (``table["lake"]``).
TABLE_FIELDS = (
    "table_format", "format_version", "location", "partition_spec", "last_updated",
    "current_snapshot_id", "snapshot_at", "snapshot_operation", "total_records",
)


@dataclass
class CatalogReadResult:
    """Tables a reader resolved plus per-table failures.

    One table failing to load (dropped mid-build, permission on a single
    namespace) is recorded in ``errors`` and never blocks the other tables.
    """

    tables: dict[tuple[str, str], dict] = field(default_factory=dict)
    errors: list[dict] = field(default_factory=list)
    missing: list[tuple[str, str]] = field(default_factory=list)


@runtime_checkable
class CatalogMetadataReader(Protocol):
    """Reads table and column metadata from a lake catalog."""

    catalog_type: str

    def read_tables(self, tables: list[tuple[str, str]]) -> CatalogReadResult:
        """Load ``[(namespace, table), ...]``. Keys of the result are lower-cased.

        Raises only for a failure that affects every table (authentication,
        unreachable endpoint); per-table failures go to ``errors``.
        """
        ...

    def close(self) -> None:
        ...


def apply_catalog_tables(fivetran_normalized: dict, catalog_type: str,
                         tables: dict[tuple[str, str], dict]) -> dict:
    """Merge one catalog's tables onto the normalized Fivetran tables.

    Call once per catalog in precedence order: the first catalog to describe a
    column wins (``schema_source`` already set to another ``catalog:*`` value is
    left alone), and an attribute a warehouse ``INFORMATION_SCHEMA`` read set is
    never overwritten. Table-level attributes are merged the same way into
    ``table["lake"]``; ``lake.catalogs`` lists every catalog that knew the table.
    Primary keys from Iceberg identifier fields set ``is_primary_key`` with
    ``key_source = "catalog:<type>"`` and never clear a PK set elsewhere.

    Returns ``{"tables": n, "columns": n, "primary_keys": n}`` updated.
    """
    lookup = {(ns.lower(), t.lower()): info for (ns, t), info in tables.items()}
    source = f"catalog:{catalog_type}"
    counts = {"tables": 0, "columns": 0, "primary_keys": 0}
    for conn in fivetran_normalized.get("connections", []) or []:
        for table in conn.get("tables", []) or []:
            key = ((table.get("destination_schema") or "").lower(),
                   (table.get("destination_table") or "").lower())
            info = lookup.get(key)
            if not info:
                continue
            counts["tables"] += 1
            lake = table.setdefault("lake", {"catalogs": []})
            if catalog_type not in lake["catalogs"]:
                lake["catalogs"].append(catalog_type)
            for f in TABLE_FIELDS:
                if lake.get(f) is None and info.get(f) is not None:
                    lake[f] = info[f]
            cols = info.get("columns") or {}
            pks = {c.lower() for c in (info.get("primary_keys") or [])}
            for col in table.get("columns", []) or []:
                name = (col.get("destination_name") or "").lower()
                attrs = cols.get(name)
                if attrs and not col.get("schema_source"):
                    for f in COLUMN_FIELDS:
                        col[f] = attrs.get(f)
                    col["schema_source"] = source
                    counts["columns"] += 1
                if name in pks and not col.get("is_primary_key"):
                    col["is_primary_key"] = True
                    col["key_constraint"] = "primary_key"
                    col["key_source"] = source
                    counts["primary_keys"] += 1
    return counts


def wanted_tables(fivetran_normalized: dict) -> list[tuple[str, str]]:
    """Distinct ``(destination_schema, destination_table)`` pairs to load."""
    seen: dict[tuple[str, str], tuple[str, str]] = {}
    for conn in fivetran_normalized.get("connections", []) or []:
        for table in conn.get("tables", []) or []:
            schema, name = table.get("destination_schema"), table.get("destination_table")
            if schema and name and table.get("enabled", True):
                seen.setdefault((schema.lower(), name.lower()), (schema, name))
    return [seen[k] for k in sorted(seen)]
