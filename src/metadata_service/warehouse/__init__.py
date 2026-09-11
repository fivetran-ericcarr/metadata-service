"""Warehouse metadata readers.

Two reads against the destination warehouse:

* authoritative primary keys (and lineage) from the Fivetran Platform
  Connector's ``fivetran_metadata`` schema — the modern source for metadata the
  config/Metadata REST APIs no longer provide;
* column ``data_type``/``max_length``/``nullable`` from the destination's own
  ``INFORMATION_SCHEMA.COLUMNS``, which is the only place the latter two exist
  (neither the Fivetran schema API nor the dbt catalog carries them).

The dialect is auto-detected from the Fivetran destination's ``service``;
``WAREHOUSE_TYPE`` remains an explicit override.
"""

from .ansi import build_column_schema_sql, rows_to_column_map, validate_identifier
from .base import (
    WarehouseMetadataReader,
    apply_column_schema,
    apply_primary_keys,
    get_warehouse_reader,
    resolve_warehouse_type,
)
from .destination import DestinationInfo, describe_destination, destination_from_dict
from .fivetran_metadata import build_primary_key_sql, rows_to_pk_map

__all__ = [
    "DestinationInfo",
    "WarehouseMetadataReader",
    "apply_column_schema",
    "apply_primary_keys",
    "build_column_schema_sql",
    "build_primary_key_sql",
    "describe_destination",
    "destination_from_dict",
    "get_warehouse_reader",
    "resolve_warehouse_type",
    "rows_to_column_map",
    "rows_to_pk_map",
    "validate_identifier",
]
