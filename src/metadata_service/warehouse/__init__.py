"""Warehouse metadata readers.

Read authoritative primary keys (and lineage) from the Fivetran Platform
Connector's ``fivetran_metadata`` schema in the destination warehouse — the
modern source for metadata the config/Metadata REST APIs no longer provide.
"""

from .base import WarehouseMetadataReader, apply_primary_keys, get_warehouse_reader

__all__ = ["WarehouseMetadataReader", "get_warehouse_reader", "apply_primary_keys"]
