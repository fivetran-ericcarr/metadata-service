"""Lake catalog readers (Managed Data Lake destinations).

A Fivetran Managed Data Lake destination has no SQL information schema: column
attributes, partitioning, table format and snapshot state live in the table
format's catalog. Readers here yield the same column attributes the warehouse
readers do (``data_type``/``nullable``/``max_length``) plus table-level lake
facts and, where the catalog knows them (Iceberg identifier fields), PKs.

* :class:`PolarisCatalogReader` — Fivetran-managed Apache Polaris and any
  Iceberg REST catalog (plain REST, OAuth client credentials).
* :class:`GlueCatalogReader` — AWS Glue Data Catalog (boto3).
* Unity Catalog, BigLake metastore, OneLake — documented stubs.

Readers are chosen from ``GET /v1/destinations/{id}``; ``LAKE_CATALOGS`` and
``POLARIS_*``/``GLUE_*`` settings override.
"""

from .base import (
    CatalogMetadataReader,
    CatalogReadResult,
    apply_catalog_tables,
    wanted_tables,
)
from .factory import PlannedReader, plan_catalog_readers, requested_catalogs
from .iceberg import parse_table_metadata
from .locations import physical_locations

__all__ = [
    "CatalogMetadataReader",
    "CatalogReadResult",
    "PlannedReader",
    "apply_catalog_tables",
    "parse_table_metadata",
    "physical_locations",
    "plan_catalog_readers",
    "requested_catalogs",
    "wanted_tables",
]
