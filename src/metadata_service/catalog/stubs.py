"""Catalogs detected from the destination but not read yet.

Each class documents the read it would make. The factory reports these as
``not_implemented`` in ``build_scope.catalog_enrichment`` instead of failing,
and their physical locations are still served (``catalog.locations``).

TODO(unity): Databricks Unity Catalog — ``GET /api/2.1/unity-catalog/tables/
    {catalog}.{schema}.{table}`` with a PAT/OAuth M2M token on the workspace in
    the destination config (``server_host_name``); columns carry ``type_text``,
    ``nullable``; ``table_constraints`` carries PRIMARY KEY constraints. Unity
    also serves Iceberg REST at ``/api/2.1/unity-catalog/iceberg``, so the
    Polaris reader can be pointed at it once the token exchange is pluggable.
TODO(biglake): BigQuery BigLake metastore — ``bigquery.tables.get`` on the
    metastore dataset (schema fields with ``mode``/``maxLength``), or the BigLake
    Iceberg REST catalog (``https://biglake.googleapis.com/iceberg/v1/restcatalog``)
    with a Google access token.
TODO(onelake): Microsoft OneLake — table metadata via the OneLake Table API
    (Iceberg/Delta REST, ``https://onelake.table.fabric.microsoft.com``) with an
    Entra service principal; Delta log schema for column types.
"""

from __future__ import annotations

from .base import CatalogReadResult


class _NotImplementedCatalog:
    catalog_type = "unknown"

    def read_tables(self, tables: list[tuple[str, str]]) -> CatalogReadResult:  # pragma: no cover
        raise NotImplementedError(f"{self.catalog_type} catalog reader is not implemented yet.")

    def close(self) -> None:  # pragma: no cover
        pass


class UnityCatalogReader(_NotImplementedCatalog):
    catalog_type = "unity"


class BigLakeCatalogReader(_NotImplementedCatalog):
    catalog_type = "biglake"


class OneLakeCatalogReader(_NotImplementedCatalog):
    catalog_type = "onelake"


NOT_IMPLEMENTED = {c.catalog_type: c for c in
                   (UnityCatalogReader, BigLakeCatalogReader, OneLakeCatalogReader)}
