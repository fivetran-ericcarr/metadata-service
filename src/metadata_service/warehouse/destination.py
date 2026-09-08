"""Interpret a Fivetran destination record into warehouse coordinates.

``GET /v1/destinations/{id}`` returns the destination's ``service`` (the Fivetran
warehouse identifier) plus a ``config`` object. This module maps ``service`` onto
the reader dialect and pulls the non-secret coordinates (database/catalog/project
and host) out of the service-specific config shape.

Credentials are NOT available here: Fivetran masks every password-format config
field as the literal ``"******"``. Non-secret values (``host``, ``database``,
``user``, ``role``) do come back in cleartext, but the operator must still supply
the read-only credential via ``WAREHOUSE_*`` settings.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

#: Redaction sentinel Fivetran substitutes for password-format config fields.
REDACTED = "******"

#: ``service`` -> reader dialect. Values are the vocabulary of
#: ``Settings.warehouse_type`` and of the reader factory.
SERVICE_TO_WAREHOUSE_TYPE: dict[str, str] = {
    "snowflake": "snowflake",
    "redshift": "redshift",
    "postgres_warehouse": "postgres",
    "aurora_postgres_warehouse": "postgres",
    "azure_postgres_warehouse": "postgres",
    "panoply": "redshift",
    "sql_server_warehouse": "sql_server",
    "azure_sql_warehouse": "sql_server",
    "azure_sql_data_warehouse": "sql_server",
    "azure_sql_managed_db_warehouse": "sql_server",
    "big_query": "bigquery",
    "big_query_dts": "bigquery",
    "databricks": "databricks",
    "managed_data_lake": "managed_data_lake",
    "adls": "managed_data_lake",
    "onelake": "managed_data_lake",
    "new_s3_datalake": "managed_data_lake",
}

#: Dialects whose column metadata is readable with the ANSI
#: ``INFORMATION_SCHEMA.COLUMNS`` query (DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
#: IS_NULLABLE are spelled identically on all of these).
ANSI_INFORMATION_SCHEMA_TYPES = frozenset({"snowflake", "redshift", "postgres", "sql_server"})

#: Dialects that expose column metadata, but not through the ANSI query.
NON_ANSI_TYPES = frozenset({"bigquery", "databricks"})

#: Dialects with no queryable INFORMATION_SCHEMA at all. Fivetran's Managed Data
#: Lake writes Iceberg/Delta to object storage; column metadata lives in the
#: catalog (Polaris/Glue/Unity), not in a SQL information schema.
NO_INFORMATION_SCHEMA_TYPES = frozenset({"managed_data_lake"})

# Where each dialect keeps the top-level container name in ``config``. BigQuery's
# container is the GCP project; Databricks' is the Unity Catalog catalog.
_DATABASE_KEYS: dict[str, tuple[str, ...]] = {
    "snowflake": ("database",),
    "redshift": ("database",),
    "postgres": ("database",),
    "sql_server": ("database",),
    "bigquery": ("project_id", "data_set_location"),
    "databricks": ("catalog",),
}

_HOST_KEYS = ("host", "server_host_name", "server_hostname", "hostname")


@dataclass(frozen=True)
class DestinationInfo:
    """Non-secret coordinates of the Fivetran destination for a group."""

    destination_id: str | None = None
    group_id: str | None = None
    service: str | None = None
    warehouse_type: str | None = None
    database: str | None = None
    host: str | None = None
    region: str | None = None
    setup_status: str | None = None

    def supports_information_schema(self) -> bool:
        return self.warehouse_type in ANSI_INFORMATION_SCHEMA_TYPES

    def as_dict(self) -> dict:
        """Serializable form for the normalized document (never includes config)."""
        return {
            "destination_id": self.destination_id,
            "group_id": self.group_id,
            "service": self.service,
            "warehouse_type": self.warehouse_type,
            "database": self.database,
            "host": self.host,
            "region": self.region,
            "setup_status": self.setup_status,
        }


def _first_value(config: dict, keys) -> str | None:
    """First non-empty, non-redacted string among ``keys``."""
    for key in keys:
        value = config.get(key)
        if isinstance(value, str) and value.strip() and value != REDACTED:
            return value.strip()
    return None


def describe_destination(payload: dict | None) -> DestinationInfo | None:
    """Map a raw ``/v1/destinations/{id}`` payload to :class:`DestinationInfo`.

    Returns None for an empty/unusable payload. An unrecognized ``service`` still
    yields a DestinationInfo (with ``warehouse_type=None``) so the service name
    survives into the snapshot for diagnosis.
    """
    if not isinstance(payload, dict) or not payload:
        return None

    service = payload.get("service")
    service_key = (service or "").strip().lower()
    warehouse_type = SERVICE_TO_WAREHOUSE_TYPE.get(service_key)
    if service_key and warehouse_type is None:
        logger.info(
            "Unrecognized Fivetran destination service %r; set WAREHOUSE_TYPE explicitly "
            "to enable the warehouse reader.", service,
        )

    config = payload.get("config")
    config = config if isinstance(config, dict) else {}

    return DestinationInfo(
        destination_id=payload.get("id"),
        group_id=payload.get("group_id"),
        service=service,
        warehouse_type=warehouse_type,
        database=_first_value(config, _DATABASE_KEYS.get(warehouse_type or "", ("database",))),
        host=_first_value(config, _HOST_KEYS),
        region=payload.get("region"),
        setup_status=payload.get("setup_status"),
    )


def destination_from_dict(data: dict | None) -> DestinationInfo | None:
    """Rebuild a DestinationInfo from :meth:`DestinationInfo.as_dict` output."""
    if not isinstance(data, dict) or not data:
        return None
    fields = {f: data.get(f) for f in DestinationInfo.__dataclass_fields__}
    return DestinationInfo(**fields)
