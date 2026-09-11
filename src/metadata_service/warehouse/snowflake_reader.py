"""Snowflake reader for destination metadata.

Two reads, from two different places in the same account:

* :meth:`SnowflakeMetadataReader.read_primary_keys` resolves authoritative
  primary keys from the Fivetran Platform Connector's ``fivetran_metadata``
  schema, joining SOURCE_COLUMN (is_primary_key) -> COLUMN_LINEAGE ->
  DESTINATION_COLUMN -> DESTINATION_TABLE -> DESTINATION_SCHEMA.
* :meth:`SnowflakeMetadataReader.read_column_schema` reads column data types,
  character lengths and nullability from the database's own
  ``INFORMATION_SCHEMA.COLUMNS`` — neither the Fivetran schema API nor the dbt
  catalog carries max_length or nullable.

Requires the optional extra: ``pip install 'metadata-service[warehouse-snowflake]'``.
"""

from __future__ import annotations

import logging
import re

from .ansi import build_column_schema_sql, rows_to_column_map
from .destination import DestinationInfo
from ..config import Settings
from ..exceptions import MetadataServiceError

logger = logging.getLogger(__name__)

_IDENT = re.compile(r"^[A-Za-z0-9_]+$")
_SNOWFLAKE_HOST_SUFFIX = ".snowflakecomputing.com"


def _account_from_host(host: str | None) -> str | None:
    """Derive a Snowflake account identifier from a destination config host
    (``<account>.snowflakecomputing.com``). Returns None for anything else."""
    if not host:
        return None
    host = host.strip().lower()
    if not host.endswith(_SNOWFLAKE_HOST_SUFFIX):
        return None
    return host[: -len(_SNOWFLAKE_HOST_SUFFIX)] or None


class SnowflakeMetadataReader:
    def __init__(self, settings: Settings, destination: DestinationInfo | None = None) -> None:
        self._s = settings
        self._conn = None
        # Explicit WAREHOUSE_* settings win; the Fivetran destination fills the
        # gaps. The credential is never auto-filled (Fivetran masks it).
        self._database = settings.warehouse_database or (
            destination.database if destination else None
        )
        self._account = settings.warehouse_account or _account_from_host(
            destination.host if destination else None
        )
        for name, val in (("WAREHOUSE_DATABASE", self._database),
                          ("WAREHOUSE_METADATA_SCHEMA", settings.warehouse_metadata_schema)):
            if not val or not _IDENT.match(val):
                raise MetadataServiceError(f"{name} must be a simple identifier, got {val!r}.")
        self._fqn = f"{self._database}.{settings.warehouse_metadata_schema}"
        self._information_schema_fqn = f"{self._database}.INFORMATION_SCHEMA"

    # -- connection -------------------------------------------------------
    def _connect(self):
        if self._conn is not None:
            return self._conn
        try:
            import snowflake.connector as sf
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise MetadataServiceError(
                "Snowflake warehouse reader requires extras: "
                "pip install 'metadata-service[warehouse-snowflake]'."
            ) from exc

        kwargs = dict(
            account=self._account,
            user=self._s.warehouse_user,
            role=self._s.warehouse_role,
            warehouse=self._s.warehouse_name,
            database=self._database,
        )
        if self._s.warehouse_private_key_path:
            kwargs["private_key"] = self._load_private_key(
                self._s.warehouse_private_key_path,
                passphrase=self._s.warehouse_private_key_passphrase,
            )
        elif self._s.warehouse_password:
            kwargs["password"] = self._s.warehouse_password
        self._conn = sf.connect(**{k: v for k, v in kwargs.items() if v is not None})
        return self._conn

    @staticmethod
    def _load_private_key(path: str, passphrase: str | None = None) -> bytes:
        """Load a PEM private key, optionally passphrase-protected
        (WAREHOUSE_PRIVATE_KEY_PASSPHRASE) — the common security posture."""
        from cryptography.hazmat.primitives import serialization

        with open(path, "rb") as fh:
            key = serialization.load_pem_private_key(
                fh.read(), password=passphrase.encode() if passphrase else None
            )
        return key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    # -- queries ----------------------------------------------------------
    def read_primary_keys(self, connection_ids: list[str] | None = None) -> dict[tuple[str, str], list[str]]:
        sql = f"""
            select ds.name as dest_schema, dt.name as dest_table, dc.name as dest_column
            from {self._fqn}.SOURCE_COLUMN sc
            join {self._fqn}.COLUMN_LINEAGE cl on cl.source_column_id = sc.id
            join {self._fqn}.DESTINATION_COLUMN dc on dc.id = cl.destination_column_id
            join {self._fqn}.DESTINATION_TABLE dt on dt.id = dc.table_id
            join {self._fqn}.DESTINATION_SCHEMA ds on ds.id = dt.schema_id
            where sc.is_primary_key = true
        """
        params: list = []
        if connection_ids:
            placeholders = ", ".join(["%s"] * len(connection_ids))
            sql += f" and sc.connection_id in ({placeholders})"
            params = list(connection_ids)

        cur = self._connect().cursor()
        try:
            cur.execute(sql, params)
            pk_map: dict[tuple[str, str], list[str]] = {}
            for schema, table, column in cur.fetchall():
                if not (schema and table and column):
                    continue
                pk_map.setdefault((schema.lower(), table.lower()), []).append(column)
            logger.info("Read %s PK columns across %s tables from %s",
                        sum(len(v) for v in pk_map.values()), len(pk_map), self._fqn)
            return pk_map
        finally:
            cur.close()

    def read_column_schema(self, schemas: list[str] | None = None) -> dict[tuple[str, str, str], dict]:
        """Read data_type/max_length/nullable from ``<db>.INFORMATION_SCHEMA.COLUMNS``.

        ``schemas`` bounds the read to the destination schemas in the snapshot;
        without it every non-system schema in the database is returned, which on a
        large account is a lot of rows for metadata nobody asked for.
        """
        sql, params = build_column_schema_sql(self._information_schema_fqn, schemas)
        cur = self._connect().cursor()
        try:
            cur.execute(sql, params)
            column_map = rows_to_column_map(cur.fetchall())
            logger.info("Read %s column definitions from %s",
                        len(column_map), self._information_schema_fqn)
            return column_map
        finally:
            cur.close()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
