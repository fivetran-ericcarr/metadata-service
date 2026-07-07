"""Snowflake reader for the Fivetran Platform Connector's ``fivetran_metadata``.

Resolves authoritative primary keys to destination schema/table/column by joining
SOURCE_COLUMN (is_primary_key) -> COLUMN_LINEAGE -> DESTINATION_COLUMN ->
DESTINATION_TABLE -> DESTINATION_SCHEMA. Requires the optional extra:
``pip install 'metadata-service[warehouse-snowflake]'``.
"""

from __future__ import annotations

import logging
import re

from ..config import Settings
from ..exceptions import MetadataServiceError

logger = logging.getLogger(__name__)

_IDENT = re.compile(r"^[A-Za-z0-9_]+$")


class SnowflakeMetadataReader:
    def __init__(self, settings: Settings) -> None:
        self._s = settings
        self._conn = None
        for name, val in (("WAREHOUSE_DATABASE", settings.warehouse_database),
                          ("WAREHOUSE_METADATA_SCHEMA", settings.warehouse_metadata_schema)):
            if not val or not _IDENT.match(val):
                raise MetadataServiceError(f"{name} must be a simple identifier, got {val!r}.")
        self._fqn = f"{settings.warehouse_database}.{settings.warehouse_metadata_schema}"

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
            account=self._s.warehouse_account,
            user=self._s.warehouse_user,
            role=self._s.warehouse_role,
            warehouse=self._s.warehouse_name,
            database=self._s.warehouse_database,
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

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
