"""Postgres reader for destination metadata.

Same two reads as :mod:`.snowflake_reader` — authoritative primary keys from the
Fivetran Platform Connector's ``fivetran_metadata`` schema, and column
data_type/max_length/nullable from ``INFORMATION_SCHEMA.COLUMNS`` — sharing the
same query builders (:mod:`.fivetran_metadata`, :mod:`.ansi`). Only the driver
and how the information schema is addressed differ:

* Postgres has no cross-database dot notation, so the connection is opened
  directly against the target database (``dbname=...``) and
  ``INFORMATION_SCHEMA`` is referenced unqualified rather than
  ``<database>.INFORMATION_SCHEMA`` the way Snowflake requires.
* Postgres folds unquoted identifiers to lower-case, so the identical
  ``fivetran_metadata`` join text works unchanged — no case-sensitivity
  handling needed here.

Requires the optional extra: ``pip install 'metadata-service[warehouse-postgres]'``.
"""

from __future__ import annotations

import logging

from .ansi import build_column_schema_sql, rows_to_column_map, validate_identifier
from .destination import DestinationInfo
from .fivetran_metadata import build_primary_key_sql, rows_to_pk_map
from ..config import Settings
from ..exceptions import MetadataServiceError

logger = logging.getLogger(__name__)

_DEFAULT_PORT = 5432

#: Postgres has no database-qualified INFORMATION_SCHEMA (unlike Snowflake);
#: the connection is already scoped to the right database via ``dbname``.
_INFORMATION_SCHEMA = "information_schema"


class PostgresMetadataReader:
    def __init__(self, settings: Settings, destination: DestinationInfo | None = None) -> None:
        self._s = settings
        self._conn = None
        # Explicit WAREHOUSE_* settings win; the Fivetran destination fills the
        # gaps. The credential is never auto-filled (Fivetran masks it).
        self._database = settings.warehouse_database or (
            destination.database if destination else None
        )
        self._host = settings.warehouse_host or (destination.host if destination else None)
        self._port = settings.warehouse_port or (destination.port if destination else None) or _DEFAULT_PORT
        if not self._database or not self._host:
            raise MetadataServiceError(
                "Postgres warehouse reader needs a host and database: set WAREHOUSE_HOST + "
                "WAREHOUSE_DATABASE, or configure FIVETRAN_GROUP_ID so they can be "
                "auto-detected from the Fivetran destination."
            )
        if not settings.warehouse_password:
            # Unlike Snowflake, this reader has no key-pair auth path — Postgres
            # warehouse destinations authenticate with a password.
            raise MetadataServiceError(
                "Postgres warehouse reader requires WAREHOUSE_PASSWORD "
                "(WAREHOUSE_PRIVATE_KEY_PATH is Snowflake-only)."
            )
        validate_identifier(settings.warehouse_metadata_schema, "WAREHOUSE_METADATA_SCHEMA")
        self._fqn = settings.warehouse_metadata_schema

    # -- connection -------------------------------------------------------
    def _connect(self):
        if self._conn is not None:
            return self._conn
        try:
            import psycopg2
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise MetadataServiceError(
                "Postgres warehouse reader requires extras: "
                "pip install 'metadata-service[warehouse-postgres]'."
            ) from exc

        self._conn = psycopg2.connect(
            host=self._host,
            port=self._port,
            dbname=self._database,
            user=self._s.warehouse_user,
            password=self._s.warehouse_password,
        )
        return self._conn

    # -- queries ----------------------------------------------------------
    def read_primary_keys(self, connection_ids: list[str] | None = None) -> dict[tuple[str, str], list[str]]:
        sql, params = build_primary_key_sql(self._fqn, connection_ids)
        cur = self._connect().cursor()
        try:
            cur.execute(sql, params)
            pk_map = rows_to_pk_map(cur.fetchall())
            logger.info("Read %s PK columns across %s tables from %s",
                        sum(len(v) for v in pk_map.values()), len(pk_map), self._fqn)
            return pk_map
        finally:
            cur.close()

    def read_column_schema(self, schemas: list[str] | None = None) -> dict[tuple[str, str, str], dict]:
        """Read data_type/max_length/nullable from ``information_schema.columns``.

        ``schemas`` bounds the read to the destination schemas in the snapshot;
        without it every non-system schema in the database is returned.
        """
        sql, params = build_column_schema_sql(_INFORMATION_SCHEMA, schemas)
        cur = self._connect().cursor()
        try:
            cur.execute(sql, params)
            column_map = rows_to_column_map(cur.fetchall())
            logger.info("Read %s column definitions from %s.%s",
                        len(column_map), self._database, _INFORMATION_SCHEMA)
            return column_map
        finally:
            cur.close()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
