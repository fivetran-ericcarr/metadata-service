"""Shared ANSI ``INFORMATION_SCHEMA.COLUMNS`` query for column-level attributes.

``DATA_TYPE``, ``CHARACTER_MAXIMUM_LENGTH`` and ``IS_NULLABLE`` are spelled
identically on Snowflake, Redshift, Postgres and SQL Server, so one query + one
row mapper serves every dialect in
:data:`~.destination.ANSI_INFORMATION_SCHEMA_TYPES`. BigQuery and Databricks are
deliberately excluded — their information schemas differ enough to need their own
statements.

Only the query is shared; connecting is each reader's job (drivers, auth modes
and parameter styles differ).
"""

from __future__ import annotations

import logging
import re

from ..exceptions import MetadataServiceError

logger = logging.getLogger(__name__)

#: Column attributes this module resolves, in ``SELECT`` order.
_SELECT = (
    "TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, "
    "DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE"
)

#: Information schemas that only ever describe the platform itself. Excluded so a
#: full-database read doesn't drag in thousands of irrelevant system columns.
_SYSTEM_SCHEMAS = ("INFORMATION_SCHEMA", "PG_CATALOG", "SYS", "PERFORMANCE_SCHEMA")

_IDENT = re.compile(r"^[A-Za-z0-9_]+$")


def validate_identifier(value: str | None, setting_name: str) -> str:
    """Validate a value that a reader will interpolate directly into SQL as an
    identifier (database/schema name) rather than bind as a parameter.

    Every reader needs this: the information-schema and ``fivetran_metadata``
    fully-qualified names can't be bound as bind parameters (drivers only bind
    values, not identifiers), so whatever produced them — a ``WAREHOUSE_*``
    setting or an auto-detected destination database — must be checked first.
    Raises :class:`MetadataServiceError` naming ``setting_name`` if it isn't a
    simple identifier.
    """
    if not value or not _IDENT.match(value):
        raise MetadataServiceError(f"{setting_name} must be a simple identifier, got {value!r}.")
    return value


def build_column_schema_sql(
    information_schema_fqn: str,
    schemas: list[str] | None = None,
    *,
    placeholder: str = "%s",
) -> tuple[str, list]:
    """Build the ANSI column-metadata query and its bind parameters.

    ``information_schema_fqn`` is the fully-qualified information schema (e.g.
    ``MY_DB.INFORMATION_SCHEMA``) — it is an identifier and so is interpolated,
    which is why every caller must validate it as a bare identifier first.
    ``schemas`` and the system-schema exclusions are bound as parameters.

    ``placeholder`` is the driver's paramstyle marker: ``%s`` for the Snowflake
    connector and psycopg, ``?`` for pyodbc.
    """
    sql = f"select {_SELECT} from {information_schema_fqn}.COLUMNS"
    params: list = []
    clauses = []

    if schemas:
        # Compared upper-cased on both sides so a lower-cased Fivetran destination
        # schema still matches a warehouse that reports identifiers upper-case.
        marks = ", ".join([placeholder] * len(schemas))
        clauses.append(f"upper(TABLE_SCHEMA) in ({marks})")
        params.extend(s.upper() for s in schemas)
    else:
        marks = ", ".join([placeholder] * len(_SYSTEM_SCHEMAS))
        clauses.append(f"upper(TABLE_SCHEMA) not in ({marks})")
        params.extend(_SYSTEM_SCHEMAS)

    if clauses:
        sql += " where " + " and ".join(clauses)
    return sql, params


def _to_nullable(value) -> bool | None:
    """Map ``IS_NULLABLE`` to a bool. ANSI spells it ``YES``/``NO``; some drivers
    hand back a real boolean. Anything else stays None rather than guessing."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        token = value.strip().upper()
        if token in ("YES", "Y", "TRUE", "T"):
            return True
        if token in ("NO", "N", "FALSE", "F"):
            return False
    return None


def _to_max_length(value) -> int | None:
    """``CHARACTER_MAXIMUM_LENGTH`` is NULL for non-character types, and some
    drivers return it as a Decimal — coerce to int, or None when not a number."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def rows_to_column_map(rows) -> dict[tuple[str, str, str], dict]:
    """Map ANSI result rows to ``(schema, table, column) -> attributes``.

    Keys are lower-cased so they join against Fivetran destination identifiers
    (which are conventionally lower-case) regardless of how the warehouse cases
    its own catalog. Rows missing any of the three key parts are skipped.
    """
    out: dict[tuple[str, str, str], dict] = {}
    for row in rows or []:
        schema, table, column, data_type, max_length, is_nullable = (list(row) + [None] * 6)[:6]
        if not (schema and table and column):
            continue
        out[(str(schema).lower(), str(table).lower(), str(column).lower())] = {
            "data_type": data_type or None,
            "max_length": _to_max_length(max_length),
            "nullable": _to_nullable(is_nullable),
        }
    return out
