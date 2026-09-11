"""Shared query for the Fivetran Platform Connector's ``fivetran_metadata`` schema.

The Platform Connector replicates the same logical schema — ``SOURCE_COLUMN``,
``COLUMN_LINEAGE``, ``DESTINATION_COLUMN``, ``DESTINATION_TABLE``,
``DESTINATION_SCHEMA`` — into every destination it supports, so one query and one
row mapper serve every reader; only the connection and the schema's fully
qualified name differ per dialect (a database-qualified name on Snowflake, just
the schema name on Postgres, which has no cross-database dot notation).
"""

from __future__ import annotations

_SELECT = "ds.name as dest_schema, dt.name as dest_table, dc.name as dest_column"


def build_primary_key_sql(
    fqn: str, connection_ids: list[str] | None = None, *, placeholder: str = "%s"
) -> tuple[str, list]:
    """Build the authoritative-PK query and its bind parameters.

    ``fqn`` is the fully-qualified ``fivetran_metadata`` schema — an identifier,
    so it is interpolated directly, which is why every caller must validate it as
    a bare identifier first (see :func:`.ansi.validate_identifier`).
    ``connection_ids`` is bound as a parameter, never interpolated.
    """
    sql = f"""
        select {_SELECT}
        from {fqn}.SOURCE_COLUMN sc
        join {fqn}.COLUMN_LINEAGE cl on cl.source_column_id = sc.id
        join {fqn}.DESTINATION_COLUMN dc on dc.id = cl.destination_column_id
        join {fqn}.DESTINATION_TABLE dt on dt.id = dc.table_id
        join {fqn}.DESTINATION_SCHEMA ds on ds.id = dt.schema_id
        where sc.is_primary_key = true
    """
    params: list = []
    if connection_ids:
        marks = ", ".join([placeholder] * len(connection_ids))
        sql += f" and sc.connection_id in ({marks})"
        params = list(connection_ids)
    return sql, params


def rows_to_pk_map(rows) -> dict[tuple[str, str], list[str]]:
    """Map result rows to ``(dest_schema_lower, dest_table_lower) -> [dest_column, ...]``.

    Keys are lower-cased (matching :func:`.base.apply_primary_keys`'s own
    lower-casing) so casing differences between the warehouse's catalog and the
    normalized Fivetran document never cause a miss. Column names are kept as the
    warehouse returned them — ``apply_primary_keys`` matches those case-insensitively
    too, but the *original* casing is what a consumer would expect to see.
    """
    pk_map: dict[tuple[str, str], list[str]] = {}
    for row in rows or []:
        schema, table, column = (list(row) + [None] * 3)[:3]
        if not (schema and table and column):
            continue
        pk_map.setdefault((str(schema).lower(), str(table).lower()), []).append(column)
    return pk_map
