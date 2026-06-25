"""Fivetran extraction orchestration.

Produces a raw extraction payload that the FivetranNormalizer consumes. A failure
on a single table's columns does not abort the run; it is captured in ``errors``.
"""

from __future__ import annotations

import logging

from ..clients.fivetran_client import FivetranClient
from ..exceptions import FivetranError
from ..models.common import utcnow_iso

logger = logging.getLogger(__name__)


class FivetranExtractor:
    def __init__(self, client: FivetranClient) -> None:
        self._client = client

    def extract(
        self,
        group_id: str | None = None,
        *,
        connected_only: bool = False,
        skip_paused: bool = False,
        enrich_connector_types: bool = True,
    ) -> dict:
        """Extract Fivetran connection, schema, table, and column metadata.

        Filters:
        - ``connected_only``: skip connections whose ``setup_state != "connected"``
          (i.e. broken/incomplete setups).
        - ``skip_paused``: skip connections that are paused (``paused`` flag set or
          ``sync_state == "paused"``).
        """
        errors: list[dict] = []
        connector_type_cache: dict[str, dict] = {}

        logger.info(
            "Fivetran extraction starting (group_id=%s, connected_only=%s, skip_paused=%s)",
            group_id or "<all>", connected_only, skip_paused,
        )
        connections_summary = self._client.list_connections(group_id=group_id)

        out_connections: list[dict] = []
        skipped_count = 0
        table_count = 0
        column_count = 0

        for summary in connections_summary:
            connection_id = summary.get("id") or summary.get("connection_id")
            if not connection_id:
                errors.append(
                    {"error_type": "MissingConnectionId", "error_message": "Connection summary had no id.", "context": summary}
                )
                continue

            try:
                detail = self._client.get_connection(connection_id)
            except FivetranError as exc:
                detail = dict(summary)
                errors.append(self._err(connection_id, None, None, exc))

            status = detail.get("status") or {}
            setup_state = status.get("setup_state")
            sync_state = status.get("sync_state")
            is_paused = bool(detail.get("paused")) or sync_state == "paused"

            if connected_only and setup_state and setup_state != "connected":
                logger.debug("Skipping connection %s (setup_state=%s)", connection_id, setup_state)
                skipped_count += 1
                continue
            if skip_paused and is_paused:
                logger.debug("Skipping paused connection %s (sync_state=%s)", connection_id, sync_state)
                skipped_count += 1
                continue

            try:
                schemas = self._client.get_connection_schemas(connection_id)
            except FivetranError as exc:
                schemas = {}
                errors.append(self._err(connection_id, None, None, exc))

            columns_by_table: dict[str, dict] = {}
            for schema_name, table_name in _iter_enabled_tables(schemas):
                table_count += 1
                try:
                    cols = self._client.get_table_columns(connection_id, schema_name, table_name)
                    columns_by_table[f"{schema_name}.{table_name}"] = cols
                    column_count += len((cols or {}).get("columns", {}) or {})
                except FivetranError as exc:
                    errors.append(self._err(connection_id, schema_name, table_name, exc))
                    continue

            service = detail.get("service")
            connector_type: dict | None = None
            if enrich_connector_types and service:
                if service not in connector_type_cache:
                    try:
                        connector_type_cache[service] = self._client.get_connector_type(service)
                    except FivetranError as exc:
                        connector_type_cache[service] = {}
                        errors.append(self._err(connection_id, None, None, exc))
                connector_type = connector_type_cache.get(service) or None

            out_connections.append(
                {
                    "detail": detail,
                    "schemas": schemas,
                    "columns": columns_by_table,
                    "connector_type": connector_type,
                }
            )

        logger.info(
            "Fivetran extraction complete: %s connections (%s skipped), %s tables, %s columns, %s errors",
            len(out_connections),
            skipped_count,
            table_count,
            column_count,
            len(errors),
        )
        return {
            "extracted_at": utcnow_iso(),
            "source": "fivetran",
            "connections": out_connections,
            "errors": errors,
        }

    @staticmethod
    def _err(connection_id: str | None, schema: str | None, table: str | None, exc: Exception) -> dict:
        return {
            "source": "fivetran",
            "connection_id": connection_id,
            "schema": schema,
            "table": table,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }


def _iter_enabled_tables(schemas: dict):
    """Yield (schema_name, table_name) for every enabled table in a schema config."""
    schema_map = ((schemas or {}).get("schemas")) or {}
    for schema_name, schema_cfg in schema_map.items():
        if schema_cfg.get("enabled") is False:
            continue
        tables = (schema_cfg or {}).get("tables") or {}
        for table_name, table_cfg in tables.items():
            if table_cfg.get("enabled") is False:
                continue
            yield schema_name, table_name
