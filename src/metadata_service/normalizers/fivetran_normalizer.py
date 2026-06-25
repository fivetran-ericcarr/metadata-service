"""Normalize raw Fivetran extraction payloads into stable connection records.

Defensive parsing: always use ``.get()``, tolerate missing fields, preserve both
source and destination names for every table and column.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class FivetranNormalizer:
    def normalize(self, raw: dict) -> dict:
        """Return ``{"extracted_at": ..., "connections": [...]}``."""
        connections = []
        for item in (raw or {}).get("connections", []) or []:
            normalized = self._normalize_connection(item)
            if normalized:
                connections.append(normalized)
        return {
            "extracted_at": (raw or {}).get("extracted_at"),
            "connections": connections,
            "errors": list((raw or {}).get("errors") or []),
        }

    def _normalize_connection(self, item: dict) -> dict | None:
        detail = item.get("detail") or {}
        schemas_cfg = item.get("schemas") or {}
        columns_by_table = item.get("columns") or {}

        connection_id = detail.get("id") or detail.get("connection_id")
        if not connection_id:
            logger.debug("Skipping Fivetran connection with no id")
            return None

        status = detail.get("status") or {}
        tables = self._normalize_tables(schemas_cfg, columns_by_table)

        return {
            "connection_id": connection_id,
            "connector_service": detail.get("service"),
            "group_id": detail.get("group_id"),
            "destination_schema": detail.get("schema"),
            "setup_state": status.get("setup_state"),
            "sync_state": status.get("sync_state"),
            "last_successful_sync": (
                detail.get("succeeded_at")
                or detail.get("last_successful_sync")
                or detail.get("last_synced_at")
            ),
            "schema_change_handling": schemas_cfg.get("schema_change_handling"),
            "tables": tables,
        }

    def _normalize_tables(self, schemas_cfg: dict, columns_by_table: dict) -> list[dict]:
        tables: list[dict] = []
        schema_map = (schemas_cfg or {}).get("schemas") or {}
        for schema_name, schema_obj in schema_map.items():
            schema_obj = schema_obj or {}
            dest_schema = schema_obj.get("name_in_destination") or schema_name
            table_map = schema_obj.get("tables") or {}
            for table_name, table_obj in table_map.items():
                table_obj = table_obj or {}
                key = f"{schema_name}.{table_name}"
                columns = self._normalize_columns(
                    table_obj.get("columns") or {},
                    (columns_by_table.get(key) or {}).get("columns") or {},
                )
                tables.append(
                    {
                        "source_schema": schema_name,
                        "source_table": table_name,
                        "destination_schema": dest_schema,
                        "destination_table": table_obj.get("name_in_destination") or table_name,
                        "enabled": table_obj.get("enabled", True),
                        "columns": columns,
                    }
                )
        return tables

    @staticmethod
    def _normalize_columns(schema_columns: dict, endpoint_columns: dict) -> list[dict]:
        """Merge schema-config columns with the columns endpoint (endpoint wins)."""
        merged: dict[str, dict] = {}
        for name, cfg in (schema_columns or {}).items():
            merged[name] = dict(cfg or {})
        for name, cfg in (endpoint_columns or {}).items():
            merged.setdefault(name, {})
            merged[name].update(cfg or {})

        out: list[dict] = []
        for name, cfg in merged.items():
            is_pk, key_constraint = _derive_key(cfg)
            out.append(
                {
                    "source_name": name,
                    "destination_name": cfg.get("name_in_destination") or name,
                    "enabled": cfg.get("enabled", True),
                    "is_primary_key": is_pk,
                    "key_constraint": key_constraint,
                    "hashed": bool(cfg.get("hashed", False)),
                }
            )
        return out


def _derive_key(cfg: dict) -> tuple[bool, str | None]:
    """Determine primary-key status for a Fivetran column.

    Fivetran's config API does not expose an ``is_primary_key`` field for most
    connectors. Instead, key columns are *locked* from exclusion via
    ``enabled_patch_settings`` (``allowed: false``, ``reason_code:
    "SYSTEM_COLUMN"``) with a human reason naming the constraint:

    - "...as it is a Primary Key"                      -> confident PK
    - "...primary key or a foreign key" (SaaS/SDK)     -> key, but PK/FK ambiguous

    Returns ``(is_primary_key, key_constraint)`` where ``key_constraint`` is one of
    ``"primary_key"``, ``"primary_or_foreign_key"``, or ``None``.

    An explicit ``is_primary_key`` field (file connectors / future API support)
    always takes precedence.
    """
    explicit = cfg.get("is_primary_key")
    if explicit is not None:
        return bool(explicit), ("primary_key" if explicit else None)

    eps = cfg.get("enabled_patch_settings") or {}
    reason = (eps.get("reason") or "").lower()
    locked = eps.get("allowed") is False
    if locked and eps.get("reason_code") == "SYSTEM_COLUMN" and "primary key" in reason:
        if "foreign key" in reason:
            return False, "primary_or_foreign_key"
        return True, "primary_key"
    return False, None
