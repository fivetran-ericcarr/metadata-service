"""Pydantic models describing normalized Fivetran metadata.

These mirror the dict shapes produced by ``FivetranNormalizer``. The pipeline
itself works on plain dicts (so the JSON contract is the source of truth); these
models exist for documentation, optional validation, and typed API responses.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class FivetranColumn(BaseModel):
    model_config = ConfigDict(extra="allow")

    source_name: str
    destination_name: str
    enabled: bool = True
    is_primary_key: bool = False
    hashed: bool = False


class FivetranTable(BaseModel):
    model_config = ConfigDict(extra="allow")

    source_schema: str
    source_table: str
    destination_schema: str
    destination_table: str
    enabled: bool = True
    columns: list[FivetranColumn] = []


class FivetranConnection(BaseModel):
    model_config = ConfigDict(extra="allow")

    connection_id: str
    connector_service: str | None = None
    group_id: str | None = None
    destination_schema: str | None = None
    setup_state: str | None = None
    sync_state: str | None = None
    last_successful_sync: str | None = None
    schema_change_handling: str | None = None
    tables: list[FivetranTable] = []
