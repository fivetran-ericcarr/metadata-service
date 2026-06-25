"""Pydantic models describing normalized dbt metadata.

Mirror the dict shapes produced by ``DbtNormalizer``. See ``models/fivetran.py``
for the rationale on why the pipeline uses dicts rather than these models.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class DbtTest(BaseModel):
    model_config = ConfigDict(extra="allow")

    unique_id: str
    name: str | None = None
    test_type: str | None = None
    attached_node: str | None = None
    attached_column: str | None = None
    severity: str | None = None
    tags: list[str] = []
    latest_status: str | None = None
    failures: int | None = None
    execution_time: float | None = None


class DbtModel(BaseModel):
    model_config = ConfigDict(extra="allow")

    unique_id: str
    name: str | None = None
    package_name: str | None = None
    database: str | None = None
    schema_: str | None = None
    alias: str | None = None
    relation_name: str | None = None
    materialized: str | None = None
    description: str | None = None
    columns: list[dict] = []
    tags: list[str] = []
    meta: dict = {}
    depends_on: list[str] = []
    refs: list = []
    sources: list = []
    tests: list[DbtTest] = []
    latest_status: str | None = None
    execution_time: float | None = None


class DbtSource(BaseModel):
    model_config = ConfigDict(extra="allow")

    unique_id: str
    source_name: str | None = None
    table_name: str | None = None
    database: str | None = None
    schema_: str | None = None
    identifier: str | None = None
    relation_name: str | None = None
    description: str | None = None
    columns: list[dict] = []
    freshness: dict | None = None
    freshness_result: dict | None = None
    tests: list[DbtTest] = []


class DbtLineageEdge(BaseModel):
    model_config = ConfigDict(extra="allow")

    from_unique_id: str
    to_unique_id: str
    edge_type: str
