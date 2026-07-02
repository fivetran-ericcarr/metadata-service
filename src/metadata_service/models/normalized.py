"""Top-level normalized metadata document model.

The canonical JSON contract consumed by the agentic DQ application. The pipeline
emits plain dicts in this shape; this model documents the contract and can be
used to validate a snapshot (``NormalizedMetadata.model_validate(doc)``).
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from .common import SCHEMA_VERSION


class FivetranSection(BaseModel):
    model_config = ConfigDict(extra="allow")

    extracted_at: str | None = None
    connections: list[dict] = []


class DbtSection(BaseModel):
    model_config = ConfigDict(extra="allow")

    extracted_at: str | None = None
    projects: list[dict] = []
    environments: list[dict] = []
    jobs: list[dict] = []
    runs: list[dict] = []
    models: list[dict] = []
    sources: list[dict] = []
    tests: list[dict] = []
    lineage_edges: list[dict] = []


class MetadataSources(BaseModel):
    model_config = ConfigDict(extra="allow")

    fivetran: FivetranSection = Field(default_factory=FivetranSection)
    dbt: DbtSection = Field(default_factory=DbtSection)


class NormalizedMetadata(BaseModel):
    model_config = ConfigDict(extra="allow")

    generated_at: str
    version: str = SCHEMA_VERSION
    sources: MetadataSources = Field(default_factory=MetadataSources)
    warehouse_objects: list[dict] = []
    dq_recommendations: list[dict] = []
    metric_quality: list[dict] = []
    activations: dict = Field(default_factory=dict)
    schema_drift: list[dict] = []
    errors: list[dict] = []
