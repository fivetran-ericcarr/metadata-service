"""Typed models and shared helpers for the metadata service."""

from .common import (
    SCHEMA_VERSION,
    build_object_id,
    snapshot_timestamp,
    utcnow_iso,
)

__all__ = [
    "SCHEMA_VERSION",
    "build_object_id",
    "snapshot_timestamp",
    "utcnow_iso",
]
