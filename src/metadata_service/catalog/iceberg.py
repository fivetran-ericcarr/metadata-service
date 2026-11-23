"""Parse Apache Iceberg table metadata into the catalog reader's table record.

Input is the ``metadata`` object of an Iceberg REST ``LoadTableResult`` (the
same JSON as a ``vN.metadata.json`` file), so the parser serves any Iceberg
REST catalog — Fivetran-managed Polaris, Snowflake Open Catalog, Unity's
Iceberg REST endpoint, Glue's Iceberg REST endpoint.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

_FIXED = re.compile(r"^fixed\[(\d+)\]$")


def _iso_ms(ms) -> str | None:
    if not isinstance(ms, (int, float)):
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def render_type(t) -> str | None:
    """Iceberg type -> display string. Primitives are already strings
    (``long``, ``decimal(38,10)``, ``timestamptz``); nested types are objects."""
    if isinstance(t, str):
        # Catalogs differ on spacing ("decimal(10, 2)" vs Glue's "decimal(10,2)");
        # one spelling keeps column-type drift from flapping between them.
        return re.sub(r"\s+", "", t)
    if not isinstance(t, dict):
        return None
    kind = t.get("type")
    if kind == "list":
        return f"list<{render_type(t.get('element'))}>"
    if kind == "map":
        return f"map<{render_type(t.get('key'))},{render_type(t.get('value'))}>"
    if kind == "struct":
        return "struct"
    return str(kind) if kind else None


def _max_length(t) -> int | None:
    if isinstance(t, str):
        m = _FIXED.match(t)
        if m:
            return int(m.group(1))
    return None


def _current_schema(metadata: dict) -> dict:
    schemas = metadata.get("schemas") or []
    current = metadata.get("current-schema-id")
    for s in schemas:
        if s.get("schema-id") == current:
            return s
    if schemas:
        return schemas[-1]
    return metadata.get("schema") or {}  # format v1


def _current_spec(metadata: dict) -> list[dict]:
    specs = metadata.get("partition-specs") or []
    default = metadata.get("default-spec-id")
    for s in specs:
        if s.get("spec-id") == default:
            return s.get("fields") or []
    return metadata.get("partition-spec") or []  # format v1


def parse_table_metadata(metadata: dict, *, catalog: str, namespace: str, table: str,
                         metadata_location: str | None = None) -> dict:
    """Iceberg metadata -> catalog table record (see ``catalog.base``)."""
    metadata = metadata or {}
    schema = _current_schema(metadata)
    fields = schema.get("fields") or []
    by_id = {f.get("id"): f for f in fields}

    columns: dict[str, dict] = {}
    for f in fields:
        name = f.get("name")
        if not name:
            continue
        columns[name.lower()] = {
            "name": name,
            "data_type": render_type(f.get("type")),
            "nullable": not bool(f.get("required")),
            "max_length": _max_length(f.get("type")),
            "field_id": f.get("id"),
            "doc": f.get("doc"),
        }

    identifier_ids = schema.get("identifier-field-ids") or []
    primary_keys = [by_id[i]["name"] for i in identifier_ids if i in by_id] or None

    partition_spec = []
    for pf in _current_spec(metadata):
        src = by_id.get(pf.get("source-id"), {})
        partition_spec.append({
            "source_column": src.get("name"),
            "transform": pf.get("transform"),
            "name": pf.get("name"),
        })

    snapshot = None
    current_id = metadata.get("current-snapshot-id")
    for s in metadata.get("snapshots") or []:
        if s.get("snapshot-id") == current_id:
            snapshot = s
    summary = (snapshot or {}).get("summary") or {}
    total = summary.get("total-records")
    try:
        total = int(total) if total is not None else None
    except (TypeError, ValueError):
        total = None

    return {
        "catalog": catalog,
        "namespace": namespace,
        "table": table,
        "table_format": "iceberg",
        "format_version": metadata.get("format-version"),
        "location": metadata.get("location"),
        "metadata_location": metadata_location,
        "partition_spec": partition_spec,
        "last_updated": _iso_ms(metadata.get("last-updated-ms")),
        "current_snapshot_id": current_id if current_id not in (-1, None) else None,
        "snapshot_at": _iso_ms((snapshot or {}).get("timestamp-ms")),
        "snapshot_operation": summary.get("operation"),
        "total_records": total,
        "primary_keys": primary_keys,
        "columns": columns,
    }
