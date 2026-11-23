"""AWS Glue Data Catalog reader (boto3).

A Managed Data Lake destination with ``should_maintain_tables_in_glue`` keeps
one Glue database per destination schema, each Iceberg table registered with
``table_type=ICEBERG`` and ``metadata_location`` parameters. Glue's column list
is the Hive projection of the Iceberg schema; per-column ``iceberg.field.optional``
parameters carry nullability. Glue does not know primary keys.

Credentials come from the standard AWS chain (``AWS_PROFILE``, SSO, env keys,
an instance/workload role); the region from the destination config.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

from .base import CatalogReadResult

logger = logging.getLogger(__name__)

_LENGTH = re.compile(r"^(?:var)?char\((\d+)\)$", re.IGNORECASE)


def _iso(value) -> str | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(value) if value else None


def parse_glue_table(t: dict, *, namespace: str) -> dict:
    """Glue ``Table`` structure -> catalog table record (see ``catalog.base``)."""
    params = {k.lower(): v for k, v in (t.get("Parameters") or {}).items()}
    sd = t.get("StorageDescriptor") or {}
    table_type = (params.get("table_type") or "").lower()
    fmt = "iceberg" if table_type == "iceberg" else (
        "delta" if "delta" in table_type or params.get("spark.sql.sources.provider") == "delta"
        else ("hive" if sd else None))

    columns: dict[str, dict] = {}
    for c in list(sd.get("Columns") or []) + list(t.get("PartitionKeys") or []):
        name = c.get("Name")
        if not name:
            continue
        cparams = c.get("Parameters") or {}
        optional = cparams.get("iceberg.field.optional")
        ctype = c.get("Type")
        m = _LENGTH.match(ctype or "")
        columns[name.lower()] = {
            "name": name,
            "data_type": ctype,
            "nullable": None if optional is None else str(optional).lower() == "true",
            "max_length": int(m.group(1)) if m else None,
        }
    return {
        "catalog": "glue",
        "namespace": namespace,
        "table": t.get("Name"),
        "table_format": fmt,
        "format_version": None,
        "location": sd.get("Location"),
        "metadata_location": params.get("metadata_location"),
        "partition_spec": [{"source_column": p.get("Name"), "transform": "identity",
                            "name": p.get("Name")} for p in t.get("PartitionKeys") or []],
        "last_updated": _iso(t.get("UpdateTime") or t.get("CreateTime")),
        "current_snapshot_id": None,
        "snapshot_at": None,
        "snapshot_operation": None,
        "total_records": None,
        "primary_keys": None,
        "columns": columns,
    }


class GlueCatalogReader:
    catalog_type = "glue"

    def __init__(self, *, region: str | None = None, catalog_id: str | None = None,
                 client=None) -> None:
        if client is None:
            try:
                import boto3  # noqa: PLC0415 - optional extra
            except ImportError as exc:  # pragma: no cover - exercised via factory
                raise ImportError("Glue reader needs boto3: pip install 'metadata-service[catalog-glue]'") from exc
            client = boto3.client("glue", region_name=region)
        self._glue = client
        self._catalog_id = catalog_id

    def _kwargs(self, **kw) -> dict:
        if self._catalog_id:
            kw["CatalogId"] = self._catalog_id
        return kw

    def read_tables(self, tables: list[tuple[str, str]]) -> CatalogReadResult:
        result = CatalogReadResult()
        by_db: dict[str, set[str]] = {}
        for ns, t in tables:
            by_db.setdefault(ns, set()).add(t.lower())
        for database, wanted in sorted(by_db.items()):
            # Glue database names are lower-case; one GetTables page-walk per
            # database is far cheaper than one GetTable per table.
            try:
                found: dict[str, dict] = {}
                paginator = self._glue.get_paginator("get_tables")
                for page in paginator.paginate(**self._kwargs(DatabaseName=database.lower())):
                    for t in page.get("TableList") or []:
                        if (t.get("Name") or "").lower() in wanted:
                            found[t["Name"].lower()] = t
            except Exception as exc:  # one database never blocks the others
                if type(exc).__name__ == "EntityNotFoundException":
                    result.missing.extend((database, t) for t in sorted(wanted))
                    continue
                result.errors.append({
                    "source": "catalog", "catalog": self.catalog_type, "schema": database,
                    "error_type": type(exc).__name__, "error_message": str(exc)[:200],
                })
                continue
            for name in sorted(wanted):
                t = found.get(name)
                if t is None:
                    result.missing.append((database, name))
                    continue
                result.tables[(database.lower(), name)] = parse_glue_table(t, namespace=database)
        return result

    def close(self) -> None:
        pass
