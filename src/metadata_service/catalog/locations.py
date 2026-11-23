"""Physical location of a table per engine/catalog, from destination facts only.

No catalog call is needed: Fivetran names lake tables the same way in every
catalog it maintains (namespace = destination schema), so a snapshot's
destination record is enough to tell an agent how to address a table on each
engine. Used by ``get_semantic_context``.
"""

from __future__ import annotations


def physical_locations(destination: dict | None, schema: str | None, table: str | None,
                       *, relation_name: str | None = None) -> list[dict]:
    """``[{"catalog", "engines", "fqn", ...}]`` for one destination table.

    ``relation_name`` (a dbt model's) wins for a warehouse destination, where
    dbt may build outside the Fivetran database.
    """
    destination = destination or {}
    out: list[dict] = []
    lake = destination.get("lake") or {}
    if lake:
        formats = lake.get("table_formats") or ["iceberg"]
        for c in lake.get("catalogs") or []:
            kind = c.get("type")
            if kind == "polaris":
                name = c.get("catalog") or destination.get("destination_id")
                out.append({"catalog": "polaris", "catalog_name": name, "namespace": schema,
                            "table": table, "format": "iceberg",
                            "fqn": f"{name}.{schema}.{table}",
                            "engines": ["duckdb", "snowflake", "spark", "trino", "pyiceberg"],
                            "endpoint": c.get("endpoint")})
            elif kind == "glue":
                out.append({"catalog": "glue", "database": (schema or "").lower(), "table": table,
                            "format": "iceberg", "region": c.get("region"),
                            "fqn": f"awsdatacatalog.{(schema or '').lower()}.{table}",
                            "engines": ["athena", "emr", "redshift_spectrum"]})
            elif kind == "unity":
                out.append({"catalog": "unity", "catalog_name": c.get("catalog"), "schema": schema,
                            "table": table, "format": "delta" if "delta" in formats else "iceberg",
                            "fqn": f"{c.get('catalog')}.{schema}.{table}",
                            "engines": ["databricks"]})
            elif kind == "biglake":
                out.append({"catalog": "biglake", "fqn": f"{c.get('project_id')}.{schema}.{table}",
                            "format": "iceberg", "engines": ["bigquery"]})
            elif kind == "onelake":
                out.append({"catalog": "onelake", "workspace": c.get("workspace"),
                            "lakehouse": c.get("lakehouse"), "format": "delta",
                            "fqn": f"{c.get('lakehouse')}.{schema}.{table}",
                            "engines": ["fabric"]})
        return out
    wtype = destination.get("warehouse_type")
    if relation_name:
        return [{"catalog": wtype or "warehouse", "fqn": relation_name.replace('"', ""),
                 "engines": [wtype] if wtype else []}]
    if wtype and schema and table:
        db = destination.get("database")
        fqn = f"{db}.{schema}.{table}" if db else f"{schema}.{table}"
        out.append({"catalog": wtype, "fqn": fqn, "engines": [wtype]})
    return out
