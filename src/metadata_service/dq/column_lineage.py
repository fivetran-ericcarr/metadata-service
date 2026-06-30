"""Column-level lineage by parsing compiled dbt SQL with sqlglot.

dbt's APIs don't expose column-to-column lineage, so we derive it from each
model's ``compiled_code`` (in the manifest), using the warehouse column lists
from ``catalog.json`` to resolve ``SELECT *``. Produces per-hop edges
``{from_unique_id, from_column, to_unique_id, to_column}`` that chain
transitively (source -> staging -> mart).

Optional: requires the ``lineage`` extra (``sqlglot``). Returns ``[]`` if sqlglot
is unavailable so the build degrades gracefully.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_DIALECT = "snowflake"


def build_column_lineage(manifest: dict, catalog: dict) -> list[dict]:
    try:
        from sqlglot import expressions as exp
        from sqlglot.lineage import lineage as sqlglot_lineage
    except ImportError:
        logger.info("Column lineage skipped (install the 'lineage' extra for sqlglot).")
        return []

    nodes = (manifest or {}).get("nodes") or {}
    catalog = catalog or {}
    schema = _build_schema(catalog)
    name_index = _build_name_index(manifest)
    catalog_nodes = catalog.get("nodes") or {}

    edges: list[dict] = []
    seen: set[tuple] = set()
    for uid, node in nodes.items():
        if (node or {}).get("resource_type") != "model":
            continue
        sql = node.get("compiled_code")
        if not sql:
            continue
        out_cols = list(((catalog_nodes.get(uid) or {}).get("columns") or {}).keys())
        for col in out_cols:
            try:
                graph = sqlglot_lineage(col, sql, schema=schema, dialect=_DIALECT)
            except Exception:  # parser limits / exotic SQL — skip this column
                continue
            for leaf in graph.walk():
                if leaf.downstream:
                    continue
                # Resolve the real table from the leaf's source expression — its
                # ``name`` may use a query alias (r/i/p) rather than the table name.
                src = getattr(leaf, "source", None)
                if not isinstance(src, exp.Table) or not src.name:
                    continue
                table = src.name
                up_col = (getattr(leaf, "name", "") or "").rsplit(".", 1)[-1]
                if not up_col:
                    continue
                up_uid = name_index.get(table.upper())
                if not up_uid or up_uid == uid:
                    continue
                key = (up_uid, up_col.lower(), uid, col.lower())
                if key in seen:
                    continue
                seen.add(key)
                edges.append({
                    "from_unique_id": up_uid, "from_column": up_col.lower(),
                    "to_unique_id": uid, "to_column": col.lower(),
                })
    logger.info("Derived %s column-lineage edges", len(edges))
    return edges


def _build_schema(catalog: dict) -> dict:
    """Nested {db: {schema: {table: {column: type}}}} from catalog nodes + sources."""
    sch: dict = {}
    for group in ("nodes", "sources"):
        for n in (catalog.get(group) or {}).values():
            md = n.get("metadata") or {}
            db, scm, name = md.get("database"), md.get("schema"), md.get("name")
            if not (db and scm and name):
                continue
            cols = {c: (v.get("type") or "TEXT") for c, v in (n.get("columns") or {}).items()}
            sch.setdefault(db, {}).setdefault(scm, {})[name] = cols
    return sch


def _build_name_index(manifest: dict) -> dict[str, str]:
    """Upper-cased table name -> unique_id, for mapping lineage leaves back to nodes."""
    index: dict[str, str] = {}
    for uid, src in (manifest.get("sources") or {}).items():
        for nm in (src.get("identifier"), src.get("name")):
            if nm:
                index.setdefault(nm.upper(), uid)
    for uid, node in (manifest.get("nodes") or {}).items():
        if (node or {}).get("resource_type") != "model":
            continue
        for nm in (node.get("alias"), node.get("name")):
            if nm:
                index.setdefault(nm.upper(), uid)
    return index


def downstream_columns(edges: list[dict], start_uid: str, start_column: str) -> list[dict]:
    """BFS the column-edge graph from (uid, column) -> list of {unique_id, column}."""
    adj: dict[tuple, list[tuple]] = {}
    for e in edges:
        adj.setdefault((e["from_unique_id"], e["from_column"]), []).append(
            (e["to_unique_id"], e["to_column"]))
    seen: set[tuple] = set()
    queue = [(start_uid, start_column.lower())]
    out: list[dict] = []
    while queue:
        cur = queue.pop(0)
        for nxt in adj.get(cur, []):
            if nxt in seen:
                continue
            seen.add(nxt)
            out.append({"unique_id": nxt[0], "column": nxt[1]})
            queue.append(nxt)
    return out
