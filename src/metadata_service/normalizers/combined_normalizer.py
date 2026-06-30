"""Combine normalized Fivetran + dbt metadata into the final document.

Builds the ``warehouse_objects`` array using deterministic matching (no fuzzy
matching), generates DQ recommendations, and assembles the canonical snapshot.
"""

from __future__ import annotations

import logging

from ..config import Settings
from ..dq.lineage import LineageGraph
from ..dq.recommendations import recommend_for_object
from ..models.common import (
    MATCH_CASE_INSENSITIVE,
    MATCH_CONFIGURED_ALIAS,
    MATCH_EXACT_SCHEMA_TABLE,
    MATCH_UNMATCHED,
    SCHEMA_VERSION,
    build_object_id,
    utcnow_iso,
)

logger = logging.getLogger(__name__)


class CombinedNormalizer:
    def __init__(self, settings: Settings | None = None, aliases: dict | None = None) -> None:
        self._settings = settings
        self._warehouse = getattr(settings, "warehouse_type", "warehouse")
        self._stale_hours = getattr(settings, "stale_sync_threshold_hours", 24)
        # aliases: {"dest_schema.dest_table": "dbt_schema.dbt_table"}
        self._aliases = {k.lower(): v.lower() for k, v in (aliases or {}).items()}

    def build(self, fivetran_normalized: dict, dbt_normalized: dict) -> dict:
        fivetran_normalized = fivetran_normalized or {}
        dbt_normalized = dbt_normalized or {}

        models = dbt_normalized.get("models") or []
        sources = dbt_normalized.get("sources") or []
        exposures = dbt_normalized.get("exposures") or []
        lineage = LineageGraph(dbt_normalized.get("lineage_edges"))

        source_index = _index_dbt(sources, id_field="identifier", name_field="table_name")
        model_index = _index_dbt(models, id_field="alias", name_field="name")
        models_by_uid = {m["unique_id"]: m for m in models}
        sources_by_uid = {s["unique_id"]: s for s in sources}

        warehouse_objects: list[dict] = []
        all_recommendations: list[dict] = []

        for conn in fivetran_normalized.get("connections", []) or []:
            for table in conn.get("tables", []) or []:
                obj = self._build_object(
                    conn, table, source_index, model_index,
                    sources_by_uid, models_by_uid, lineage, exposures,
                )
                recs = recommend_for_object(obj, stale_threshold_hours=self._stale_hours)
                self._apply_recommendations(obj, recs)
                obj["dq_summary"] = self._summarize(obj, recs)
                warehouse_objects.append(obj)
                all_recommendations.extend(recs)

        errors = list(fivetran_normalized.get("errors") or []) + list(dbt_normalized.get("errors") or [])

        return {
            "generated_at": utcnow_iso(),
            "version": SCHEMA_VERSION,
            "sources": {
                "fivetran": {
                    "extracted_at": fivetran_normalized.get("extracted_at"),
                    "connections": fivetran_normalized.get("connections") or [],
                },
                "dbt": {
                    "extracted_at": dbt_normalized.get("extracted_at"),
                    "projects": dbt_normalized.get("projects") or [],
                    "environments": dbt_normalized.get("environments") or [],
                    "jobs": dbt_normalized.get("jobs") or [],
                    "runs": dbt_normalized.get("runs") or [],
                    "models": models,
                    "sources": sources,
                    "tests": dbt_normalized.get("tests") or [],
                    "exposures": exposures,
                    "lineage_edges": dbt_normalized.get("lineage_edges") or [],
                },
            },
            "warehouse_objects": warehouse_objects,
            "dq_recommendations": all_recommendations,
            "schema_drift": [],
            "errors": errors,
        }

    # -- per-object -------------------------------------------------------
    def _build_object(self, conn, table, source_index, model_index,
                      sources_by_uid, models_by_uid, lineage, exposures=None) -> dict:
        dest_schema = table.get("destination_schema")
        dest_table = table.get("destination_table")
        object_id = build_object_id(None, dest_schema, dest_table, warehouse=self._warehouse)

        source_obj, confidence, notes = self._match(dest_schema, dest_table, source_index)
        model_uids: list[str] = []
        source_uid = None

        if source_obj is not None:
            source_uid = source_obj["unique_id"]
            model_uids = [d for d in lineage.descendants(source_uid) if d.startswith("model.")]
        else:
            model_obj, mconf, mnotes = self._match(dest_schema, dest_table, model_index)
            if model_obj is not None:
                confidence, notes = mconf, mnotes
                model_uids = [model_obj["unique_id"]]

        object_tests = self._collect_tests(source_obj, model_uids, models_by_uid)
        freshness = self._freshness(source_obj)
        object_exposures = self._collect_exposures(source_uid, model_uids, exposures)

        columns = self._build_columns(table, source_obj, model_uids, models_by_uid, object_tests)

        return {
            "object_id": object_id,
            "database": None,
            "schema": dest_schema,
            "name": dest_table,
            "object_type": "table",
            "origin": {
                "system": "fivetran",
                "connection_id": conn.get("connection_id"),
                "connector_service": conn.get("connector_service"),
                "source_schema": table.get("source_schema"),
                "source_table": table.get("source_table"),
                "last_successful_sync": conn.get("last_successful_sync"),
                "sync_state": conn.get("sync_state"),
                "setup_state": conn.get("setup_state"),
                "enabled": table.get("enabled", True),
            },
            "dbt": {
                "source_unique_id": source_uid,
                "model_unique_ids": model_uids,
                "tests": object_tests,
                "exposures": object_exposures,
                "freshness": freshness,
            },
            "columns": columns,
            "match_confidence": confidence,
            "match_notes": notes,
        }

    def _match(self, dest_schema, dest_table, index):
        """Deterministic matching against a dbt index. Returns (obj, confidence, notes)."""
        if not dest_schema or not dest_table:
            return None, MATCH_UNMATCHED, ["missing destination schema/table"]

        exact_key = (dest_schema, dest_table)
        if exact_key in index["exact"]:
            return index["exact"][exact_key], MATCH_EXACT_SCHEMA_TABLE, []

        alias_target = self._aliases.get(f"{dest_schema}.{dest_table}".lower())
        if alias_target:
            ci_key = tuple(alias_target.split(".", 1))
            if ci_key in index["ci"]:
                return index["ci"][ci_key], MATCH_CONFIGURED_ALIAS, [f"alias -> {alias_target}"]

        ci_key = (dest_schema.lower(), dest_table.lower())
        if ci_key in index["ci"]:
            return index["ci"][ci_key], MATCH_CASE_INSENSITIVE, ["matched case-insensitively"]

        return None, MATCH_UNMATCHED, []

    @staticmethod
    def _collect_exposures(source_uid, model_uids, exposures) -> list[dict]:
        """Exposures whose lineage touches this object's source or downstream models."""
        if not exposures:
            return []
        owned = set(model_uids)
        if source_uid:
            owned.add(source_uid)
        out: list[dict] = []
        for exp in exposures:
            if set(exp.get("depends_on") or []) & owned:
                out.append({
                    "name": exp.get("name"),
                    "label": exp.get("label"),
                    "type": exp.get("type"),
                    "maturity": exp.get("maturity"),
                    "url": exp.get("url"),
                    "owner_name": exp.get("owner_name"),
                })
        return out

    @staticmethod
    def _collect_tests(source_obj, model_uids, models_by_uid) -> list[dict]:
        tests: list[dict] = []
        if source_obj:
            tests.extend(source_obj.get("tests") or [])
        for uid in model_uids:
            model = models_by_uid.get(uid)
            if model:
                tests.extend(model.get("tests") or [])
        return tests

    @staticmethod
    def _freshness(source_obj) -> dict | None:
        if not source_obj:
            return None
        result = source_obj.get("freshness_result")
        if result:
            return {"status": result.get("status"), "max_loaded_at": result.get("max_loaded_at")}
        if source_obj.get("freshness"):
            return {"status": None, "max_loaded_at": None, "configured": True}
        return None

    @staticmethod
    def _build_columns(table, source_obj, model_uids, models_by_uid, object_tests) -> list[dict]:
        # dbt column descriptions keyed by lowercased column name.
        dbt_desc: dict[str, str] = {}
        for col in (source_obj or {}).get("columns") or []:
            if col.get("description"):
                dbt_desc.setdefault((col.get("name") or "").lower(), col["description"])
        for uid in model_uids:
            for col in (models_by_uid.get(uid) or {}).get("columns") or []:
                if col.get("description"):
                    dbt_desc.setdefault((col.get("name") or "").lower(), col["description"])

        # tests per column (by attached_column).
        tests_by_col: dict[str, list[str]] = {}
        for test in object_tests:
            col = (test.get("attached_column") or "").lower()
            if not col:
                continue
            tests_by_col.setdefault(col, [])
            ttype = test.get("test_type")
            if ttype and ttype not in tests_by_col[col]:
                tests_by_col[col].append(ttype)

        columns = []
        for col in table.get("columns") or []:
            name = col.get("destination_name")
            lname = (name or "").lower()
            columns.append(
                {
                    "name": name,
                    "source_name": col.get("source_name"),
                    "enabled": col.get("enabled", True),
                    "is_primary_key": col.get("is_primary_key", False),
                    "key_constraint": col.get("key_constraint"),
                    "key_source": col.get("key_source"),
                    "hashed": col.get("hashed", False),
                    "dbt_description": dbt_desc.get(lname),
                    "dbt_tests": tests_by_col.get(lname, []),
                    "recommended_tests": [],
                }
            )
        return columns

    @staticmethod
    def _apply_recommendations(obj: dict, recs: list[dict]) -> None:
        """Fill per-column ``recommended_tests`` from column-targeted dbt_test recs."""
        by_col: dict[str, list[str]] = {}
        for rec in recs:
            if rec.get("recommendation_type") != "dbt_test":
                continue
            col = (rec.get("target") or {}).get("column")
            if not col:
                continue
            by_col.setdefault(col, [])
            if rec.get("test_name") and rec["test_name"] not in by_col[col]:
                by_col[col].append(rec["test_name"])
        for column in obj.get("columns") or []:
            column["recommended_tests"] = by_col.get(column.get("name"), [])

    def _summarize(self, obj: dict, recs: list[dict]) -> dict:
        columns = obj.get("columns") or []
        pk_cols = [c for c in columns if c.get("is_primary_key")]
        has_pk = bool(pk_cols)

        def has_pk_tests() -> bool:
            if not has_pk:
                return False
            for col in pk_cols:
                present = {t.lower() for t in (col.get("dbt_tests") or [])}
                if not {"not_null", "unique"}.issubset(present):
                    return False
            return True

        freshness = (obj.get("dbt") or {}).get("freshness") or {}
        has_freshness = bool(freshness.get("status")) or bool(freshness.get("configured"))

        failing = 0
        for test in (obj.get("dbt") or {}).get("tests") or []:
            if (test.get("status") or "").lower() in {"fail", "error", "runtime error"}:
                failing += 1
        if (freshness.get("status") or "").lower() in {"fail", "error", "runtime error"}:
            failing += 1

        rec_tests = sum(1 for r in recs if r.get("recommendation_type") == "dbt_test")
        high_risk = any(r.get("recommendation_type") == "risk" and r.get("severity") == "high" for r in recs)

        if high_risk or failing > 0:
            risk = "high"
        elif rec_tests > 0 or (has_pk and not has_pk_tests()) or obj.get("match_confidence") == MATCH_UNMATCHED:
            risk = "medium"
        else:
            risk = "low"

        return {
            "has_primary_key": has_pk,
            "has_primary_key_tests": has_pk_tests(),
            "has_freshness_check": has_freshness,
            "failing_tests_count": failing,
            "recommended_tests_count": rec_tests,
            "risk_level": risk,
        }


def _index_dbt(objects: list[dict], *, id_field: str, name_field: str) -> dict:
    """Build exact and case-insensitive (schema, table) indexes for dbt objects."""
    exact: dict[tuple, dict] = {}
    ci: dict[tuple, dict] = {}
    for obj in objects:
        schema = obj.get("schema")
        table = obj.get(id_field) or obj.get(name_field) or obj.get("name")
        if not schema or not table:
            continue
        exact.setdefault((schema, table), obj)
        ci.setdefault((schema.lower(), table.lower()), obj)
    return {"exact": exact, "ci": ci}
