"""Normalize raw dbt artifacts (manifest/catalog/run_results/sources) into
stable model, source, test, and lineage records.

dbt artifacts vary by version, so parsing is defensive: missing fields are
tolerated and surfaced as warnings rather than raised.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class DbtNormalizer:
    def normalize(self, raw: dict) -> dict:
        raw = raw or {}
        artifacts = raw.get("artifacts") or {}
        manifest = artifacts.get("manifest") or {}
        catalog = artifacts.get("catalog") or {}
        run_results = artifacts.get("run_results") or {}
        sources_artifact = artifacts.get("sources") or {}

        warnings: list[dict] = list(raw.get("errors") or [])

        run_status = self._index_run_results(run_results)
        freshness = self._index_freshness(sources_artifact)
        catalog_nodes = (catalog.get("nodes") or {}) if isinstance(catalog, dict) else {}

        manifest_nodes = manifest.get("nodes") or {}
        manifest_sources = manifest.get("sources") or {}
        manifest_exposures = manifest.get("exposures") or {}

        models = self._build_models(manifest_nodes, catalog_nodes, run_status, warnings)
        sources = self._build_sources(manifest_sources, freshness, warnings)
        tests = self._build_tests(manifest_nodes, run_status, warnings)

        self._attach_tests(tests, models, sources)
        lineage_edges = self._build_lineage(manifest_nodes, manifest_exposures)
        exposures = self._build_exposures(manifest_exposures)
        metrics, semantic_models = self._build_metrics(
            manifest.get("metrics") or {}, manifest.get("semantic_models") or {})
        from ..dq.column_lineage import build_column_lineage
        column_lineage_edges = build_column_lineage(manifest, catalog)

        return {
            "extracted_at": raw.get("extracted_at"),
            "projects": _passthrough(raw.get("projects"), ("id", "name", "account_id")),
            "environments": _passthrough(raw.get("environments"), ("id", "name", "type", "project_id", "deployment_type")),
            "jobs": _passthrough(raw.get("jobs"), ("id", "name", "project_id", "environment_id")),
            "runs": _passthrough(raw.get("runs"), ("id", "job_definition_id", "status", "status_humanized", "finished_at")),
            "models": models,
            "sources": sources,
            "tests": tests,
            "exposures": exposures,
            "metrics": metrics,
            "semantic_models": semantic_models,
            "lineage_edges": lineage_edges,
            "column_lineage_edges": column_lineage_edges,
            "errors": warnings,
        }

    @staticmethod
    def _build_metrics(metrics: dict, semantic: dict) -> tuple[list[dict], list[dict]]:
        """Extract metrics + semantic models, resolving each metric to its
        upstream dbt model unique_ids (metric -> semantic_model/metric -> model)."""
        sm_models: dict[str, list[str]] = {}
        sm_out: list[dict] = []
        for uid, sm in (semantic or {}).items():
            models = [n for n in (sm.get("depends_on") or {}).get("nodes") or [] if n.startswith("model.")]
            sm_models[uid] = models
            sm_out.append({
                "unique_id": uid, "name": sm.get("name"),
                "model_unique_ids": models,
                "measures": [m.get("name") for m in sm.get("measures") or []],
            })

        deps = {uid: (m.get("depends_on") or {}).get("nodes") or [] for uid, m in (metrics or {}).items()}

        def resolve(uid: str, seen: set[str]) -> set[str]:
            if uid in seen:
                return set()
            seen.add(uid)
            found: set[str] = set()
            for dep in deps.get(uid, []):
                if dep.startswith("semantic_model."):
                    found |= set(sm_models.get(dep, []))
                elif dep.startswith("metric."):
                    found |= resolve(dep, seen)
                elif dep.startswith("model."):
                    found.add(dep)
            return found

        met_out: list[dict] = []
        for uid, m in (metrics or {}).items():
            met_out.append({
                "unique_id": uid, "name": m.get("name"), "label": m.get("label"),
                "type": m.get("type"), "description": m.get("description"),
                "model_unique_ids": sorted(resolve(uid, set())),
            })
        return met_out, sm_out

    @staticmethod
    def _build_exposures(exposures: dict) -> list[dict]:
        out = []
        for uid, exp in (exposures or {}).items():
            exp = exp or {}
            owner = exp.get("owner") or {}
            out.append({
                "unique_id": uid,
                "name": exp.get("name"),
                "label": exp.get("label"),
                "type": exp.get("type"),
                "maturity": exp.get("maturity"),
                "url": exp.get("url"),
                "description": exp.get("description"),
                "owner_name": owner.get("name"),
                "owner_email": owner.get("email"),
                "depends_on": (exp.get("depends_on") or {}).get("nodes") or [],
            })
        return out

    # -- indexes ----------------------------------------------------------
    @staticmethod
    def _index_run_results(run_results: dict) -> dict[str, dict]:
        index: dict[str, dict] = {}
        for result in (run_results or {}).get("results", []) or []:
            uid = result.get("unique_id")
            if not uid:
                continue
            index[uid] = {
                "status": result.get("status"),
                "execution_time": result.get("execution_time"),
                "failures": result.get("failures"),
                "message": result.get("message"),
            }
        return index

    @staticmethod
    def _index_freshness(sources_artifact: dict) -> dict[str, dict]:
        index: dict[str, dict] = {}
        for result in (sources_artifact or {}).get("results", []) or []:
            uid = result.get("unique_id")
            if not uid:
                continue
            timing = result.get("max_loaded_at")
            criteria = result.get("criteria") or {}
            index[uid] = {
                "status": result.get("status"),
                "max_loaded_at": timing,
                "snapshotted_at": result.get("snapshotted_at"),
                "criteria": criteria,
            }
        return index

    # -- builders ---------------------------------------------------------
    def _build_models(self, nodes: dict, catalog_nodes: dict, run_status: dict, warnings: list) -> list[dict]:
        models = []
        for uid, node in nodes.items():
            if (node or {}).get("resource_type") != "model":
                continue
            try:
                config = node.get("config") or {}
                catalog_cols = ((catalog_nodes.get(uid) or {}).get("columns")) or {}
                status = run_status.get(uid) or {}
                models.append(
                    {
                        "unique_id": uid,
                        "name": node.get("name"),
                        "package_name": node.get("package_name"),
                        "database": node.get("database"),
                        "schema": node.get("schema"),
                        "alias": node.get("alias"),
                        "relation_name": node.get("relation_name"),
                        "materialized": config.get("materialized") or node.get("materialized"),
                        "description": node.get("description"),
                        "columns": _build_columns(node.get("columns") or {}, catalog_cols),
                        "tags": node.get("tags") or [],
                        "meta": node.get("meta") or config.get("meta") or {},
                        "depends_on": (node.get("depends_on") or {}).get("nodes") or [],
                        "refs": node.get("refs") or [],
                        "sources": node.get("sources") or [],
                        "tests": [],
                        "latest_status": status.get("status"),
                        "execution_time": status.get("execution_time"),
                    }
                )
            except Exception as exc:  # pragma: no cover - defensive
                warnings.append({"source": "dbt", "unique_id": uid, "error_type": type(exc).__name__,
                                 "error_message": f"Failed to normalize model: {exc}"})
        return models

    def _build_sources(self, sources: dict, freshness: dict, warnings: list) -> list[dict]:
        out = []
        for uid, node in sources.items():
            if (node or {}).get("resource_type") != "source":
                continue
            try:
                out.append(
                    {
                        "unique_id": uid,
                        "source_name": node.get("source_name"),
                        "table_name": node.get("name"),
                        "database": node.get("database"),
                        "schema": node.get("schema"),
                        "identifier": node.get("identifier"),
                        "relation_name": node.get("relation_name"),
                        "description": node.get("description"),
                        "columns": _build_columns(node.get("columns") or {}, {}),
                        "freshness": node.get("freshness"),
                        "freshness_result": freshness.get(uid),
                        "tests": [],
                    }
                )
            except Exception as exc:  # pragma: no cover - defensive
                warnings.append({"source": "dbt", "unique_id": uid, "error_type": type(exc).__name__,
                                 "error_message": f"Failed to normalize source: {exc}"})
        return out

    def _build_tests(self, nodes: dict, run_status: dict, warnings: list) -> list[dict]:
        tests = []
        for uid, node in nodes.items():
            if (node or {}).get("resource_type") != "test":
                continue
            try:
                test_meta = node.get("test_metadata") or {}
                config = node.get("config") or {}
                attached_node = node.get("attached_node")
                if not attached_node:
                    deps = (node.get("depends_on") or {}).get("nodes") or []
                    attached_node = deps[0] if deps else None
                attached_column = node.get("column_name") or (test_meta.get("kwargs") or {}).get("column_name")
                status = run_status.get(uid) or {}
                tests.append(
                    {
                        "unique_id": uid,
                        "name": node.get("name"),
                        "test_type": test_meta.get("name") or _infer_test_type(node.get("name")),
                        "attached_node": attached_node,
                        "attached_column": attached_column,
                        "severity": (config.get("severity") or "error"),
                        "tags": node.get("tags") or [],
                        "latest_status": status.get("status"),
                        "failures": status.get("failures"),
                        "execution_time": status.get("execution_time"),
                    }
                )
            except Exception as exc:  # pragma: no cover - defensive
                warnings.append({"source": "dbt", "unique_id": uid, "error_type": type(exc).__name__,
                                 "error_message": f"Failed to normalize test: {exc}"})
        return tests

    @staticmethod
    def _attach_tests(tests: list[dict], models: list[dict], sources: list[dict]) -> None:
        by_uid: dict[str, dict] = {}
        for obj in models:
            by_uid[obj["unique_id"]] = obj
        for obj in sources:
            by_uid[obj["unique_id"]] = obj
        for test in tests:
            target = by_uid.get(test.get("attached_node"))
            if target is None:
                continue
            target["tests"].append(
                {
                    "unique_id": test["unique_id"],
                    "name": test.get("name"),
                    "test_type": test.get("test_type"),
                    "attached_column": test.get("attached_column"),
                    "status": test.get("latest_status"),
                    "severity": test.get("severity"),
                }
            )

    @staticmethod
    def _build_lineage(nodes: dict, exposures: dict) -> list[dict]:
        edges: list[dict] = []
        seen: set[tuple[str, str]] = set()

        def add(frm: str, to: str, edge_type: str) -> None:
            key = (frm, to)
            if frm and to and key not in seen:
                seen.add(key)
                edges.append({"from_unique_id": frm, "to_unique_id": to, "edge_type": edge_type})

        for uid, node in nodes.items():
            if (node or {}).get("resource_type") != "model":
                continue
            for dep in (node.get("depends_on") or {}).get("nodes") or []:
                edge_type = "source->model" if str(dep).startswith("source.") else "model->model"
                add(dep, uid, edge_type)

        for uid, exposure in (exposures or {}).items():
            for dep in (exposure.get("depends_on") or {}).get("nodes") or []:
                add(dep, uid, "model->exposure")

        return edges


# -- module helpers -------------------------------------------------------
def _build_columns(manifest_columns: dict, catalog_columns: dict) -> list[dict]:
    out = []
    for name, col in (manifest_columns or {}).items():
        col = col or {}
        catalog_col = catalog_columns.get(name) or {}
        out.append(
            {
                "name": name,
                "description": col.get("description"),
                "data_type": col.get("data_type") or catalog_col.get("type"),
                "tags": col.get("tags") or [],
                "meta": col.get("meta") or {},
            }
        )
    return out


def _infer_test_type(name: str | None) -> str | None:
    if not name:
        return None
    lowered = name.lower()
    for known in ("not_null", "unique", "accepted_values", "relationships"):
        if known in lowered:
            return known
    return None


def _passthrough(items, fields: tuple[str, ...]) -> list[dict]:
    out = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        out.append({f: item.get(f) for f in fields if f in item})
    return out
