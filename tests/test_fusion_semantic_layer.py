"""dbt Fusion manifests omit the dbt Semantic Layer.

dbt Fusion 2.x writes ``manifest.json`` with empty ``metrics`` / ``semantic_models``
and no ``semantic_manifest.json``, even when the project defines them. A snapshot
built from such a run had no governed metrics, so an agent asking
``get_semantic_context("cancellation rate", kind="metric")`` was told the metric
does not exist. The extractor now carries the Semantic Layer forward from the
last run that wrote one, and an empty Semantic Layer is reported as unavailable,
not as "not defined".
"""

from __future__ import annotations

import copy

from metadata_service.config import Settings
from metadata_service.exceptions import DbtArtifactNotFoundError
from metadata_service.extractors.dbt_extractor import DbtExtractor
from metadata_service.mcp import tools
from metadata_service.normalizers import DbtNormalizer
from metadata_service.semantic import list_semantic_metrics, semantic_context
from metadata_service.storage.base import get_storage

QUOTE = "model.gw.fct_quote"
POLICY = "model.gw.fct_policy"

SEMANTIC_MODELS = {
    "semantic_model.gw.quotes": {
        "name": "quotes", "description": "Quotes (fct_quote).",
        "depends_on": {"nodes": [QUOTE]},
        "node_relation": {"relation_name": "db.gold.fct_quote"},
        "defaults": {"agg_time_dimension": "quote_date"},
        "entities": [{"name": "quote", "type": "primary", "expr": "quote_id"}],
        "dimensions": [{"name": "quote_date", "type": "time", "type_params": {"time_granularity": "day"}}],
        "measures": [{"name": "bound_quote_count", "agg": "sum",
                      "expr": "case when is_bound then 1 else 0 end"}],
    },
    "semantic_model.gw.policies": {
        "name": "policies", "description": "Policies (fct_policy).",
        "depends_on": {"nodes": [POLICY]},
        "node_relation": {"relation_name": "db.gold.fct_policy"},
        "defaults": {"agg_time_dimension": "cancellation_date"},
        "entities": [{"name": "policy", "type": "primary", "expr": "policy_id"}],
        "dimensions": [{"name": "cancellation_date", "type": "time",
                        "type_params": {"time_granularity": "day"}}],
        "measures": [{"name": "cancelled_policy_count", "agg": "sum",
                      "expr": "case when is_cancelled then 1 else 0 end"}],
    },
}

METRICS = {
    "metric.gw.bound_quotes": {
        "name": "bound_quotes", "label": "Bound Quotes", "type": "simple",
        "description": "Quotes that were bound.",
        "type_params": {"measure": {"name": "bound_quote_count"}},
        "depends_on": {"nodes": ["semantic_model.gw.quotes"]},
    },
    "metric.gw.cancelled_policies": {
        "name": "cancelled_policies", "label": "Cancelled Policies", "type": "simple",
        "description": "Policies that were cancelled.",
        "type_params": {"measure": {"name": "cancelled_policy_count"}},
        "depends_on": {"nodes": ["semantic_model.gw.policies"]},
    },
    "metric.gw.cancellation_rate": {
        "name": "cancellation_rate", "label": "Cancellation Rate", "type": "ratio",
        "description": "Share of bound policies that were cancelled (cancelled policies / bound quotes).",
        "type_params": {"numerator": {"name": "cancelled_policies"},
                        "denominator": {"name": "bound_quotes"}},
        "depends_on": {"nodes": ["metric.gw.cancelled_policies", "metric.gw.bound_quotes"]},
    },
}


def _model(uid: str) -> dict:
    name = uid.rsplit(".", 1)[-1]
    return {"resource_type": "model", "name": name, "schema": "gold", "database": "db",
            "description": f"{name} mart", "columns": {}, "depends_on": {"nodes": []}}


def _manifest(version: str, *, semantic: bool, models=(QUOTE, POLICY)) -> dict:
    return {
        "metadata": {"dbt_version": version, "generated_at": f"gen-{version}"},
        "nodes": {uid: _model(uid) for uid in models},
        "sources": {}, "exposures": {},
        "metrics": copy.deepcopy(METRICS) if semantic else {},
        "semantic_models": copy.deepcopy(SEMANTIC_MODELS) if semantic else {},
    }


RUNS = [  # newest first, like list_runs
    {"id": 300, "status": 10, "finished_at": "2026-11-04T03:11:00Z"},  # Fusion, fully reused
    {"id": 200, "status": 10, "finished_at": "2026-11-03T03:11:00Z"},  # Fusion
    {"id": 100, "status": 10, "finished_at": "2026-10-19T19:09:00Z"},  # dbt-core
]


class _Client:
    """dbt Cloud artifacts per run: ``{run_id: {path: doc}}``; missing -> 404."""

    def __init__(self, by_run: dict):
        self.by_run = by_run
        self.calls: list[tuple[int, str]] = []

    def get_run(self, account_id, run_id, include_related=None):
        return next(r for r in RUNS if r["id"] == run_id)

    def get_run_artifact(self, account_id, run_id, path, step=None):
        self.calls.append((run_id, path))
        doc = self.by_run.get(run_id, {}).get(path)
        if doc is None:
            raise DbtArtifactNotFoundError(f"{path} run {run_id}")
        return copy.deepcopy(doc)


def _fusion_history(anchor_models=(QUOTE, POLICY)) -> dict:
    run_results = {"args": {"which": "build"}, "results": []}
    return {
        300: {"manifest.json": _manifest("2.0.11", semantic=False, models=anchor_models),
              "run_results.json": run_results},
        200: {"manifest.json": _manifest("2.0.11", semantic=False), "run_results.json": run_results},
        100: {"manifest.json": _manifest("1.10.0", semantic=True), "run_results.json": run_results,
              "semantic_manifest.json": {"metrics": [], "semantic_models": []}},
    }


def _extract(client, **kw):
    errors: list[dict] = []
    artifacts = DbtExtractor(client, "1", **kw)._download_from_recent_success(
        [dict(r) for r in RUNS], errors)
    return artifacts, errors


def _doc(artifacts, errors) -> dict:
    dbt = DbtNormalizer().normalize({"artifacts": artifacts, "errors": errors})
    return {"generated_at": "2026-11-04T03:32:16Z", "version": "1.0",
            "sources": {"dbt": dbt}, "warehouse_objects": [], "errors": dbt["errors"]}


# -- extractor ----------------------------------------------------------------
def test_fusion_manifest_semantic_layer_carried_from_last_core_run():
    client = _Client(_fusion_history())
    artifacts, errors = _extract(client)

    assert artifacts["artifacts_run_id"] == 300  # everything else stays the anchor's
    sl = artifacts["semantic_layer"]
    assert sl["run_id"] == 100 and sl["dbt_version"] == "1.10.0"
    assert set(sl["metrics"]) == set(METRICS) and set(sl["semantic_models"]) == set(SEMANTIC_MODELS)
    assert sl["dropped"] == []
    # Cheap semantic_manifest probe per run; the large manifest only from the donor.
    assert (200, "semantic_manifest.json") in client.calls
    assert (200, "manifest.json") not in client.calls
    assert (100, "manifest.json") in client.calls
    err = next(e for e in errors if e["error_type"] == "SemanticLayerCarriedForward")
    assert "run 100" in err["error_message"] and "2.0.11" in err["error_message"]


def test_semantic_models_on_removed_models_are_dropped_with_their_metrics():
    client = _Client(_fusion_history(anchor_models=(QUOTE,)))  # fct_policy no longer built
    artifacts, _ = _extract(client)
    sl = artifacts["semantic_layer"]
    assert set(sl["semantic_models"]) == {"semantic_model.gw.quotes"}
    assert set(sl["metrics"]) == {"metric.gw.bound_quotes"}
    assert set(sl["dropped"]) == {"semantic_model.gw.policies", "metric.gw.cancelled_policies",
                                  "metric.gw.cancellation_rate"}


def test_dbt_core_manifest_without_semantic_layer_probes_nothing():
    history = _fusion_history()
    history[300]["manifest.json"] = _manifest("1.10.0", semantic=False)
    client = _Client(history)
    artifacts, errors = _extract(client)
    assert "semantic_layer" not in artifacts
    assert not [c for c in client.calls if c[1] == "semantic_manifest.json"]
    assert not [e for e in errors if "SemanticLayer" in e["error_type"]]


def test_lookback_zero_disables_and_reports_no_semantic_layer():
    client = _Client(_fusion_history())
    artifacts, errors = _extract(client, semantic_lookback_runs=0)
    assert "semantic_layer" not in artifacts
    err = next(e for e in errors if e["error_type"] == "NoSemanticLayer")
    assert "Metrics the project defines are absent" in err["error_message"]


# -- normalizer + semantic context --------------------------------------------
def test_cancellation_rate_resolves_from_carried_semantic_layer():
    artifacts, errors = _extract(_Client(_fusion_history()))
    doc = _doc(artifacts, errors)

    metrics = {m["name"]: m for m in doc["sources"]["dbt"]["metrics"]}
    assert metrics["cancellation_rate"]["definition_source"]["run_id"] == 100

    r = semantic_context(doc, "cancellation rate", kind="metric")
    assert r["resolved_as"] == "metric"
    s = r["subjects"][0]
    assert s["name"] == "cancellation_rate" and s["type"] == "ratio"
    num, den = s["definition"]["measures"]
    assert num["role"] == "numerator" and num["metric"] == "cancelled_policies"
    assert den["role"] == "denominator" and den["metric"] == "bound_quotes"
    assert s["definition_source"]["run_id"] == 100
    assert "semantic_layer" not in r

    # Unkinded topic query and a stem search ("cancel" -> cancellation) find it too.
    assert semantic_context(doc, "cancellation rate")["subjects"][0]["name"] == "cancellation_rate"
    names = [m["name"] for m in list_semantic_metrics(doc, search="cancel")["metrics"]]
    assert names[0] == "cancellation_rate" and "cancelled_policies" in names


def test_cancellation_rate_over_mcp_tools(tmp_path):
    artifacts, errors = _extract(_Client(_fusion_history()))
    settings = Settings(metadata_storage_backend="local", metadata_local_path=str(tmp_path))
    get_storage(settings).write_snapshot(_doc(artifacts, errors))

    r = tools.get_semantic_context("cancellation rate", kind="metric", settings=settings)
    assert r["subjects"][0]["name"] == "cancellation_rate"
    assert r["subjects"][0]["definition"]["measures"][0]["metric"] == "cancelled_policies"
    listed = tools.list_semantic_metrics(search="cancel", settings=settings)
    assert listed["metrics"][0]["name"] == "cancellation_rate"


def test_empty_semantic_layer_is_reported_as_unavailable_not_undefined():
    artifacts, errors = _extract(_Client(_fusion_history()), semantic_lookback_runs=0)
    doc = _doc(artifacts, errors)

    r = semantic_context(doc, "cancellation rate", kind="metric")
    assert r["subjects"] == []
    assert r["semantic_layer"]["available"] is False
    assert "does not mean the metric is undefined" in r["semantic_layer"]["message"]
    assert "dbt Fusion 2.0.11" in r["semantic_layer"]["reason"]
    listed = list_semantic_metrics(doc, search="cancel")
    assert listed["count"] == 0 and listed["semantic_layer"]["available"] is False


def test_stem_match_does_not_overreach():
    from metadata_service.semantic.context import _overlap

    assert _overlap({"cancel"}, {"cancellation"}) == 0.75
    assert _overlap({"rate"}, {"rating"}) == 0
    assert _overlap({"rate"}, {"rate"}) == 1.0
