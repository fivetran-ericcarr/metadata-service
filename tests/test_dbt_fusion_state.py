"""dbt Fusion + dbt State (state-aware orchestration) artifacts.

With dbt State on, a run after a 0-row sync re-executes nothing: unchanged nodes
report ``status: reused`` ("NO-OP - No new changes on any upstreams"). The last
executed result stands, but the run's run_results.json does not say what it
was. Read alone, ``reused`` must never count as passing — the extractor carries
the last executed result forward from earlier runs, and anything it cannot
resolve stays UNVERIFIED (the activation gate warns, never allows).
"""

from __future__ import annotations

import copy

import pytest

from metadata_service.config import Settings
from metadata_service.dq.activation_gate import evaluate_syncs
from metadata_service.dq.lineage import LineageGraph
from metadata_service.exceptions import DbtArtifactNotFoundError, DbtAuthError, DbtError
from metadata_service.extractors.dbt_extractor import DbtExtractor
from metadata_service.normalizers import CombinedNormalizer, DbtNormalizer, FivetranNormalizer
from metadata_service.warehouse import apply_column_schema

from .conftest import load_fixture

M = "model.demo.stg_salesforce__account"
D = "model.demo.dim_account"
TP = "test.demo.not_null_stg_salesforce__account_id"
TU = "test.demo.unique_stg_salesforce__account_id"
TW = "test.demo.accepted_values_stg_salesforce__account_status"
TD = "test.demo.not_null_dim_account_account_id"


class _FusionClient:
    """Fake DbtClient serving per-run, per-step artifacts like dbt Cloud."""

    def __init__(self, fx: dict, *, catalog: dict | None = None, fail_runs=(), fatal_runs=()):
        self.fx = fx
        self.catalog = catalog
        self.fail_runs = set(fail_runs)
        self.fatal_runs = set(fatal_runs)
        self.calls: list[tuple[int, str, int | None]] = []

    def get_run(self, account_id, run_id, include_related=None):
        self.calls.append((run_id, "get_run", None))
        return next(r for r in self.fx["runs"] if r["id"] == run_id)

    def get_run_artifact(self, account_id, run_id, path, step=None):
        self.calls.append((run_id, path, step))
        if run_id in self.fatal_runs:
            raise DbtAuthError("token revoked")
        if run_id in self.fail_runs:
            raise DbtError("boom")
        if path == "manifest.json":
            return load_fixture("dbt_manifest.json")
        if path == "catalog.json":
            if self.catalog is None:
                raise DbtArtifactNotFoundError(path)
            return copy.deepcopy(self.catalog)
        if path == "sources.json":
            return copy.deepcopy(self.fx["sources"])
        by_step = self.fx["run_results"].get(str(run_id)) or {}
        key = "default" if step is None else str(step)
        doc = by_step.get(key)
        if doc == "default":
            doc = by_step["default"]
        if doc is None:
            raise DbtArtifactNotFoundError(f"{path} run {run_id} step {step}")
        return copy.deepcopy(doc)


def _fx() -> dict:
    return load_fixture("dbt_fusion_state_runs.json")


def _extract(client, *, lookback=10, runs=None):
    fx = client.fx
    runs = runs if runs is not None else [dict(r) for r in fx["runs"]]
    errors: list[dict] = []
    artifacts = DbtExtractor(client, "3643", reused_lookback_runs=lookback) \
        ._download_from_recent_success(runs, errors)
    return artifacts, errors


def _normalize(artifacts, errors):
    snap = DbtNormalizer().normalize({"artifacts": artifacts, "errors": errors})
    return {t["unique_id"]: t for t in snap["tests"]}, {m["unique_id"]: m for m in snap["models"]}, snap


# -- extractor: carry-forward ---------------------------------------------
def test_reused_nodes_take_their_last_executed_result():
    client = _FusionClient(_fx())
    artifacts, errors = _extract(client)

    assert artifacts["artifacts_run_id"] == 300
    assert artifacts["run_results"]["args"]["which"] == "build"  # Fusion still reports build
    assert "run_results_steps" not in artifacts  # last-step default already test-bearing
    carried = artifacts["reused_last_executed"]
    assert carried[TP] == {"status": "pass", "failures": 0, "message": "Pass", "run_id": 200,
                           "generated_at": "2026-11-04T03:06:00Z"}
    assert carried[M]["status"] == "success" and carried[M]["run_id"] == 200
    # TU was reused in 200 as well; its last execution is the dbt-core build step of
    # run 100 (whose last step is docs generate) — compiled code equal modulo whitespace.
    assert carried[TU]["status"] == "warn" and carried[TU]["failures"] == 3
    assert carried[TU]["run_id"] == 100
    assert artifacts["reused_lookback_run_ids"] == [200, 100]
    # Fusion's own carried-forward warn needs no lookup.
    assert TW not in carried


def test_changed_compiled_code_is_not_carried_forward():
    client = _FusionClient(_fx())
    artifacts, errors = _extract(client)
    assert TD not in artifacts["reused_last_executed"]
    err = next(e for e in errors if e["error_type"] == "ReusedWithoutPriorResult")
    assert err["unique_ids"] == [TD] and err["count"] == 1
    assert "UNVERIFIED" in err["error_message"]


def test_lookback_stops_once_everything_resolved():
    fx = _fx()
    fx["run_results"]["300"]["default"]["results"] = [
        r for r in fx["run_results"]["300"]["default"]["results"] if r["unique_id"] in (M, TP)]
    client = _FusionClient(fx)
    artifacts, errors = _extract(client)
    assert artifacts["reused_lookback_run_ids"] == [200]
    assert not any(c[0] == 100 for c in client.calls)
    assert not any(e["error_type"] == "ReusedWithoutPriorResult" for e in errors)


def test_no_reused_results_costs_no_extra_calls():
    fx = _fx()
    fx["run_results"]["300"] = fx["run_results"]["200"]
    fx["run_results"]["300"]["default"]["results"] = [
        r for r in fx["run_results"]["300"]["default"]["results"] if r["status"] != "reused"]
    client = _FusionClient(fx)
    artifacts, _ = _extract(client)
    assert "reused_last_executed" not in artifacts
    assert {c[0] for c in client.calls} == {300}


def test_lookback_zero_leaves_reused_unverified():
    client = _FusionClient(_fx())
    artifacts, errors = _extract(client, lookback=0)
    assert artifacts["reused_last_executed"] == {}
    err = next(e for e in errors if e["error_type"] == "ReusedWithoutPriorResult")
    assert err["count"] == 5  # M, D, TP, TU, TD
    tests, models, _ = _normalize(artifacts, errors)
    assert tests[TP]["latest_status"] == "reused"
    assert tests[TP]["reused"] is True and tests[TP]["failures"] is None
    assert models[M]["latest_status"] == "reused"


def test_newer_runs_than_the_anchor_are_never_used():
    fx = _fx()
    # A newer run that failed (no manifest) sits ahead of the anchor in the list.
    newer = {"id": 400, "status": 20, "status_humanized": "Error", "finished_at": "2026-11-04T04:00:00Z"}
    fx["runs"].insert(0, newer)
    fx["run_results"]["400"] = {"default": copy.deepcopy(fx["run_results"]["200"]["default"])}
    client = _FusionClient(fx)
    artifacts, _ = _extract(client, runs=[dict(r) for r in fx["runs"]])
    assert artifacts["artifacts_run_id"] == 300
    assert 400 not in artifacts["reused_lookback_run_ids"]


def test_lookback_error_on_one_run_is_recorded_and_walk_continues():
    client = _FusionClient(_fx(), fail_runs={200})
    artifacts, errors = _extract(client)
    assert any(e.get("run_id") == 200 and e.get("resource") == "reused_lookback" for e in errors)
    # Run 100 still resolves TU; the rest stay unverified.
    assert artifacts["reused_last_executed"][TU]["run_id"] == 100
    assert TP not in artifacts["reused_last_executed"]


def test_fatal_error_during_lookback_reraises():
    client = _FusionClient(_fx(), fatal_runs={200})
    with pytest.raises(DbtAuthError):
        _extract(client)


# -- normalizer -----------------------------------------------------------
def test_normalized_statuses_and_provenance():
    client = _FusionClient(_fx())
    artifacts, errors = _extract(client)
    tests, models, snap = _normalize(artifacts, errors)

    assert tests[TP]["latest_status"] == "pass"
    assert tests[TP]["reported_status"] == "reused"
    assert tests[TP]["reused"] is True
    assert tests[TP]["last_executed_status"] == "pass"
    assert tests[TP]["last_executed_run_id"] == 200
    assert tests[TP]["last_executed_at"] == "2026-11-04T03:06:00Z"

    # Carried failures, not the reused row's placeholder 0.
    assert tests[TU]["latest_status"] == "warn" and tests[TU]["failures"] == 3

    # Fusion's own carry-forward: status kept, flagged reused.
    assert tests[TW]["latest_status"] == "warn" and tests[TW]["failures"] == 20
    assert tests[TW]["reused"] is True and tests[TW]["last_executed_run_id"] is None

    assert tests[TD]["latest_status"] == "reused"  # unverified
    assert tests[TD]["last_executed_status"] is None

    assert models[M]["latest_status"] == "success" and models[M]["reused"] is True
    attached = {t["unique_id"]: t for t in models[M]["tests"]}
    assert attached[TP]["reused"] is True and attached[TP]["last_executed_run_id"] == 200


def test_fusion_freshness_status_is_lowercased():
    client = _FusionClient(_fx())
    artifacts, errors = _extract(client)
    _, _, snap = _normalize(artifacts, errors)
    assert snap["sources"][0]["freshness_result"]["status"] == "pass"


def test_carried_results_ignored_when_run_results_are_not_test_bearing():
    artifacts = {"manifest": load_fixture("dbt_manifest.json"),
                 "run_results": {"args": {"which": "generate"},
                                 "results": [{"unique_id": TP, "status": "reused"}]},
                 "reused_last_executed": {TP: {"status": "pass", "run_id": 1}}}
    tests, _, _ = _normalize(artifacts, [])
    assert tests[TP]["latest_status"] is None


def test_command_falls_back_to_args_command():
    from metadata_service.normalizers.dbt_normalizer import run_results_command
    assert run_results_command({"args": {"command": "build"}}) == "build"


# -- no catalog.json -------------------------------------------------------
def test_missing_catalog_falls_back_to_warehouse_types(fivetran_raw):
    """Fusion retired the docs step; if a run yields no catalog.json, column types
    must come from the warehouse INFORMATION_SCHEMA read, without errors."""
    client = _FusionClient(_fx(), catalog=None)
    artifacts, errors = _extract(client)
    assert "catalog" not in artifacts
    dbt_norm = DbtNormalizer().normalize({"artifacts": artifacts, "errors": errors})
    assert dbt_norm["models"] and all(
        c.get("data_type") is None for m in dbt_norm["models"] for c in m["columns"]
        if c["name"] == "status")

    fivetran_norm = FivetranNormalizer().normalize(fivetran_raw)
    apply_column_schema(fivetran_norm, {
        ("salesforce", "account", "id"): {"data_type": "NUMBER(38,0)", "max_length": None,
                                          "nullable": False},
    })
    doc = CombinedNormalizer(Settings()).build(fivetran_norm, dbt_norm, {})
    cols = {c["name"]: c for c in doc["warehouse_objects"][0]["columns"]}
    assert cols["id"]["data_type"] == "NUMBER(38,0)"
    assert cols["id"]["data_type_source"] == "information_schema"
    # No catalog and no warehouse match: type is unknown, never invented.
    assert cols["email"]["data_type"] is None and cols["email"]["data_type_source"] is None


# -- gate / dq_summary / metric trust --------------------------------------
def _graph():
    return LineageGraph([
        {"from_unique_id": "source.p.raw.acct", "to_unique_id": "model.p.stg_acct"},
        {"from_unique_id": "model.p.stg_acct", "to_unique_id": "model.p.dim_acct"},
    ])


_COVERAGE = [{"object_id": "warehouse://x/raw/acct",
              "dbt": {"source_unique_id": "source.p.raw.acct", "model_unique_ids": []}}]


def _models(dim_tests, stg_tests, stg_status="success"):
    return [
        {"unique_id": "model.p.dim_acct", "schema": "marts", "alias": "dim_acct", "name": "dim_acct",
         "governance": {"contract_enforced": True}, "tests": dim_tests, "latest_status": "success"},
        {"unique_id": "model.p.stg_acct", "schema": "staging", "alias": "stg_acct", "name": "stg_acct",
         "governance": {"contract_enforced": True}, "tests": stg_tests, "latest_status": stg_status},
    ]


def _sync():
    return {"sync_id": 1, "label": "x", "source_object": {"table_schema": "marts", "table_name": "dim_acct"},
            "destination_name": "sf", "destination_object": "Account", "mappings": []}


def _gate(models):
    return evaluate_syncs([_sync()], models=models, sources=[], lineage=_graph(),
                          warehouse_objects=_COVERAGE)[0]["readiness"]


def _t(uid, status, *, reused=False, failures=0, severity="ERROR"):
    return {"unique_id": uid, "name": uid, "status": status, "severity": severity,
            "failures": failures, "reused": reused}


def test_gate_allows_on_carried_forward_passes():
    r = _gate(_models([_t("a", "pass", reused=True)], [_t("b", "pass", reused=True)]))
    assert r["verdict"] == "allow"
    assert r["upstream"]["unverified_reused"] == 0


def test_gate_never_allows_on_unverified_reused():
    # Everything reused, nothing resolved: no evidence at all.
    r = _gate(_models([_t("a", "reused", reused=True, failures=None)],
                      [_t("b", "reused", reused=True, failures=None)]))
    assert r["verdict"] == "warn"
    assert r["upstream"]["tests_with_results"] == 0
    codes = {x["code"] for x in r["reasons"]}
    assert {"no_test_results", "reused_without_prior_result"} <= codes


def test_gate_warns_on_partially_unverified_reused():
    r = _gate(_models([_t("a", "pass")], [_t("b", "reused", reused=True, failures=None)]))
    assert r["verdict"] == "warn"
    assert r["upstream"]["unverified_reused"] == 1
    assert any(x["code"] == "reused_without_prior_result" for x in r["reasons"])


def test_gate_counts_unverified_reused_model():
    r = _gate(_models([_t("a", "pass")], [_t("b", "pass")], stg_status="reused"))
    assert r["verdict"] == "warn"
    assert "model.p.stg_acct" in next(
        x for x in r["reasons"] if x["code"] == "reused_without_prior_result")["detail"]


def test_gate_still_blocks_on_carried_forward_warn_failures():
    r = _gate(_models([_t("a", "warn", reused=True, failures=20, severity="WARN")],
                      [_t("b", "reused", reused=True, failures=None)]))
    assert r["verdict"] == "block"


def test_dq_summary_and_metric_trust_with_reused(fivetran_raw, dbt_raw):
    dbt_norm = DbtNormalizer().normalize(dbt_raw)
    for m in dbt_norm["models"]:
        for t in m["tests"]:
            t.update(status="reused", reused=True, failures=None)
    doc = CombinedNormalizer(Settings()).build(
        FivetranNormalizer().normalize(fivetran_raw), dbt_norm, {})
    objs = [o for o in doc["warehouse_objects"] if (o.get("dbt") or {}).get("tests")]
    assert objs
    for o in objs:
        s = o["dq_summary"]
        assert s["unverified_tests_count"] == len(o["dbt"]["tests"])
        assert s["reused_tests_count"] == len(o["dbt"]["tests"])
        assert s["failing_tests_count"] == 0
        assert s["risk_level"] in ("medium", "high")  # never "low" on unverified evidence
    mqs = [mq for mq in doc["metric_quality"] if mq["upstream_object_count"]]
    assert mqs
    for mq in mqs:
        assert mq["unverified_tests"] >= 1
        assert mq["trust_level"] != "trusted"
