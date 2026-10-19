"""dbt Cloud multi-step runs: run_results.json must come from the build/test step.

dbt Cloud serves artifacts as of a run's LAST step. A job ending in ``dbt docs
generate`` returns compile ``success`` for every node, which used to overwrite
the build's ``warn``/``fail`` test statuses — the snapshot failed OPEN.
"""

from __future__ import annotations

import copy
import json

import httpx
import pytest

from metadata_service.clients.dbt_client import DbtClient
from metadata_service.config import Settings
from metadata_service.exceptions import DbtArtifactNotFoundError, DbtAuthError, DbtError
from metadata_service.extractors.dbt_extractor import DbtExtractor
from metadata_service.normalizers import DbtNormalizer

from .conftest import load_fixture

T_WARN_1 = "test.demo.unique_stg_salesforce__account_id"
T_WARN_2 = "test.demo.accepted_values_stg_salesforce__account_status"
T_PASS = "test.demo.not_null_stg_salesforce__account_id"
MODEL = "model.demo.stg_salesforce__account"


class _StepClient:
    """Fake DbtClient serving per-step run_results like dbt Cloud's ``?step=``."""

    def __init__(self, run_results: dict, run_steps: list[dict], manifest: dict):
        self.run_results = run_results  # {"default" | "<index>": doc}
        self.run_steps = run_steps
        self.manifest = manifest
        self.calls: list[tuple[str, int | None]] = []

    def get_run(self, account_id, run_id, include_related=None):
        self.calls.append(("get_run", None))
        return {"id": run_id, "run_steps": self.run_steps}

    def get_run_artifact(self, account_id, run_id, path, step=None):
        self.calls.append((path, step))
        if path == "manifest.json":
            return self.manifest
        if path != "run_results.json":
            raise DbtArtifactNotFoundError(path)
        key = "default" if step is None else str(step)
        if key not in self.run_results:
            raise DbtArtifactNotFoundError(f"{path} step {step}")
        return copy.deepcopy(self.run_results[key])


def _multistep_client(**overrides) -> tuple[_StepClient, dict]:
    fx = load_fixture("dbt_multistep_run.json")
    fx.update(overrides)
    return _StepClient(fx["run_results"], fx["run_steps"], load_fixture("dbt_manifest.json")), fx["run"]


def _extract(client, run) -> tuple[dict, list[dict]]:
    errors: list[dict] = []
    artifacts = DbtExtractor(client, "3643")._download_from_recent_success([run], errors)
    return artifacts, errors


def _test_statuses(artifacts, errors) -> dict[str, str | None]:
    snap = DbtNormalizer().normalize({"artifacts": artifacts, "errors": errors})
    return {t["unique_id"]: t["latest_status"] for t in snap["tests"]}, snap["errors"]


def test_build_warns_survive_trailing_docs_generate():
    client, run = _multistep_client()
    artifacts, errors = _extract(client, run)

    assert artifacts["run_results"]["args"]["which"] == "build"
    assert artifacts["run_results_steps"] == [5]
    statuses, snap_errors = _test_statuses(artifacts, errors)
    assert statuses[T_WARN_1] == "warn"
    assert statuses[T_WARN_2] == "warn"
    assert statuses[T_PASS] == "pass"
    assert "success" not in {statuses[T_WARN_1], statuses[T_WARN_2], statuses[T_PASS]}
    assert not any(e["error_type"] == "NonTestRunResults" for e in snap_errors)
    # Every step was probed once; steps without run_results (clone/deps/freshness) 404'd.
    assert sorted(s for p, s in client.calls if p == "run_results.json" and s) == [1, 2, 3, 4, 5, 6]


def test_last_step_build_costs_no_extra_calls():
    fx = load_fixture("dbt_multistep_run.json")
    build = fx["run_results"]["5"]
    client, run = _multistep_client(run_results={"default": build})
    artifacts, _ = _extract(client, run)
    assert artifacts["run_results"] == build
    assert "run_results_steps" not in artifacts
    assert not any(s is not None for _, s in client.calls)
    assert ("get_run", None) not in client.calls


def test_run_steps_already_on_run_skip_get_run():
    client, run = _multistep_client()
    run = dict(run, run_steps=client.run_steps)
    artifacts, _ = _extract(client, run)
    assert artifacts["run_results_steps"] == [5]
    assert ("get_run", None) not in client.calls


def _rr(inv, which, results):
    return {"metadata": {"invocation_id": inv}, "args": {"which": which},
            "results": [{"unique_id": u, "status": s} for u, s in results]}


def test_multiple_executing_steps_merge_latest_status_wins():
    steps = [{"index": i} for i in range(1, 6)]
    run_results = {
        "1": _rr("a", "run", [(MODEL, "success")]),
        "2": _rr("b", "test", [(T_PASS, "pass"), (T_WARN_1, "fail")]),
        "3": _rr("b", "test", [(T_PASS, "pass"), (T_WARN_1, "fail")]),  # carried forward: deduped
        "4": _rr("c", "retry", [(T_WARN_1, "warn")]),
        "5": _rr("d", "compile", [(MODEL, "success"), (T_PASS, "success"), (T_WARN_1, "success")]),
    }
    run_results["default"] = run_results["5"]
    client = _StepClient(run_results, steps, load_fixture("dbt_manifest.json"))
    artifacts, _ = _extract(client, {"id": 7, "status": 10})

    assert artifacts["run_results_steps"] == [1, 2, 4]
    assert artifacts["run_results"]["args"]["which"] == "retry"  # latest test-bearing step
    by_uid = {r["unique_id"]: r["status"] for r in artifacts["run_results"]["results"]}
    assert by_uid == {MODEL: "success", T_PASS: "pass", T_WARN_1: "warn"}


def test_falls_back_to_run_step_when_no_build_or_test():
    steps = [{"index": 1}, {"index": 2}]
    run_results = {"1": _rr("a", "run", [(MODEL, "error")]),
                   "2": _rr("b", "generate", [(MODEL, "success")])}
    run_results["default"] = run_results["2"]
    client = _StepClient(run_results, steps, load_fixture("dbt_manifest.json"))
    artifacts, _ = _extract(client, {"id": 7, "status": 10})
    assert artifacts["run_results"]["args"]["which"] == "run"
    assert artifacts["run_results"]["results"][0]["status"] == "error"


def test_generate_only_run_reports_unknown_not_success():
    """Defensive guard: no executing step anywhere -> statuses unknown + error, never success."""
    fx = load_fixture("dbt_multistep_run.json")
    gen = fx["run_results"]["6"]
    client, run = _multistep_client(run_results={"default": gen, "6": gen})
    artifacts, errors = _extract(client, run)
    assert artifacts["run_results"]["args"]["which"] == "generate"  # nothing better existed

    statuses, snap_errors = _test_statuses(artifacts, errors)
    assert set(statuses.values()) == {None}
    guard = [e for e in snap_errors if e["error_type"] == "NonTestRunResults"]
    assert len(guard) == 1 and "UNKNOWN" in guard[0]["error_message"]
    assert guard[0]["run_id"] == run["id"]


def test_normalizer_guard_applies_to_any_input_path():
    """Fixture/local builds bypass the extractor; the normalizer still refuses compile results."""
    gen = load_fixture("dbt_multistep_run.json")["run_results"]["6"]
    snap = DbtNormalizer().normalize({"artifacts": {"manifest": load_fixture("dbt_manifest.json"),
                                                    "run_results": gen}})
    assert all(t["latest_status"] is None for t in snap["tests"])
    assert all(m["latest_status"] is None for m in snap["models"])
    assert any(e["error_type"] == "NonTestRunResults" for e in snap["errors"])


def test_run_results_without_args_kept_as_before():
    legacy = load_fixture("dbt_run_results.json")  # no args.which (older dbt / fixtures)
    snap = DbtNormalizer().normalize({"artifacts": {"manifest": load_fixture("dbt_manifest.json"),
                                                    "run_results": legacy}})
    assert any(t["latest_status"] for t in snap["tests"])
    assert not any(e["error_type"] == "NonTestRunResults" for e in snap["errors"])


def test_run_steps_lookup_failure_is_recorded_and_guarded():
    class NoSteps(_StepClient):
        def get_run(self, *a, **k):
            raise DbtError("boom")

    fx = load_fixture("dbt_multistep_run.json")
    client = NoSteps(fx["run_results"], fx["run_steps"], load_fixture("dbt_manifest.json"))
    artifacts, errors = _extract(client, fx["run"])
    assert any(e.get("resource") == "run_steps" for e in errors)
    _, snap_errors = _test_statuses(artifacts, errors)
    assert any(e["error_type"] == "NonTestRunResults" for e in snap_errors)


def test_fatal_error_during_step_walk_reraises():
    class Revoked(_StepClient):
        def get_run_artifact(self, account_id, run_id, path, step=None):
            if step is not None:
                raise DbtAuthError("revoked")
            return super().get_run_artifact(account_id, run_id, path, step)

    fx = load_fixture("dbt_multistep_run.json")
    client = Revoked(fx["run_results"], fx["run_steps"], load_fixture("dbt_manifest.json"))
    with pytest.raises(DbtAuthError):
        _extract(client, fx["run"])


# -- DbtClient wire format ----------------------------------------------------
def _http_client(handler) -> DbtClient:
    http = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://cloud.getdbt.com/api")
    return DbtClient(Settings(dbt_account_id="1", dbt_service_token="tok"), client=http)


def test_client_sends_step_param_and_include_related():
    seen: list[httpx.URL] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url)
        if request.url.path.endswith("/runs/9/"):
            return httpx.Response(200, json={"data": {"id": 9, "run_steps": [{"index": 1}]}})
        return httpx.Response(200, json={"args": {"which": "build"}, "results": []})

    client = _http_client(handler)
    assert client.get_run_artifact("1", 9, "run_results.json", step=5)["args"]["which"] == "build"
    assert seen[-1].params.get("step") == "5"
    client.get_run_artifact("1", 9, "run_results.json")
    assert "step" not in seen[-1].params
    assert client.get_run("1", 9, include_related=["run_steps"])["run_steps"] == [{"index": 1}]
    assert json.loads(seen[-1].params["include_related"]) == ["run_steps"]
