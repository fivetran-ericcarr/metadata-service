"""dbt extraction orchestration.

Lists projects/environments/jobs, finds recent successful runs, and downloads
the run artifacts the normalizer needs. Artifacts are the most portable source
of dbt metadata, so they are preferred over the Discovery API.
"""

from __future__ import annotations

import logging

from ..clients.dbt_client import DbtClient
from ..exceptions import (
    DbtArtifactNotFoundError,
    DbtAuthError,
    DbtError,
    DbtPermissionError,
    DbtRateLimitError,
)
from ..models.common import utcnow_iso
from ..normalizers.dbt_normalizer import (
    EXECUTING_COMMANDS,
    REUSED_STATUS,
    TEST_BEARING_COMMANDS,
    run_results_command,
)

logger = logging.getLogger(__name__)

# Errors that cannot heal between calls: retrying every subsequent resource only
# burns quota / hammers a revoked token and yields a silently dbt-empty snapshot.
# Re-raise so the build fails loudly instead of degrading into per-resource errors
# (mirrors FivetranExtractor._FATAL_ERRORS).
_FATAL_ERRORS = (DbtAuthError, DbtPermissionError, DbtRateLimitError)

_ARTIFACT_PATHS = ["manifest.json", "catalog.json", "run_results.json", "sources.json"]
_ARTIFACT_KEYS = {
    "manifest.json": "manifest",
    "catalog.json": "catalog",
    "run_results.json": "run_results",
    "sources.json": "sources",
}


class DbtExtractor:
    def __init__(self, client: DbtClient, account_id: str, *, reused_lookback_runs: int = 10) -> None:
        self._client = client
        self._account_id = account_id
        self._reused_lookback_runs = max(0, int(reused_lookback_runs))

    def extract(
        self,
        *,
        run_limit: int = 50,
        project_id: int | None = None,
        job_id: int | None = None,
    ) -> dict:
        """Extract dbt projects/jobs/runs and download artifacts from recent runs.

        Scoping (important for large/shared accounts):
        - ``project_id``: limit projects/environments/jobs/runs to one project.
        - ``job_id``: pull artifacts from a specific deployment job's recent runs.
        """
        errors: list[dict] = []
        logger.info(
            "dbt extraction starting (account_id=%s, project_id=%s, job_id=%s)",
            self._account_id, project_id, job_id,
        )

        projects = self._safe(lambda: self._client.list_projects(self._account_id), errors, "projects") or []
        if project_id is not None:
            projects = [p for p in projects if p.get("id") == project_id]
        environments = self._safe(
            lambda: self._client.list_environments(self._account_id, project_id=project_id), errors, "environments"
        ) or []
        jobs = self._safe(
            lambda: self._client.list_jobs(self._account_id, project_id=project_id), errors, "jobs"
        ) or []
        runs = self._safe(
            lambda: self._client.list_runs(
                self._account_id, job_id=job_id, project_id=project_id, limit=run_limit
            ),
            errors,
            "runs",
        ) or []

        artifacts = self._download_from_recent_success(runs, errors)

        logger.info(
            "dbt extraction complete: %s projects, %s environments, %s jobs, %s runs, %s artifacts, %s errors",
            len(projects),
            len(environments),
            len(jobs),
            len(runs),
            len(artifacts),
            len(errors),
        )
        return {
            "extracted_at": utcnow_iso(),
            "source": "dbt",
            "projects": projects,
            "environments": environments,
            "jobs": jobs,
            "runs": runs,
            "artifacts": artifacts,
            "errors": errors,
        }

    def _download_from_recent_success(self, runs: list[dict], errors: list[dict]) -> dict:
        """Walk recent runs (newest first); use artifacts from ONE run only.

        Mixing artifacts across runs (manifest from run A, catalog/run_results
        from an older run B) pairs different code versions — node unique_ids stop
        lining up in the normalizer. The first run that yields a manifest anchors
        the whole artifact set; missing optional artifacts are simply absent, not
        backfilled from other runs.
        """
        # dbt status 10 == success; finished_at present indicates a completed run.
        candidates = [r for r in runs if str(r.get("status")) == "10" or r.get("status_humanized") == "Success"]
        fallback = not candidates
        if fallback:
            candidates = [r for r in runs if r.get("finished_at")]

        for run in candidates:
            run_id = run.get("id")
            if run_id is None:
                continue
            artifacts: dict[str, dict] = {}
            for path in _ARTIFACT_PATHS:
                key = _ARTIFACT_KEYS[path]
                try:
                    artifacts[key] = self._client.get_run_artifact(self._account_id, run_id, path)
                    logger.debug("Downloaded %s from run %s", path, run_id)
                except DbtArtifactNotFoundError:
                    # Not every run produces every artifact (e.g. no source freshness).
                    continue
                except _FATAL_ERRORS:
                    raise  # auth/permission/rate-limit won't recover on the next artifact
                except DbtError as exc:
                    errors.append({"source": "dbt", "run_id": run_id, "artifact": path,
                                   "error_type": type(exc).__name__, "error_message": str(exc)})
            if "manifest" in artifacts:
                self._resolve_step_run_results(run, artifacts, errors)
                self._carry_forward_reused(run, runs, artifacts, errors)
                if fallback:
                    errors.append({
                        "source": "dbt", "run_id": run_id,
                        "error_type": "NonSuccessRunArtifacts",
                        "error_message": f"No successful run found; artifacts taken from "
                                         f"non-success run {run_id} "
                                         f"(status={run.get('status_humanized') or run.get('status')}).",
                    })
                artifacts["artifacts_run_id"] = run_id  # provenance for the snapshot
                return artifacts

        errors.append({
            "source": "dbt",
            "error_type": "NoManifest",
            "error_message": "Could not download manifest.json from any recent run.",
        })
        return {}

    def _resolve_step_run_results(self, run: dict, artifacts: dict, errors: list[dict]) -> None:
        """Replace a non-test run_results.json with the run's build/test step results.

        dbt Cloud serves each artifact as of the run's LAST step. A job ending in
        ``dbt docs generate`` (or compile / run-operation / ...) therefore returns
        run_results with a compile ``success`` for every node, hiding the warns and
        failures of the earlier ``dbt build`` — the snapshot would fail OPEN.

        When the default run_results is already from build/test/retry nothing else
        is fetched (the common case costs no extra calls). Otherwise the run's
        steps are walked via ``?step=<index>`` and every executing step
        (build/test/retry/run/seed/snapshot) is merged in step order, the LATEST
        status per unique_id winning — so ``dbt run`` + ``dbt test`` or a ``dbt
        build`` + ``dbt retry`` both resolve to the current state of each node.
        All steps belong to this one run, so the single-run rule above holds;
        manifest/catalog/sources stay the last step's (same code checkout).

        If no executing step exists the default is left in place and the
        normalizer's ``NonTestRunResults`` guard marks statuses unknown.
        """
        default = artifacts.get("run_results")
        resolved, steps = self._step_run_results(run, default, errors)
        if steps is None:
            return
        artifacts["run_results"] = resolved
        artifacts["run_results_steps"] = steps  # provenance
        logger.info("dbt run %s: run_results.json resolved from step(s) %s instead of the "
                    "last step (%s)", run.get("id"), steps, run_results_command(default))

    def _step_run_results(self, run: dict, default: dict | None,
                          errors: list[dict]) -> tuple[dict | None, list[int] | None]:
        """The test-bearing run_results of ``run`` and the steps it was merged from.

        Returns ``(default, None)`` when the default artifact is already
        test-bearing (or legacy without ``args``) or no executing step exists.
        """
        command = run_results_command(default)
        if default is not None and (command is None or command in TEST_BEARING_COMMANDS):
            return default, None  # already test-bearing (or legacy without args: keep as before)

        run_id = run.get("id")
        steps = run.get("run_steps")
        if not steps:
            try:
                steps = (self._client.get_run(self._account_id, run_id,
                                              include_related=["run_steps"]) or {}).get("run_steps")
            except _FATAL_ERRORS:
                raise
            except DbtError as exc:
                errors.append({"source": "dbt", "run_id": run_id, "resource": "run_steps",
                               "error_type": type(exc).__name__, "error_message": str(exc)})
                return default, None
        indices = sorted({s.get("index") for s in steps or [] if isinstance(s.get("index"), int)})

        selected: list[tuple[int, dict]] = []
        seen_invocations: set[str] = set()
        for index in indices:
            try:
                doc = self._client.get_run_artifact(self._account_id, run_id, "run_results.json", step=index)
            except DbtArtifactNotFoundError:
                continue  # step wrote no run_results (clone, deps, freshness, ...)
            except _FATAL_ERRORS:
                raise
            except DbtError as exc:
                errors.append({"source": "dbt", "run_id": run_id, "step": index,
                               "artifact": "run_results.json",
                               "error_type": type(exc).__name__, "error_message": str(exc)})
                continue
            # Later steps re-serve an earlier step's file when they write none of their own.
            invocation = ((doc or {}).get("metadata") or {}).get("invocation_id")
            if invocation:
                if invocation in seen_invocations:
                    continue
                seen_invocations.add(invocation)
            if run_results_command(doc) in EXECUTING_COMMANDS:
                selected.append((index, doc))

        if not selected:
            return default, None

        merged: dict[str, dict] = {}
        for _, doc in selected:
            for result in doc.get("results") or []:
                uid = result.get("unique_id")
                if uid:
                    merged.pop(uid, None)  # re-insert so order follows the latest result
                    merged[uid] = result
        base = next((d for _, d in reversed(selected)
                     if run_results_command(d) in TEST_BEARING_COMMANDS), selected[-1][1])
        return dict(base, results=list(merged.values())), [i for i, _ in selected]

    def _carry_forward_reused(self, anchor: dict, runs: list[dict], artifacts: dict,
                              errors: list[dict]) -> None:
        """Find the last EXECUTED result of every node the anchor run reports ``reused``.

        dbt State (state-aware orchestration, default on dbt Fusion) skips a node
        whose code and upstream data are unchanged and writes ``status: reused``
        ("NO-OP - No new changes on any upstreams") — the last result stands, but
        this run_results does not say what it was. Read alone, a fully-reused run
        has no test evidence at all.

        Earlier runs of the same extraction scope (newest first, any finished
        run — an errored run's executed nodes are still real executions) are
        walked until every reused node has an executed status, or
        ``reused_lookback_runs`` runs were read. A candidate result is only taken
        when its ``compiled_code`` matches the reused one (whitespace-insensitive;
        compared when both sides carry it): a different compiled SQL means the
        result came from a different code version and does not stand for this
        node. Nodes still unresolved stay ``reused`` and the normalizer marks
        them unverified (never passing).

        Result: ``artifacts["reused_last_executed"] = {unique_id: {status,
        failures, message, run_id, generated_at}}`` plus the probed run ids in
        ``artifacts["reused_lookback_run_ids"]``.
        """
        results = (artifacts.get("run_results") or {}).get("results") or []
        if run_results_command(artifacts.get("run_results")) not in (None, *EXECUTING_COMMANDS):
            return  # the normalizer discards non-test run_results anyway
        pending = {r["unique_id"]: r for r in results
                   if r.get("unique_id") and str(r.get("status") or "").lower() == REUSED_STATUS}
        if not pending:
            return

        anchor_id = anchor.get("id")
        older = [r for r in runs or []
                 if r.get("id") is not None and r.get("finished_at") and r.get("id") != anchor_id
                 and (not isinstance(anchor_id, int) or not isinstance(r.get("id"), int)
                      or r["id"] < anchor_id)]
        older.sort(key=lambda r: (str(r.get("finished_at") or ""), r.get("id") or 0), reverse=True)

        carried: dict[str, dict] = {}
        code_changed: list[str] = []
        probed: list = []
        for run in older[: self._reused_lookback_runs]:
            if not pending:
                break
            run_id = run.get("id")
            probed.append(run_id)
            try:
                default = self._client.get_run_artifact(self._account_id, run_id, "run_results.json")
            except DbtArtifactNotFoundError:
                default = None
            except _FATAL_ERRORS:
                raise
            except DbtError as exc:
                errors.append({"source": "dbt", "run_id": run_id, "artifact": "run_results.json",
                               "resource": "reused_lookback",
                               "error_type": type(exc).__name__, "error_message": str(exc)})
                continue
            doc, _ = self._step_run_results(run, default, errors)
            if run_results_command(doc) not in EXECUTING_COMMANDS:
                continue  # docs-only / compile / no run_results: nothing executed here
            generated_at = ((doc or {}).get("metadata") or {}).get("generated_at")
            for result in doc.get("results") or []:
                uid = result.get("unique_id")
                reused = pending.get(uid)
                if reused is None:
                    continue
                status = str(result.get("status") or "").lower()
                if not status or status in (REUSED_STATUS, "skipped"):
                    continue  # not executed in this run either: keep walking back
                if not _same_code(reused.get("compiled_code"), result.get("compiled_code")):
                    code_changed.append(uid)
                    pending.pop(uid)
                    continue
                carried[uid] = {
                    "status": result.get("status"),
                    "failures": result.get("failures"),
                    "message": result.get("message"),
                    "run_id": run_id,
                    "generated_at": generated_at,
                }
                pending.pop(uid)

        artifacts["reused_last_executed"] = carried
        artifacts["reused_lookback_run_ids"] = probed
        unresolved = sorted(pending) + sorted(code_changed)
        if unresolved:
            errors.append({
                "source": "dbt", "run_id": anchor_id,
                "error_type": "ReusedWithoutPriorResult",
                "count": len(unresolved),
                "unique_ids": unresolved[:50],
                "error_message": (
                    f"{len(unresolved)} node(s) were `reused` by dbt State in run {anchor_id} and "
                    f"no matching executed result was found in the {len(probed)} earlier run(s) "
                    f"read ({len(code_changed)} with changed compiled code). Their status is "
                    f"UNVERIFIED (not passing) in this snapshot."),
            })
        logger.info("dbt run %s: %s reused node(s) carried forward from %s earlier run(s); "
                    "%s unresolved", anchor_id, len(carried), len(probed), len(unresolved))

    @staticmethod
    def _safe(fn, errors: list[dict], what: str):
        try:
            return fn()
        except _FATAL_ERRORS:
            raise  # auth/permission/rate-limit is fatal for the whole dbt extraction
        except DbtError as exc:
            logger.warning("dbt %s fetch failed: %s", what, exc)
            errors.append({"source": "dbt", "resource": what, "error_type": type(exc).__name__, "error_message": str(exc)})
            return None


def _same_code(a, b) -> bool:
    """Whitespace-insensitive compiled_code equality; unknown on either side passes."""
    if not a or not b:
        return True
    return " ".join(str(a).split()) == " ".join(str(b).split())
