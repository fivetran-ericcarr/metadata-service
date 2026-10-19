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
    def __init__(self, client: DbtClient, account_id: str) -> None:
        self._client = client
        self._account_id = account_id

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
        command = run_results_command(default)
        if default is not None and (command is None or command in TEST_BEARING_COMMANDS):
            return  # already test-bearing (or legacy without args: keep as before)

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
                return
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
            return

        merged: dict[str, dict] = {}
        for _, doc in selected:
            for result in doc.get("results") or []:
                uid = result.get("unique_id")
                if uid:
                    merged.pop(uid, None)  # re-insert so order follows the latest result
                    merged[uid] = result
        base = next((d for _, d in reversed(selected)
                     if run_results_command(d) in TEST_BEARING_COMMANDS), selected[-1][1])
        artifacts["run_results"] = dict(base, results=list(merged.values()))
        artifacts["run_results_steps"] = [i for i, _ in selected]  # provenance
        logger.info("dbt run %s: run_results.json resolved from step(s) %s (%s) instead of the "
                    "last step (%s)", run_id, artifacts["run_results_steps"],
                    ", ".join(run_results_command(d) for _, d in selected), command)

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
