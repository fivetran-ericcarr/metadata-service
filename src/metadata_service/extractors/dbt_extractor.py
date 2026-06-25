"""dbt extraction orchestration.

Lists projects/environments/jobs, finds recent successful runs, and downloads
the run artifacts the normalizer needs. Artifacts are the most portable source
of dbt metadata, so they are preferred over the Discovery API.
"""

from __future__ import annotations

import logging

from ..clients.dbt_client import DbtClient
from ..exceptions import DbtArtifactNotFoundError, DbtError
from ..models.common import utcnow_iso

logger = logging.getLogger(__name__)

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
        """Walk recent runs (newest first) collecting artifacts until all found."""
        artifacts: dict[str, dict] = {}
        # dbt status 10 == success; finished_at present indicates a completed run.
        candidates = [r for r in runs if str(r.get("status")) == "10" or r.get("status_humanized") == "Success"]
        if not candidates:
            candidates = [r for r in runs if r.get("finished_at")]

        for run in candidates:
            run_id = run.get("id")
            if run_id is None:
                continue
            for path in _ARTIFACT_PATHS:
                key = _ARTIFACT_KEYS[path]
                if key in artifacts:
                    continue
                try:
                    artifacts[key] = self._client.get_run_artifact(self._account_id, run_id, path)
                    logger.debug("Downloaded %s from run %s", path, run_id)
                except DbtArtifactNotFoundError:
                    # Not every run produces every artifact (e.g. no source freshness).
                    continue
                except DbtError as exc:
                    errors.append({"source": "dbt", "run_id": run_id, "artifact": path,
                                   "error_type": type(exc).__name__, "error_message": str(exc)})
            if all(k in artifacts for k in ("manifest", "run_results")):
                break

        if "manifest" not in artifacts:
            errors.append({
                "source": "dbt",
                "error_type": "NoManifest",
                "error_message": "Could not download manifest.json from any recent run.",
            })
        return artifacts

    @staticmethod
    def _safe(fn, errors: list[dict], what: str):
        try:
            return fn()
        except DbtError as exc:
            logger.warning("dbt %s fetch failed: %s", what, exc)
            errors.append({"source": "dbt", "resource": what, "error_type": type(exc).__name__, "error_message": str(exc)})
            return None
