"""dbt Cloud / dbt Platform API client.

Covers the Admin API v2 (projects, environments, jobs, runs, artifacts) and an
optional Discovery/Metadata GraphQL layer when ``DBT_METADATA_API_URL`` is set.

Auth is encapsulated in ``_auth_headers`` so the scheme can change in one place.
Current dbt Cloud service tokens use ``Authorization: Token <token>``.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from ..config import Settings
from ..exceptions import (
    DbtArtifactNotFoundError,
    DbtAuthError,
    DbtError,
    DbtPermissionError,
    DbtRateLimitError,
)

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 60.0
_MAX_RETRIES = 3
_BACKOFF_SECONDS = 1.0


class DbtClient:
    """Wrapper over the dbt Cloud Admin API (v2) and optional Discovery API."""

    def __init__(
        self,
        settings: Settings,
        *,
        client: httpx.Client | None = None,
        max_retries: int = _MAX_RETRIES,
        sleep=time.sleep,
    ) -> None:
        self._settings = settings
        self._max_retries = max_retries
        self._sleep = sleep
        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            settings.require_dbt()
            self._client = httpx.Client(
                base_url=settings.dbt_base_url,
                headers=self._auth_headers(settings.dbt_service_token or ""),
                timeout=_DEFAULT_TIMEOUT,
            )
            self._owns_client = True

    @staticmethod
    def _auth_headers(token: str) -> dict[str, str]:
        """Single place that defines the dbt Cloud auth scheme."""
        return {"Authorization": f"Token {token}", "Accept": "application/json"}

    # -- lifecycle --------------------------------------------------------
    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "DbtClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- low level --------------------------------------------------------
    def _request(
        self, method: str, path: str, params: dict | None = None, headers: dict | None = None
    ) -> httpx.Response:
        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                resp = self._client.request(method, path, params=params, headers=headers)
            except httpx.HTTPError as exc:
                last_exc = exc
                logger.warning("dbt request error (%s %s) attempt %s: %s", method, path, attempt, exc)
                self._sleep(_BACKOFF_SECONDS * attempt)
                continue

            status = resp.status_code
            if status == 401:
                raise DbtAuthError("dbt authentication failed (401).")
            if status == 403:
                raise DbtPermissionError(f"dbt permission denied (403) for {path}.")
            if status == 404:
                raise DbtArtifactNotFoundError(f"dbt resource not found (404): {path}.")
            if status == 429:
                if attempt >= self._max_retries:
                    raise DbtRateLimitError("dbt rate limit retries exhausted (429).")
                self._sleep(_BACKOFF_SECONDS * attempt * 2)
                continue
            if status >= 500:
                last_exc = DbtError(f"dbt server error {status} for {path}.")
                self._sleep(_BACKOFF_SECONDS * attempt)
                continue
            if status >= 400:
                raise DbtError(f"Unexpected dbt response {status} for {path}: {resp.text[:300]}")
            return resp

        raise DbtError(f"dbt request failed after {self._max_retries} attempts: {path}") from last_exc

    def _get_json(self, path: str, params: dict | None = None) -> Any:
        resp = self._request("GET", path, params=params)
        try:
            return resp.json()
        except ValueError as exc:
            raise DbtError(f"dbt returned non-JSON response for {path}.") from exc

    @staticmethod
    def _data(payload: Any) -> Any:
        if isinstance(payload, dict) and "data" in payload:
            return payload["data"]
        return payload

    def _paginate(self, path: str, params: dict | None = None, page_size: int = 100,
                  cap: int = 5000) -> list[dict]:
        """Page through a dbt Admin API list endpoint via limit/offset.

        The Admin API caps page size at 100 and reports ``extra.pagination.total_count``.
        """
        params = dict(params or {})
        params["limit"] = page_size
        offset = 0
        out: list[dict] = []
        while True:
            params["offset"] = offset
            payload = self._get_json(path, params=params)
            data = self._data(payload) or []
            out.extend(data)
            offset += len(data)
            total = None
            if isinstance(payload, dict):
                total = ((payload.get("extra") or {}).get("pagination") or {}).get("total_count")
            if not data or len(data) < page_size or (total is not None and offset >= total) or offset >= cap:
                break
        return out

    # -- Admin API v2 -----------------------------------------------------
    def list_projects(self, account_id: str) -> list[dict]:
        return self._paginate(f"/v2/accounts/{account_id}/projects/")

    def list_environments(self, account_id: str, project_id: int | None = None) -> list[dict]:
        params = {"project_id": project_id} if project_id else None
        return self._paginate(f"/v2/accounts/{account_id}/environments/", params=params)

    def list_jobs(
        self,
        account_id: str,
        project_id: int | None = None,
        environment_id: int | None = None,
    ) -> list[dict]:
        params: dict[str, Any] = {}
        if project_id:
            params["project_id"] = project_id
        if environment_id:
            params["environment_id"] = environment_id
        return self._paginate(f"/v2/accounts/{account_id}/jobs/", params=params or None)

    def list_runs(
        self,
        account_id: str,
        job_id: int | None = None,
        project_id: int | None = None,
        limit: int = 100,
    ) -> list[dict]:
        params: dict[str, Any] = {"limit": limit, "order_by": "-finished_at"}
        if job_id:
            params["job_definition_id"] = job_id
        if project_id:
            params["project_id"] = project_id
        return self._data(self._get_json(f"/v2/accounts/{account_id}/runs/", params=params)) or []

    def get_run(self, account_id: str, run_id: int) -> dict:
        return self._data(self._get_json(f"/v2/accounts/{account_id}/runs/{run_id}/")) or {}

    def get_run_artifact(self, account_id: str, run_id: int, path: str) -> dict:
        """Download a single run artifact (manifest.json, catalog.json, ...).

        Artifacts are returned as raw JSON (not wrapped in a ``data`` envelope).
        The artifact endpoint rejects ``Accept: application/json`` with a 406, so we
        override the Accept header to ``*/*`` for this request only.
        """
        resp = self._request(
            "GET",
            f"/v2/accounts/{account_id}/runs/{run_id}/artifacts/{path}",
            headers={"Accept": "*/*"},
        )
        try:
            return resp.json()
        except ValueError as exc:
            raise DbtArtifactNotFoundError(
                f"dbt artifact {path} for run {run_id} was not valid JSON."
            ) from exc

    # -- Discovery / Metadata GraphQL (optional) --------------------------
    def query_discovery(self, query: str, variables: dict | None = None) -> dict:
        """Run a GraphQL query against the Discovery/Metadata API.

        Only available when ``DBT_METADATA_API_URL`` is configured. Uses the same
        service token via Bearer auth (Discovery API convention).
        """
        url = self._settings.dbt_metadata_api_url
        if not url:
            raise DbtError("Discovery API not configured (DBT_METADATA_API_URL is unset).")
        headers = {
            "Authorization": f"Bearer {self._settings.dbt_service_token}",
            "Content-Type": "application/json",
        }
        with httpx.Client(timeout=_DEFAULT_TIMEOUT) as gql:
            resp = gql.post(url, json={"query": query, "variables": variables or {}}, headers=headers)
        if resp.status_code == 401:
            raise DbtAuthError("dbt Discovery API authentication failed (401).")
        if resp.status_code >= 400:
            raise DbtError(f"dbt Discovery API error {resp.status_code}: {resp.text[:300]}")
        return resp.json()
