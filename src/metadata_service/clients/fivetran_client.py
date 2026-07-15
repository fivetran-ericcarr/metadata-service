"""Fivetran REST API client.

Auth: HTTP Basic (API key / API secret).
Header: ``Accept: application/json;version=2``.
Handles cursor pagination, 429 + Retry-After, and transient 5xx retries.
Raises typed exceptions and never logs credentials.
"""

from __future__ import annotations

import logging
import time
from typing import Any
from urllib.parse import quote

import httpx

from ..config import Settings
from ..exceptions import (
    FivetranAuthError,
    FivetranError,
    FivetranNotFoundError,
    FivetranPermissionError,
    FivetranRateLimitError,
)

logger = logging.getLogger(__name__)

ACCEPT_HEADER = "application/json;version=2"
_DEFAULT_TIMEOUT = 30.0
_MAX_RETRIES = 3
_BACKOFF_SECONDS = 1.0


class FivetranClient:
    """Thin, typed wrapper over the Fivetran v1 REST API."""

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
            settings.require_fivetran()
            self._client = httpx.Client(
                base_url=settings.fivetran_base_url,
                auth=(settings.fivetran_api_key or "", settings.fivetran_api_secret or ""),
                headers={"Accept": ACCEPT_HEADER},
                timeout=_DEFAULT_TIMEOUT,
            )
            self._owns_client = True

    # -- lifecycle --------------------------------------------------------
    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "FivetranClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- low level --------------------------------------------------------
    def _request(self, method: str, path: str, params: dict | None = None) -> dict:
        """Perform a request with retry/backoff and typed error mapping."""
        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                resp = self._client.request(method, path, params=params)
            except httpx.HTTPError as exc:  # network / timeout
                last_exc = exc
                logger.warning("Fivetran request error (%s %s) attempt %s: %s", method, path, attempt, exc)
                if attempt < self._max_retries:
                    self._sleep(_BACKOFF_SECONDS * attempt)
                continue

            status = resp.status_code
            if status == 401:
                raise FivetranAuthError("Fivetran authentication failed (401).")
            if status == 403:
                raise FivetranPermissionError(f"Fivetran permission denied (403) for {path}.")
            if status == 404:
                raise FivetranNotFoundError(f"Fivetran resource not found (404): {path}.")
            if status == 429:
                retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
                if attempt >= self._max_retries:
                    raise FivetranRateLimitError("Fivetran rate limit retries exhausted (429).")
                logger.warning("Fivetran rate limited (429); sleeping %.1fs", retry_after)
                self._sleep(retry_after)
                continue
            if status >= 500:
                last_exc = FivetranError(f"Fivetran server error {status} for {path}.")
                logger.warning("Fivetran %s on %s attempt %s; retrying", status, path, attempt)
                if attempt < self._max_retries:
                    self._sleep(_BACKOFF_SECONDS * attempt)
                continue
            if status >= 400:
                raise FivetranError(f"Unexpected Fivetran response {status} for {path}: {resp.text[:300]}")

            try:
                return resp.json()
            except ValueError as exc:
                raise FivetranError(f"Fivetran returned non-JSON response for {path}.") from exc

        raise FivetranError(f"Fivetran request failed after {self._max_retries} attempts: {path}") from last_exc

    def _get_data(self, path: str, params: dict | None = None) -> Any:
        """GET a path and return the ``data`` envelope contents."""
        payload = self._request("GET", path, params=params)
        return payload.get("data", payload)

    def _get_paginated(self, path: str, params: dict | None = None,
                       max_pages: int = 1000) -> list[dict]:
        """GET a cursor-paginated collection, returning all ``items``.

        Guards against pagination bugs: a cursor that repeats (would loop and
        duplicate items forever) or more than ``max_pages`` pages stops the walk
        with a warning rather than looping to OOM.
        """
        items: list[dict] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        params = dict(params or {})
        for _ in range(max_pages):
            page_params = dict(params)
            if cursor:
                page_params["cursor"] = cursor
            data = self._get_data(path, params=page_params)
            if not isinstance(data, dict):  # pragma: no cover - defensive
                return items
            # ``items`` present-but-null (extend(None) -> TypeError) is a real
            # Fivetran response for empty collections; coerce like the other clients.
            items.extend(data.get("items") or [])
            cursor = data.get("next_cursor")
            if not cursor:
                return items
            if cursor in seen_cursors:
                logger.warning("Fivetran pagination repeated cursor on %s; stopping walk.", path)
                return items
            seen_cursors.add(cursor)
        logger.warning("Fivetran pagination exceeded %s pages on %s; results may be truncated.",
                       max_pages, path)
        return items

    # -- public API -------------------------------------------------------
    def list_connections(self, group_id: str | None = None) -> list[dict]:
        params = {"group_id": group_id} if group_id else None
        connections = self._get_paginated("/connections", params=params)
        if group_id:
            connections = [c for c in connections if c.get("group_id") == group_id]
        return connections

    def get_connection(self, connection_id: str) -> dict:
        return self._get_data(f"/connections/{quote(connection_id, safe='')}")

    def get_connection_schemas(self, connection_id: str) -> dict:
        return self._get_data(f"/connections/{quote(connection_id, safe='')}/schemas")

    def get_table_columns(self, connection_id: str, schema_name: str, table_name: str) -> dict:
        path = (
            f"/connections/{quote(connection_id, safe='')}"
            f"/schemas/{quote(schema_name, safe='')}"
            f"/tables/{quote(table_name, safe='')}/columns"
        )
        return self._get_data(path)

    def get_connector_types(self) -> list[dict]:
        return self._get_paginated("/metadata/connector-types")

    def get_connector_type(self, service: str) -> dict:
        return self._get_data(f"/metadata/connector-types/{quote(service, safe='')}")


def _parse_retry_after(value: str | None, default: float = 2.0, max_seconds: float = 60.0) -> float:
    """Numeric Retry-After, clamped so one header can't stall a run for hours.
    HTTP-date values fall back to the default."""
    if not value:
        return default
    try:
        return max(0.0, min(float(value), max_seconds))
    except (TypeError, ValueError):
        return default
