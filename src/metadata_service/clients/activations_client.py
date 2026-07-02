"""Fivetran Activations (reverse ETL) client — the Census-based API.

Separate surface from ``api.fivetran.com/v1``: base ``https://app.getcensus.com/api/v1``
(or the EU host), auth ``Authorization: Bearer <workspace access token>``. Exposes
sources, destinations, and syncs (with field mappings).
"""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from ..config import Settings
from ..exceptions import ActivationsAuthError, ActivationsError

logger = logging.getLogger(__name__)

_TIMEOUT = 60.0
_MAX_RETRIES = 3


class ActivationsClient:
    def __init__(self, settings: Settings, *, client: httpx.Client | None = None,
                 max_retries: int = _MAX_RETRIES, sleep=time.sleep) -> None:
        self._settings = settings
        self._max_retries = max_retries
        self._sleep = sleep
        if client is not None:
            self._client = client
            self._owns = False
        else:
            if not settings.activations_api_token:
                raise ActivationsError("ACTIVATIONS_API_TOKEN is not set.")
            self._client = httpx.Client(
                base_url=settings.activations_base_url,
                headers={"Authorization": f"Bearer {settings.activations_api_token}",
                         "Accept": "application/json"},
                timeout=_TIMEOUT,
            )
            self._owns = True

    def close(self) -> None:
        if self._owns:
            self._client.close()

    def __enter__(self) -> "ActivationsClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _get(self, path: str) -> Any:
        last: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                resp = self._client.get(path)
            except httpx.HTTPError as exc:
                last = exc
                self._sleep(attempt)
                continue
            if resp.status_code == 401:
                raise ActivationsAuthError("Activations authentication failed (401).")
            if resp.status_code == 429 or resp.status_code >= 500:
                last = ActivationsError(f"Activations {resp.status_code} for {path}.")
                self._sleep(attempt)
                continue
            if resp.status_code >= 400:
                raise ActivationsError(f"Activations {resp.status_code} for {path}: {resp.text[:200]}")
            payload = resp.json()
            # Census wraps list/detail results under "data".
            return payload.get("data", payload) if isinstance(payload, dict) else payload
        raise ActivationsError(f"Activations request failed after {self._max_retries} attempts: {path}") from last

    def list_syncs(self) -> list[dict]:
        data = self._get("/syncs")
        return data if isinstance(data, list) else []

    def get_sync(self, sync_id: int | str) -> dict:
        return self._get(f"/syncs/{sync_id}") or {}

    def list_sources(self) -> list[dict]:
        data = self._get("/sources")
        return data if isinstance(data, list) else []

    def list_destinations(self) -> list[dict]:
        data = self._get("/destinations")
        return data if isinstance(data, list) else []
