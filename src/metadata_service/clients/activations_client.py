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

from ._http import parse_retry_after as _parse_retry_after
from ..config import Settings
from ..exceptions import ActivationsAuthError, ActivationsError, ActivationsRateLimitError

logger = logging.getLogger(__name__)

_TIMEOUT = 60.0
_MAX_RETRIES = 3
_PER_PAGE = 100
_MAX_PAGES = 100  # backstop against a pagination loop (10k records)


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

    def _get(self, path: str, params: dict | None = None) -> Any:
        last: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                resp = self._client.get(path, params=params)
            except httpx.HTTPError as exc:
                last = exc
                if attempt < self._max_retries:
                    self._sleep(attempt)
                continue
            if resp.status_code == 401:
                raise ActivationsAuthError("Activations authentication failed (401).")
            if resp.status_code == 429:
                if attempt >= self._max_retries:
                    raise ActivationsRateLimitError("Activations rate limit retries exhausted (429).")
                self._sleep(_parse_retry_after(resp.headers.get("Retry-After"), default=float(attempt)))
                continue
            if resp.status_code >= 500:
                last = ActivationsError(f"Activations {resp.status_code} for {path}.")
                if attempt < self._max_retries:
                    self._sleep(attempt)
                continue
            if resp.status_code >= 400:
                raise ActivationsError(f"Activations {resp.status_code} for {path}: {resp.text[:200]}")
            try:
                return resp.json()
            except ValueError as exc:
                raise ActivationsError(f"Activations returned non-JSON for {path}.") from exc
        raise ActivationsError(f"Activations request failed after {self._max_retries} attempts: {path}") from last

    def _list_paginated(self, path: str) -> list[dict]:
        """Fetch every page of a Census list endpoint.

        Census paginates all list endpoints (default per_page=25) and reports a
        ``pagination`` block alongside ``data``. Fetching only page 1 silently
        truncates the workspace, so we walk ``next_page`` to the end.
        """
        items: list[dict] = []
        page: int | None = 1
        seen_pages: set = set()
        for _ in range(_MAX_PAGES):
            payload = self._get(path, params={"page": page, "per_page": _PER_PAGE})
            if isinstance(payload, list):  # defensive: unwrapped list response
                items.extend(d for d in payload if isinstance(d, dict))
                return items
            data = (payload or {}).get("data") or []
            items.extend(d for d in data if isinstance(d, dict))
            next_page = ((payload or {}).get("pagination") or {}).get("next_page")
            if not next_page or not data:
                return items
            if next_page in seen_pages:  # server echoing a page it already served
                logger.warning("Activations pagination repeated page %s on %s; stopping walk.",
                               next_page, path)
                return items
            seen_pages.add(next_page)
            page = next_page
        # Hit the page cap. Return what we have (not an empty list) so a very large
        # workspace degrades to a partial-but-usable inventory instead of vanishing.
        logger.warning("Activations pagination hit the %s-page cap on %s; returning %s records "
                       "(results may be truncated).", _MAX_PAGES, path, len(items))
        return items

    def list_syncs(self) -> list[dict]:
        """All syncs in the workspace. Census returns the full sync payload
        (source/destination attributes + mappings) in the list response."""
        return self._list_paginated("/syncs")

    def list_sources(self) -> list[dict]:
        return self._list_paginated("/sources")

    def list_destinations(self) -> list[dict]:
        return self._list_paginated("/destinations")


