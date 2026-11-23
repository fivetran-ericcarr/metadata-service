"""Apache Polaris / Iceberg REST catalog reader (plain REST over httpx).

Speaks the Iceberg REST Catalog API, so it serves the Fivetran-managed Polaris
catalog of a Managed Data Lake destination and any other Iceberg REST catalog.
No PyIceberg dependency: only metadata is read (no data files), and PyIceberg
needs a shim for the custom statistics blob type Fivetran writes.

Flow (per build):

1. ``POST {endpoint}/v1/oauth/tokens`` — OAuth2 client credentials
   (``scope=PRINCIPAL_ROLE:ALL`` by default, Polaris' convention).
2. ``GET {endpoint}/v1/config?warehouse={catalog}`` — resolves the URL prefix
   (``overrides.prefix``; falls back to the catalog name).
3. ``GET {endpoint}/v1/{prefix}/namespaces/{ns}/tables/{table}`` per table.

The endpoint, catalog name (== the Fivetran destination id) and client id come
from ``GET /v1/destinations/{id}``; only ``POLARIS_CLIENT_SECRET`` must be set.
"""

from __future__ import annotations

import logging
from urllib.parse import quote

import httpx

from .base import CatalogReadResult
from .iceberg import parse_table_metadata

logger = logging.getLogger(__name__)

#: Iceberg REST multi-level namespace separator (URL-encoded as %1F).
_NS_SEP = "\x1f"


class PolarisAuthError(RuntimeError):
    """Token exchange failed (never includes the secret)."""


class PolarisCatalogReader:
    catalog_type = "polaris"

    def __init__(self, *, endpoint: str, catalog: str, client_id: str, client_secret: str,
                 scope: str = "PRINCIPAL_ROLE:ALL", timeout: float = 30.0,
                 client: httpx.Client | None = None) -> None:
        if not (endpoint and catalog and client_id and client_secret):
            raise ValueError("Polaris reader needs endpoint, catalog, client_id and client_secret.")
        self.endpoint = endpoint.rstrip("/")
        self.catalog = catalog
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope
        self._http = client or httpx.Client(timeout=timeout)
        self._owns_client = client is None
        self._token: str | None = None
        self._prefix: str | None = None

    # -- auth / config --------------------------------------------------
    def _authenticate(self) -> None:
        resp = self._http.post(
            f"{self.endpoint}/v1/oauth/tokens",
            data={"grant_type": "client_credentials", "client_id": self._client_id,
                  "client_secret": self._client_secret, "scope": self._scope},
        )
        if resp.status_code != 200:
            # Status only: the error body can echo request parameters.
            raise PolarisAuthError(f"Polaris token exchange failed (HTTP {resp.status_code}).")
        token = (resp.json() or {}).get("access_token")
        if not token:
            raise PolarisAuthError("Polaris token response carried no access_token.")
        self._token = token

    def _headers(self) -> dict:
        if self._token is None:
            self._authenticate()
        return {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}

    def _resolve_prefix(self) -> str:
        if self._prefix is None:
            resp = self._http.get(f"{self.endpoint}/v1/config",
                                  params={"warehouse": self.catalog}, headers=self._headers())
            resp.raise_for_status()
            overrides = (resp.json() or {}).get("overrides") or {}
            self._prefix = overrides.get("prefix") or self.catalog
        return self._prefix

    # -- reads ----------------------------------------------------------
    def load_table(self, namespace: str, table: str) -> dict | None:
        """Raw LoadTableResult, or None when the table does not exist (404)."""
        prefix = self._resolve_prefix()
        ns = quote(_NS_SEP.join(namespace.split(".")), safe="")
        url = f"{self.endpoint}/v1/{quote(prefix, safe='')}/namespaces/{ns}/tables/{quote(table, safe='')}"
        resp = self._http.get(url, headers=self._headers())
        if resp.status_code == 401:
            # Token expired mid-build: refresh once.
            self._token = None
            resp = self._http.get(url, headers=self._headers())
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def list_namespaces(self) -> list[str]:
        prefix = self._resolve_prefix()
        resp = self._http.get(f"{self.endpoint}/v1/{quote(prefix, safe='')}/namespaces",
                              headers=self._headers())
        resp.raise_for_status()
        return [".".join(ns) for ns in (resp.json() or {}).get("namespaces") or []]

    def read_tables(self, tables: list[tuple[str, str]]) -> CatalogReadResult:
        result = CatalogReadResult()
        # Auth + prefix failures affect every table: let them raise.
        self._resolve_prefix()
        for namespace, table in tables:
            try:
                payload = self.load_table(namespace, table)
            except Exception as exc:  # one table never blocks the others
                result.errors.append({
                    "source": "catalog", "catalog": self.catalog_type,
                    "schema": namespace, "table": table,
                    "error_type": type(exc).__name__,
                    "error_message": _safe_message(exc),
                })
                continue
            if payload is None:
                result.missing.append((namespace, table))
                continue
            result.tables[(namespace.lower(), table.lower())] = parse_table_metadata(
                payload.get("metadata") or {}, catalog=self.catalog_type,
                namespace=namespace, table=table,
                metadata_location=payload.get("metadata-location"),
            )
        return result

    def close(self) -> None:
        if self._owns_client:
            self._http.close()


def _safe_message(exc: Exception) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTP {exc.response.status_code} loading table"
    return str(exc)[:200]
