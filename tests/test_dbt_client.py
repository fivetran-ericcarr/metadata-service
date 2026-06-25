"""Tests for DbtClient HTTP behavior (artifact Accept header, pagination)."""

from __future__ import annotations

import httpx

from metadata_service.clients.dbt_client import DbtClient
from metadata_service.config import Settings


def _client(handler) -> DbtClient:
    settings = Settings(dbt_account_id="1", dbt_service_token="tok")
    transport = httpx.MockTransport(handler)
    http = httpx.Client(transport=transport, base_url="https://cloud.getdbt.com/api",
                        headers=DbtClient._auth_headers("tok"))
    return DbtClient(settings, client=http)


def test_get_run_artifact_overrides_accept_header():
    """Regression: artifact endpoint 406s on Accept: application/json; must send */*."""
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["accept"] = request.headers.get("accept")
        if "application/json" in (request.headers.get("accept") or ""):
            return httpx.Response(406, text="406 Not Acceptable")
        return httpx.Response(200, json={"nodes": {}, "sources": {}})

    client = _client(handler)
    artifact = client.get_run_artifact("1", 123, "manifest.json")
    assert artifact == {"nodes": {}, "sources": {}}
    assert seen["accept"] == "*/*"


def test_list_projects_paginates():
    """Admin API caps pages at 100; client must follow offset to total_count."""
    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params.get("offset", "0"))
        total = 150
        page = [{"id": i} for i in range(offset, min(offset + 100, total))]
        return httpx.Response(200, json={"data": page, "extra": {"pagination": {"total_count": total}}})

    client = _client(handler)
    projects = client.list_projects("1")
    assert len(projects) == 150
    assert projects[0]["id"] == 0 and projects[-1]["id"] == 149
