"""Tests for FivetranExtractor connection filtering (connected_only / skip_paused)."""

from __future__ import annotations

from metadata_service.extractors import FivetranExtractor


class FakeClient:
    """Minimal stand-in for FivetranClient (no network)."""

    def __init__(self, details: dict[str, dict]) -> None:
        self._details = details

    def list_connections(self, group_id=None):
        return [{"id": cid} for cid in self._details]

    def get_connection(self, connection_id):
        return self._details[connection_id]

    def get_connection_schemas(self, connection_id):
        return {}  # no schemas -> no per-table column calls

    def get_connector_type(self, service):  # pragma: no cover - not used here
        return {}


def _details():
    return {
        "c_active": {"id": "c_active", "service": "salesforce",
                     "status": {"setup_state": "connected", "sync_state": "scheduled"}, "paused": False},
        "c_paused": {"id": "c_paused", "service": "hubspot",
                     "status": {"setup_state": "connected", "sync_state": "paused"}, "paused": True},
        "c_broken": {"id": "c_broken", "service": "stripe",
                     "status": {"setup_state": "broken", "sync_state": "scheduled"}, "paused": False},
    }


def _ids(raw):
    return {c["detail"]["id"] for c in raw["connections"]}


def test_default_includes_all_states():
    raw = FivetranExtractor(FakeClient(_details())).extract(enrich_connector_types=False)
    assert _ids(raw) == {"c_active", "c_paused", "c_broken"}


def test_connected_only_skips_broken_setup():
    raw = FivetranExtractor(FakeClient(_details())).extract(connected_only=True, enrich_connector_types=False)
    assert _ids(raw) == {"c_active", "c_paused"}  # paused is still "connected" setup


def test_skip_paused_excludes_paused_connections():
    raw = FivetranExtractor(FakeClient(_details())).extract(skip_paused=True, enrich_connector_types=False)
    assert _ids(raw) == {"c_active", "c_broken"}


def test_both_filters_combined():
    raw = FivetranExtractor(FakeClient(_details())).extract(
        connected_only=True, skip_paused=True, enrich_connector_types=False
    )
    assert _ids(raw) == {"c_active"}
