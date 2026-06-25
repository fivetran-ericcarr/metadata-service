"""Tests for schema drift detection between two snapshots."""

from __future__ import annotations

from metadata_service.dq.drift import detect_drift


def _obj(object_id, columns=None, enabled=True, tests=None, freshness=None):
    return {
        "object_id": object_id,
        "origin": {"enabled": enabled},
        "columns": columns or [],
        "dbt": {"tests": tests or [], "freshness": freshness or {}},
    }


def _col(source_name, name, *, pk=False, hashed=False, enabled=True):
    return {"source_name": source_name, "name": name, "is_primary_key": pk, "hashed": hashed, "enabled": enabled}


def _changes(records):
    return {(r["object_id"], r["change_type"]) for r in records}


def test_no_drift_when_identical():
    doc = {"warehouse_objects": [_obj("warehouse://u/s/a", [_col("Id", "id")])]}
    assert detect_drift(doc, doc) == []


def test_table_added_and_removed():
    prev = {"warehouse_objects": [_obj("warehouse://u/s/b")]}
    latest = {"warehouse_objects": [_obj("warehouse://u/s/c")]}
    changes = _changes(detect_drift(prev, latest))
    assert ("warehouse://u/s/c", "new_table") in changes
    assert ("warehouse://u/s/b", "removed_table") in changes


def test_column_and_key_and_hash_changes():
    prev = {"warehouse_objects": [_obj("warehouse://u/s/a", [
        _col("Id", "id", pk=True, hashed=False),
        _col("Old", "old"),
        _col("Ren", "before"),
    ])]}
    latest = {"warehouse_objects": [_obj("warehouse://u/s/a", [
        _col("Id", "id", pk=False, hashed=True),
        _col("New", "new"),
        _col("Ren", "after"),
    ])]}
    records = detect_drift(prev, latest)
    changes = _changes(records)
    assert ("warehouse://u/s/a", "new_column") in changes
    assert ("warehouse://u/s/a", "removed_column") in changes
    assert ("warehouse://u/s/a", "primary_key_changed") in changes
    assert ("warehouse://u/s/a", "hashing_changed") in changes
    assert ("warehouse://u/s/a", "destination_name_changed") in changes
    # severities pulled from the table
    sev = {r["change_type"]: r["severity"] for r in records}
    assert sev["removed_column"] == "high"
    assert sev["new_column"] == "medium"


def test_disabled_table():
    prev = {"warehouse_objects": [_obj("warehouse://u/s/a", enabled=True)]}
    latest = {"warehouse_objects": [_obj("warehouse://u/s/a", enabled=False)]}
    assert ("warehouse://u/s/a", "disabled_table") in _changes(detect_drift(prev, latest))


def test_test_and_freshness_status_changes():
    prev = {"warehouse_objects": [_obj(
        "warehouse://u/s/a",
        tests=[{"unique_id": "t1", "name": "n1", "status": "pass"}],
        freshness={"status": "pass"},
    )]}
    latest = {"warehouse_objects": [_obj(
        "warehouse://u/s/a",
        tests=[{"unique_id": "t1", "name": "n1", "status": "fail"},
               {"unique_id": "t2", "name": "n2", "status": "pass"}],
        freshness={"status": "warn"},
    )]}
    changes = _changes(detect_drift(prev, latest))
    assert ("warehouse://u/s/a", "dbt_test_added") in changes
    assert ("warehouse://u/s/a", "dbt_test_status_changed") in changes
    assert ("warehouse://u/s/a", "freshness_status_changed") in changes


def test_empty_inputs_return_no_drift():
    assert detect_drift(None, {"warehouse_objects": []}) == []
    assert detect_drift({"warehouse_objects": []}, None) == []
