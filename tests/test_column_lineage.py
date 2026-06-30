"""Tests for sqlglot-based column-level lineage."""

from __future__ import annotations

from metadata_service.dq.column_lineage import build_column_lineage, downstream_columns

MANIFEST = {
    "sources": {
        "source.demo.src.issue": {"resource_type": "source", "identifier": "issue", "name": "issue"},
    },
    "nodes": {
        "model.demo.stg_issue": {
            "resource_type": "model", "alias": "stg_issue", "name": "stg_issue",
            "compiled_code": "select id as issue_id, user_id from DB.RAW.issue",
        },
        "model.demo.mart": {
            "resource_type": "model", "alias": "mart", "name": "mart",
            "compiled_code": "select issue_id from DB.STG.stg_issue",
        },
    },
}

CATALOG = {
    "nodes": {
        "model.demo.stg_issue": {
            "metadata": {"database": "DB", "schema": "STG", "name": "stg_issue"},
            "columns": {"issue_id": {"type": "INT"}, "user_id": {"type": "INT"}},
        },
        "model.demo.mart": {
            "metadata": {"database": "DB", "schema": "MARTS", "name": "mart"},
            "columns": {"issue_id": {"type": "INT"}},
        },
    },
    "sources": {
        "source.demo.src.issue": {
            "metadata": {"database": "DB", "schema": "RAW", "name": "issue"},
            "columns": {"id": {"type": "INT"}, "user_id": {"type": "INT"}},
        },
    },
}


def test_build_column_lineage_edges():
    edges = build_column_lineage(MANIFEST, CATALOG)
    pairs = {(e["from_unique_id"], e["from_column"], e["to_unique_id"], e["to_column"]) for e in edges}
    # source.id -> stg.issue_id
    assert ("source.demo.src.issue", "id", "model.demo.stg_issue", "issue_id") in pairs
    # stg.issue_id -> mart.issue_id
    assert ("model.demo.stg_issue", "issue_id", "model.demo.mart", "issue_id") in pairs


def test_downstream_columns_transitive():
    edges = build_column_lineage(MANIFEST, CATALOG)
    impact = downstream_columns(edges, "source.demo.src.issue", "id")
    targets = {(d["unique_id"], d["column"]) for d in impact}
    assert ("model.demo.stg_issue", "issue_id") in targets
    assert ("model.demo.mart", "issue_id") in targets  # transitive source -> stg -> mart


def test_unrelated_column_has_no_mart_impact():
    edges = build_column_lineage(MANIFEST, CATALOG)
    impact = downstream_columns(edges, "source.demo.src.issue", "user_id")
    # user_id reaches stg.user_id but not the mart (mart only selects issue_id)
    targets = {(d["unique_id"], d["column"]) for d in impact}
    assert ("model.demo.stg_issue", "user_id") in targets
    assert not any(uid == "model.demo.mart" for uid, _ in targets)
