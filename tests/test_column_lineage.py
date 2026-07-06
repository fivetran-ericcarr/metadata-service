"""Tests for sqlglot-based column-level lineage."""

from __future__ import annotations

from metadata_service.dq.column_lineage import build_column_lineage, downstream_columns

MANIFEST = {
    "sources": {
        "source.demo.src.issue": {"resource_type": "source", "identifier": "issue", "name": "issue",
                                  "database": "DB", "schema": "RAW"},
    },
    "nodes": {
        "model.demo.stg_issue": {
            "resource_type": "model", "alias": "stg_issue", "name": "stg_issue",
            "database": "DB", "schema": "STG",
            "compiled_code": "select id as issue_id, user_id from DB.RAW.issue",
        },
        "model.demo.mart": {
            "resource_type": "model", "alias": "mart", "name": "mart",
            "database": "DB", "schema": "MARTS",
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


def test_qualified_table_resolves_to_correct_schema_not_first_name_match():
    # Two sources share the table name "orders" in different schemas; a
    # schema-qualified reference must resolve to ITS schema, and an
    # unqualified "orders" is ambiguous and must produce no edge at all.
    manifest = {
        "sources": {
            "source.demo.shopify.orders": {"resource_type": "source", "identifier": "orders",
                                           "name": "orders", "database": "DB", "schema": "SHOPIFY"},
            "source.demo.stripe.orders": {"resource_type": "source", "identifier": "orders",
                                          "name": "orders", "database": "DB", "schema": "STRIPE"},
        },
        "nodes": {
            "model.demo.stg_stripe_orders": {
                "resource_type": "model", "alias": "stg_stripe_orders", "name": "stg_stripe_orders",
                "database": "DB", "schema": "STG",
                "compiled_code": "select amount from DB.STRIPE.orders",
            },
            "model.demo.stg_mystery": {
                "resource_type": "model", "alias": "stg_mystery", "name": "stg_mystery",
                "database": "DB", "schema": "STG",
                "compiled_code": "select amount from orders",
            },
        },
    }
    catalog = {
        "nodes": {
            "model.demo.stg_stripe_orders": {
                "metadata": {"database": "DB", "schema": "STG", "name": "stg_stripe_orders"},
                "columns": {"amount": {"type": "INT"}},
            },
            "model.demo.stg_mystery": {
                "metadata": {"database": "DB", "schema": "STG", "name": "stg_mystery"},
                "columns": {"amount": {"type": "INT"}},
            },
        },
        "sources": {
            "source.demo.shopify.orders": {
                "metadata": {"database": "DB", "schema": "SHOPIFY", "name": "orders"},
                "columns": {"amount": {"type": "INT"}},
            },
            "source.demo.stripe.orders": {
                "metadata": {"database": "DB", "schema": "STRIPE", "name": "orders"},
                "columns": {"amount": {"type": "INT"}},
            },
        },
    }
    edges = build_column_lineage(manifest, catalog)
    froms = {(e["from_unique_id"], e["to_unique_id"]) for e in edges}
    # Qualified ref resolves to stripe, never shopify.
    assert ("source.demo.stripe.orders", "model.demo.stg_stripe_orders") in froms
    assert not any(f == "source.demo.shopify.orders" for f, _ in froms)
    # Ambiguous bare "orders" must not guess.
    assert not any(t == "model.demo.stg_mystery" for _, t in froms)
