# Metadata Artifacts

This document describes every JSON artifact the service produces, where each one
is written, and the meaning of its fields. These artifacts are the contract the
agentic Data Quality application consumes.

## Where artifacts live

| Artifact | Producer | Default location |
|---|---|---|
| `fivetran_raw_latest.json` | `metadata-service fivetran extract` | current working directory |
| `dbt_raw_latest.json` | `metadata-service dbt extract` | current working directory |
| `latest.json` | `metadata-service build` / API refresh / MCP refresh | `metadata_snapshots/latest.json` |
| `<timestamp>.json` (history) | same as above | `metadata_snapshots/YYYY/MM/DD/<timestamp>.json` |

The snapshot directory is configurable via `METADATA_LOCAL_PATH` (default
`./metadata_snapshots`). With the `s3` backend the same files are written to
`s3://<bucket>/<prefix>/latest.json` and `s3://<bucket>/<prefix>/YYYY/MM/DD/<timestamp>.json`.

`latest.json` always mirrors the newest timestamped snapshot; the previous
timestamped snapshot is what drift compares against.

> Naming note: throughout the docs "metadata artifacts" refers collectively to
> these JSON files. The on-disk snapshot folder is named `metadata_snapshots`.

---

## 1. Raw extracts (intermediate)

Raw extracts are unnormalized payloads captured straight from the source APIs.
They are useful for debugging and for re-running normalization offline, but the
DQ application should consume the **normalized snapshot** (section 2), not these.

### `fivetran_raw_latest.json`

```json
{
  "extracted_at": "2026-06-25T12:40:00Z",
  "source": "fivetran",
  "connections": [
    {
      "detail": { "...": "raw GET /connections/{id} body" },
      "schemas": { "...": "raw GET /connections/{id}/schemas body" },
      "columns": { "<schema>.<table>": { "...": "raw columns body" } },
      "connector_type": { "...": "raw connector-type body, or null" }
    }
  ],
  "errors": []
}
```

### `dbt_raw_latest.json`

```json
{
  "extracted_at": "2026-06-25T12:40:00Z",
  "source": "dbt",
  "projects": [],
  "environments": [],
  "jobs": [],
  "runs": [],
  "artifacts": {
    "manifest": { "...": "manifest.json" },
    "catalog": { "...": "catalog.json" },
    "run_results": { "...": "run_results.json" },
    "sources": { "...": "sources.json (freshness)" }
  },
  "errors": []
}
```

`errors[]` entries (both extracts) carry enough context to locate the failure
without aborting the run, e.g.:

```json
{ "source": "fivetran", "connection_id": "abc", "schema": "salesforce",
  "table": "Account", "error_type": "FivetranRateLimitError", "error_message": "..." }
```

---

## 2. Normalized snapshot — `latest.json`

The primary artifact. Top-level shape:

```json
{
  "generated_at": "2026-06-25T00:00:00Z",
  "version": "1.0",
  "sources": { "fivetran": { ... }, "dbt": { ... } },
  "warehouse_objects": [ ... ],
  "dq_recommendations": [ ... ],
  "metric_quality": [ ... ],
  "activations": { ... },
  "schema_drift": [ ... ],
  "errors": [ ... ],
  "build_scope": { ... }
}
```

| Field | Type | Meaning |
|---|---|---|
| `generated_at` | ISO-8601 (Z) | When the snapshot was built |
| `version` | string | Snapshot schema version (currently `1.0`) |
| `sources.fivetran` | object | Normalized Fivetran metadata (section 2.1) |
| `sources.dbt` | object | Normalized dbt metadata (section 2.2) |
| `warehouse_objects` | array | Joined Fivetran + dbt objects (section 2.3) |
| `dq_recommendations` | array | DQ recommendations / risks / signals (section 2.4) |
| `metric_quality` | array | Per-metric Semantic Layer trust rollup (`{metric, trust_level, upstream_objects, failing_tests}`) |
| `activations` | object | Reverse-ETL syncs + readiness verdicts (section 2.7) |
| `schema_drift` | array | Drift vs the previous snapshot (section 2.5) |
| `errors` | array | Non-fatal extraction/normalization errors |
| `build_scope` | object | What this build **effectively** covered — `{group_id, include_fivetran, include_dbt, include_activations, connected_only, skip_paused, dbt_project_id, dbt_job_id, fixtures, pk_enrichment}`. Records what actually ran (a source with missing credentials reads `false`), so drift only compares like-for-like builds; a scope change yields a single `comparison_skipped` drift marker rather than mass table churn. |

### 2.1 `sources.fivetran`

```json
{
  "extracted_at": "2026-06-25T12:40:00Z",
  "connections": [
    {
      "connection_id": "conn_sf_001",
      "connector_service": "salesforce",
      "group_id": "group_123",
      "destination_schema": "salesforce",
      "setup_state": "connected",
      "sync_state": "scheduled",
      "last_successful_sync": "2026-06-25T12:34:56Z",
      "schema_change_handling": "ALLOW_ALL",
      "tables": [
        {
          "source_schema": "salesforce",
          "source_table": "Account",
          "destination_schema": "salesforce",
          "destination_table": "account",
          "enabled": true,
          "columns": [
            { "source_name": "Id", "destination_name": "id",
              "enabled": true, "is_primary_key": true, "hashed": false }
          ]
        }
      ]
    }
  ]
}
```

Both **source** and **destination** names are preserved for every table and
column.

### 2.2 `sources.dbt`

```json
{
  "extracted_at": "2026-06-25T12:40:00Z",
  "projects": [], "environments": [], "jobs": [], "runs": [],
  "models": [ { "unique_id": "model.demo.stg_salesforce__account",
                "name": "...", "schema": "...", "materialized": "view",
                "description": "...", "columns": [...], "tags": [...],
                "depends_on": ["source.demo.salesforce.account"],
                "tests": [...], "latest_status": "success",
                "execution_time": 1.23 } ],
  "sources": [ { "unique_id": "source.demo.salesforce.account",
                 "source_name": "salesforce", "table_name": "account",
                 "database": "analytics", "schema": "salesforce",
                 "identifier": "account", "description": "...",
                 "columns": [...], "freshness": { ... },
                 "freshness_result": { "status": "pass",
                   "max_loaded_at": "2026-06-25T12:15:00Z" },
                 "tests": [...] } ],
  "tests": [ { "unique_id": "test.demo.not_null_..._id", "name": "...",
               "test_type": "not_null", "attached_node": "model.demo...",
               "attached_column": "id", "severity": "error",
               "tags": [], "latest_status": "pass", "failures": 0,
               "execution_time": 0.4 } ],
  "lineage_edges": [ { "from_unique_id": "source.demo.salesforce.account",
                       "to_unique_id": "model.demo.stg_salesforce__account",
                       "edge_type": "source->model" } ]
}
```

`edge_type` is one of `source->model`, `model->model`, or `model->exposure`.
Tests are also attached (as compact summaries) to their owning model/source.

### 2.3 `warehouse_objects[]`

Each entry joins one Fivetran destination table with its matched dbt source and
downstream models.

```json
{
  "object_id": "warehouse://unknown/salesforce/account",
  "database": null,
  "schema": "salesforce",
  "name": "account",
  "object_type": "table",
  "origin": {
    "system": "fivetran",
    "connection_id": "conn_sf_001",
    "connector_service": "salesforce",
    "source_schema": "salesforce",
    "source_table": "Account",
    "last_successful_sync": "2026-06-25T12:34:56Z",
    "sync_state": "scheduled",
    "setup_state": "connected",
    "enabled": true
  },
  "dbt": {
    "source_unique_id": "source.demo.salesforce.account",
    "model_unique_ids": ["model.demo.stg_salesforce__account", "model.demo.dim_account"],
    "tests": [ { "unique_id": "...", "name": "...", "test_type": "not_null",
                 "attached_column": "id", "status": "pass", "severity": "error" } ],
    "freshness": { "status": "pass", "max_loaded_at": "2026-06-25T12:15:00Z" }
  },
  "columns": [
    {
      "name": "id",
      "source_name": "Id",
      "enabled": true,
      "is_primary_key": true,
      "key_constraint": "primary_key",
      "hashed": false,
      "dbt_description": "Account id from Salesforce",
      "dbt_tests": ["not_null", "unique"],
      "recommended_tests": []
    }
  ],
  "match_confidence": "exact_schema_table",
  "match_notes": [],
  "dq_summary": {
    "has_primary_key": true,
    "has_primary_key_tests": true,
    "has_freshness_check": true,
    "failing_tests_count": 1,
    "warn_tests_with_failures_count": 0,
    "recommended_tests_count": 1,
    "risk_level": "high"
  }
}
```

| Field | Meaning |
|---|---|
| `object_id` | Stable, warehouse-agnostic id: `warehouse://<db>/<schema>/<table>` (lower-cased; `db` is `unknown` because Fivetran does not expose the destination database) |
| `origin` | Fivetran provenance for the table |
| `dbt.source_unique_id` | Matched dbt source (or `null`) |
| `dbt.model_unique_ids` | Downstream dbt models reached via lineage |
| `dbt.tests` | Tests attached to the matched source + models |
| `dbt.freshness` | Freshness result/config for the matched source (or `null`) |
| `columns[].is_primary_key` | True only for an unambiguous Fivetran primary key (see below) |
| `columns[].key_constraint` | `primary_key`, `primary_or_foreign_key`, or `null` |
| `columns[].key_source` | `fivetran_platform` when the PK came from the warehouse reader, else `null` |
| `columns[].dbt_tests` | dbt test types already present on that column |
| `columns[].recommended_tests` | Test names recommended for that column (see 2.4) |
| `match_confidence` | `exact_schema_table`, `case_insensitive_schema_table`, `configured_alias`, or `unmatched`. (`exact_relation` is reserved for a future database-qualified match tier and is not currently produced.) |
| `dq_summary.failing_tests_count` | Tests with status `fail`/`error`/`runtime error`, **plus** a failing source-freshness check (the dbt run is red or the source is stale past its freshness policy) |
| `dq_summary.warn_tests_with_failures_count` | Warn-severity tests that are **firing** (failing rows while the run stays green) — the reverse-ETL trap; triage-visible, and the activation gate blocks on it |
| `dq_summary.risk_level` | `low`, `medium`, or `high` (high if failing tests or a high-severity risk; medium if warn tests are firing, recommendations exist, PK tests are missing, or unmatched) |

**Primary key detection.** Fivetran's config API does not return an
`is_primary_key` field for most connectors. Key columns are instead locked from
exclusion via `enabled_patch_settings` (`allowed: false`, `reason_code:
"SYSTEM_COLUMN"`) with a reason naming the constraint. The normalizer reads this:

- reason names only a primary key → `is_primary_key: true`, `key_constraint: "primary_key"`
  (e.g. Postgres uses the synthetic `ctid` column as the PK).
- reason names "primary key or a foreign key" (SaaS/SDK connectors) → `is_primary_key: false`,
  `key_constraint: "primary_or_foreign_key"` — these get a `not_null` recommendation
  at `medium` confidence, but not `unique` (it may be a foreign key).

An explicit `is_primary_key` field, when a connector provides one, always wins.

**Authoritative PKs (optional).** When the Snowflake warehouse reader is configured
(`WAREHOUSE_TYPE=snowflake` + `WAREHOUSE_*`), the build overrides PK flags from the
Fivetran Platform Connector's `fivetran_metadata` schema (`SOURCE_COLUMN.is_primary_key`
joined through `COLUMN_LINEAGE` to the destination column). This recovers PKs the
config API omits (e.g. GitHub) and reveals **composite** keys. Such columns are tagged
`key_source: "fivetran_platform"`; `key_source` is `null` otherwise.

### 2.4 `dq_recommendations[]`

Three `recommendation_type` variants. Explicit recommendations (`confidence` of
`high`/`medium`) are kept distinct from `heuristic` ones.

**`dbt_test`** — suggest a dbt test:

```json
{
  "object_id": "warehouse://unknown/salesforce/account",
  "recommendation_type": "dbt_test",
  "test_name": "not_null",
  "target": { "schema": "salesforce", "table": "account", "column": "id" },
  "reason": "Fivetran marks this column as a primary key.",
  "confidence": "high",
  "source": "fivetran_metadata"
}
```

**`risk`** — a data-quality risk:

```json
{
  "object_id": "warehouse://unknown/salesforce/contact",
  "recommendation_type": "risk",
  "risk": "missing_dbt_coverage",
  "severity": "medium",
  "reason": "Table is enabled in Fivetran but no dbt source or model match exists.",
  "target": { "schema": "salesforce", "table": "contact" }
}
```

Risk values: `missing_dbt_coverage` (medium), `failing_dbt_tests` (high),
`stale_fivetran_sync` (high).

**`signal`** — an informational flag:

```json
{
  "object_id": "warehouse://unknown/salesforce/account",
  "recommendation_type": "signal",
  "signal": "hashed_column",
  "target": { "schema": "salesforce", "table": "account", "column": "email" },
  "recommended_action": "Verify downstream models do not expect the raw value."
}
```

| Recommendation | Type | Confidence / Severity |
|---|---|---|
| `not_null`, `unique` on a primary key | `dbt_test` | `high` |
| `dbt_utils.unique_combination_of_columns` (composite PK) | `dbt_test` | `high` |
| `source_freshness` when a matched source lacks freshness | `dbt_test` | `medium` |
| `accepted_values` on a categorical-looking column | `dbt_test` | `heuristic` |
| `accepted_values` `[true,false]` on an `is_`/`has_` column | `dbt_test` | `heuristic` |
| `relationships` on a non-PK `*_id` column | `dbt_test` | `heuristic` |
| `unique` on a natural-key column (`email`, `username`, `slug`, `uuid`, …) | `dbt_test` | `heuristic` |
| `hashed_column` | `signal` | — |
| `potential_pii` (PII-suggestive column name, not hashed) | `signal` | `heuristic` |
| `missing_dbt_coverage` (unmatched, enabled) | `risk` | `medium` |
| `untested_dbt_object` (matched to dbt but no tests) | `risk` | `medium` |
| `failing_dbt_tests` | `risk` | `high` |
| `stale_fivetran_sync` | `risk` | `high` (threshold `STALE_SYNC_THRESHOLD_HOURS`, default 24h) |
| `impacts_exposure` (a DQ problem reaches a dashboard/ML/app) | `risk` | `high` |
| `metric_at_risk` (a DQ problem reaches a governed Semantic Layer metric) | `risk` | `high` |
| `missing_model_contract` (downstream model uncontracted; high if public) | `risk` | `medium`/`high` |
| `unowned_object` (modeled but no owner/group) | `risk` | `medium` |

### dbt Platform fields (from the manifest + Discovery API)

Beyond models/sources/tests, each `warehouse_objects[].dbt` may carry:

- `exposures[]` — dashboards/ML/apps it feeds (blast radius). Doc-level
  `sources.dbt.exposures` lists all.
- `metrics[]` — Semantic Layer metrics it feeds. Doc-level `metric_quality[]` gives a
  per-metric `trust_level` (`trusted`|`watch`|`at_risk`|`unknown`) from upstream DQ
  posture; `unknown` means the metric has no matched upstream warehouse object to
  judge it on. `failing_tests` counts distinct failing tests across the upstream.
- `governance` — `{has_enforced_contract, owners, groups, access_levels,
  uncontracted_public_models}`.
- `sources.dbt.column_lineage_edges` — `{from_unique_id, from_column, to_unique_id,
  to_column}` column→column edges (sqlglot), powering `get_column_impact`.

### 2.5 `schema_drift[]`

Differences between this snapshot and the previous one.

```json
{
  "detected_at": "2026-06-25T00:00:00Z",
  "object_id": "warehouse://unknown/salesforce/account",
  "change_type": "new_column",
  "severity": "medium",
  "details": { "column": "new_field" }
}
```

| `change_type` | Severity |
|---|---|
| `new_table` | low |
| `removed_table` | high |
| `disabled_table` | high |
| `new_column` | medium |
| `removed_column` | high |
| `disabled_column` | high |
| `primary_key_changed` | high |
| `hashing_changed` | high |
| `destination_name_changed` | high |
| `dbt_test_added` | low |
| `dbt_test_removed` | high |
| `dbt_test_status_changed` | high when the status changed *into* a bad state (→ fail/error/warn); `low` for a recovery (fail → pass) |
| `freshness_status_changed` | high when the status changed *into* a bad state; `low` for a recovery |

A `comparison_skipped` record (severity `info`) is emitted instead of the above
when the two snapshots' `build_scope` differs — drift was not evaluated, as
opposed to evaluated-and-clean.

### 2.6 `errors[]`

Non-fatal errors aggregated from both extractors and from defensive
normalization. Each entry includes at least `error_type` and `error_message`,
plus whatever context is available (`source`, `connection_id`, `schema`,
`table`, `run_id`, `artifact`, `unique_id`). The pipeline never silently
swallows errors and never logs credentials.

### 2.7 `activations`

Fivetran Activations (reverse ETL) syncs and their **readiness verdict** — the
"is it safe to push this data back to a system of record?" gate. Populated when
`ACTIVATIONS_API_TOKEN` is set (scoped to `WAREHOUSE_DATABASE`); otherwise
`{ "syncs": [], "summary": {"total": 0, "by_verdict": {}} }`.

```json
{
  "extracted_at": "2026-06-30T00:00:00Z",
  "summary": { "total": 1, "by_verdict": { "block": 1 } },
  "syncs": [
    {
      "sync_id": 3580269,
      "label": "customer_churn -> Salesforce Contact (DQ demo)",
      "paused": true,
      "source_object": { "table_catalog": "ERICC_TEST_DB", "table_schema": "DBT_ERICC_MARTS", "table_name": "CUSTOMER_CHURN" },
      "destination_name": "sf_dq_activations_ericc",
      "destination_type": "salesforce",
      "destination_object": "Contact",
      "mappings": [ { "source_column": "EMAIL", "destination_field": "Email", "is_primary_identifier": true } ],
      "readiness": {
        "verdict": "block",
        "source_node_unique_id": "model.github_dq.customer_churn",
        "reasons": [ { "code": "upstream_warn_test_failures", "severity": "high", "message": "…" } ],
        "upstream": { "node_count": 9, "failing_tests": 0, "warn_tests_with_failures": 1, "tests_seen": 18, "tests_with_results": 18, "stale_objects": 0, "missing_contract": false, "unmatched_upstream": 0 }
      }
    }
  ]
}
```

| `readiness.verdict` | Meaning |
|---|---|
| `allow` | Positive evidence only: upstream tests exist, ran, and passed; no stale syncs or governance gaps. |
| `warn` | Source model has no enforced contract, an upstream source is unmatched, **or the gate lacks evidence** — no upstream tests exist (`no_upstream_tests`), tests have no run results (`no_test_results`), or Fivetran coverage data is absent from the build (`coverage_checks_skipped`). Absence of evidence never yields `allow`. |
| `block` | Upstream failing test, a **warn-severity test with failures > 0** (or warn status with the count missing), failing source freshness, or a stale upstream Fivetran sync — do not push. |
| `unknown` | Sync source object is not matched to a dbt model/source (no lineage to assess). Also raises an `activates_unverified_data` (medium) risk in `dq_recommendations` when the source object is named. |

`readiness.upstream` also reports the evidence base: `tests_seen` (tests defined
upstream) and `tests_with_results` (those with a run result).

Each warehouse object the sync consumes also carries a compact
`warehouse_objects[].activations[]` back-reference (`sync_id`, `label`,
`destination_name`, `destination_type`, `destination_object`, `paused`,
`readiness_verdict`), and objects
feeding a `block`/`warn` sync raise an `activates_bad_data` risk in
`dq_recommendations`; an `unknown`-verdict sync with a named source object raises
`activates_unverified_data` (medium) — data pushed to prod with no quality
evidence at all. If the workspace has more matching syncs than `max_syncs`, the
overflow is recorded in `errors[]` as `Truncated` (never silently dropped).

---

## Consuming the artifacts

- **File**: read `metadata_snapshots/latest.json` directly.
- **REST**: `GET /metadata/latest`, `GET /metadata/warehouse-objects`,
  `GET /dq/recommendations`, `GET /dq/drift`, `GET /metadata/activations`,
  `GET /dq/activation-readiness`.
- **MCP**: `get_latest_metadata`, `get_warehouse_object`,
  `get_dq_recommendations`, `get_schema_drift`, `list_activations`,
  `get_activation_readiness`.

The full Pydantic contract lives in
[`models/normalized.py`](src/metadata_service/models/normalized.py).
