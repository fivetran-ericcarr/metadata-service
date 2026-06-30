# Use Case: GitHub → Fivetran → Snowflake → dbt → metadata-service

A complete, live end-to-end reference build proving the metadata-service joins
real Fivetran replication metadata with real dbt transformation metadata and
produces actionable Data Quality output.

## Summary

| | |
|---|---|
| **Source** | GitHub repo [`fivetran/dbt_github`](https://github.com/fivetran/dbt_github) |
| **Replication** | Fivetran GitHub connector → Snowflake (`ERICC_TEST_DB.github`) |
| **Transformation** | dbt project [`github-dq-dbt`](https://github.com/fivetran-ericcarr/github-dq-dbt): 7 staging models + DQ tests + source freshness |
| **Metadata join** | `metadata-service build` → 76 warehouse objects, 7 joined to dbt, 223 DQ recommendations, 0 errors |

```text
GitHub (fivetran/dbt_github)
        │  Fivetran GitHub connector (PAT auth)
        ▼
Snowflake  ERICC_TEST_DB.github   ── 73 raw tables
        │  dbt (Snowflake key-pair, deployment job)
        ▼
ERICC_TEST_DB.DBT_ERICC_staging   ── 7 staging models + tests + freshness
        │  metadata-service build (Fivetran API + dbt artifacts)
        ▼
latest.json  ── warehouse_objects joined Fivetran↔dbt, dq_recommendations, drift
```

## Components provisioned

| Layer | Object | How |
|---|---|---|
| Fivetran | GitHub connector `pungent_delegator` in group `quit_paging` | `POST /v1/connectors` (`auth_mode: PersonalAccessToken`, `sync_mode: SpecificRepositories`) |
| Snowflake | destination `ERICC_TEST_DB`, schema `github` | existing Fivetran destination |
| dbt Cloud | Snowflake connection (key-pair), credentials, **Production** deployment env, repo (deploy key), `dbt build` job | dbt Cloud Admin API v3/v2 |
| Git | [`fivetran-ericcarr/github-dq-dbt`](https://github.com/fivetran-ericcarr/github-dq-dbt) | hand-built staging + `schema.yml` tests |

The dbt layer is **hand-built** (not the Fivetran Quickstart package): a `github`
source with freshness, staging models for `repository`, `user`, `issue`,
`pull_request`, `issue_comment`, `label`, `pull_request_review`, and tests —
`not_null`/`unique` on PKs, `relationships` across entities (some `warn`), and
`accepted_values` on the `state` fields.

## Reproduce

```bash
# 0. Configure .env with the Fivetran account + dbt account (3643) credentials.

# 1. Fivetran: create + sync the GitHub connector (PAT auth) — via API
#    config: {schema: github, sync_mode: SpecificRepositories,
#             repositories: ["fivetran/dbt_github"], auth_mode: PersonalAccessToken, pats: [<PAT>]}

# 2. dbt Cloud (account 3643, project ericc_transformations_test):
#    - create a Snowflake connection (key-pair) reading ERICC_TEST_DB
#    - attach the github-dq-dbt repo via deploy key
#    - create a Production deployment environment + a `dbt build` + `dbt source freshness` job
#    - run the job (produces manifest.json / run_results.json / catalog.json / sources.json)

# 3. metadata-service: join Fivetran + dbt and write a snapshot
metadata-service build --group-id quit_paging --dbt-project-id 467825 \
  --connected-only --skip-paused

metadata-service recommendations --schema github --table issue
metadata-service serve-api   # GET /metadata/warehouse-objects?schema=github
```

## Results

```text
warehouse objects: 76 | matched: 7 | unmatched: 69
```

| Object | Match | Models | dbt tests | Freshness |
|---|---|---|---|---|
| issue | exact_schema_table | 1 | 6 | pass |
| issue_comment | exact_schema_table | 1 | 4 | pass |
| label | exact_schema_table | 1 | 3 | pass |
| pull_request | exact_schema_table | 1 | 3 | pass |
| pull_request_review | exact_schema_table | 1 | 5 | pass |
| repository | exact_schema_table | 1 | 5 | pass |
| user | case_insensitive_schema_table | 1 | 3 | pass |

- The match was **deterministic on schema + table** (`github.issue` ↔
  `source.github_dq.github.issue`) — no aliases.
- `user` matched case-insensitively because Snowflake stores the (reserved-word)
  identifier upper-cased while Fivetran reports it lower-cased.
- The 69 unmatched tables are the GitHub tables we chose not to model — they
  surface as `missing_dbt_coverage` risks.
- Recommendations: **157 `dbt_test`** + **66 `missing_dbt_coverage` risks**.

### Spotlight: `github.issue` (joined warehouse object)

```json
{
  "object_id": "warehouse://unknown/github/issue",
  "schema": "github",
  "name": "issue",
  "origin": {
    "system": "fivetran",
    "connection_id": "pungent_delegator",
    "connector_service": "github",
    "source_table": "issue",
    "last_successful_sync": "2026-06-30T11:29:56Z",
    "sync_state": "scheduled"
  },
  "dbt": {
    "source_unique_id": "source.github_dq.github.issue",
    "model_unique_ids": ["model.github_dq.stg_github__issue"],
    "tests": [
      {"test_type": "not_null", "attached_column": "issue_id", "status": "success"},
      {"test_type": "unique", "attached_column": "issue_id", "status": "success"},
      {"test_type": "accepted_values", "attached_column": "state", "status": "success"},
      {"test_type": "not_null", "attached_column": "repository_id", "status": "success"},
      {"test_type": "relationships", "attached_column": "repository_id", "status": "success"},
      {"test_type": "relationships", "attached_column": "user_id", "status": "success", "severity": "warn"}
    ],
    "freshness": {"status": "pass", "max_loaded_at": "2026-06-30T11:25:53Z"}
  },
  "dq_summary": {
    "has_primary_key": false,
    "has_freshness_check": true,
    "failing_tests_count": 0,
    "recommended_tests_count": 2,
    "risk_level": "medium"
  }
}
```

Heuristic recommendations generated for this object (column names drive them):

```json
[
  {"recommendation_type": "dbt_test", "test_name": "accepted_values",
   "target": {"schema": "github", "table": "issue", "column": "state_reason"},
   "reason": "Column name suggests a categorical field.", "confidence": "heuristic"},
  {"recommendation_type": "dbt_test", "test_name": "relationships",
   "target": {"schema": "github", "table": "issue", "column": "milestone_id"},
   "reason": "Column ends with '_id' and may reference another table.", "confidence": "heuristic"}
]
```

## Lessons learned (real findings from this build)

1. **Fivetran's config API does not expose `is_primary_key` for the GitHub
   connector.** PK detection (via `enabled_patch_settings`) found 0 keys across
   585 columns, so `dq_summary.has_primary_key` is `false` for GitHub objects —
   exactly why a hand-built dbt test layer matters here. (See
   [Fivetran metadata notes](#fivetran-metadata).)
2. **The Fivetran Metadata REST API is deprecated** — primary keys / lineage now
   come from the Platform Connector's `fivetran_metadata` schema in the
   destination, not REST.
3. **Snowflake identifier casing & reserved words.** Fivetran stores identifiers
   upper-cased; `user` is reserved, so the dbt source needs `identifier: USER`
   with `quoting: {identifier: true}`.
4. **Empty source tables aren't materialized.** `fivetran/dbt_github` has no
   milestones, so Fivetran never created `github.milestone`; the model was
   dropped.
5. **dbt artifact downloads need `Accept: */*`** — the run-artifact endpoint 406s
   on `application/json`.
6. **Scope dbt extraction by project** (`--dbt-project-id`) — account 3643 has
   240+ projects; an unscoped run grabs unrelated artifacts.

<a name="fivetran-metadata"></a>
See the project root [README](../../README.md) for the full service docs and
[ARTIFACTS.md](../../ARTIFACTS.md) for the JSON contract.
