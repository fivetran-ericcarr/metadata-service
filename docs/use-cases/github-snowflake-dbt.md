# Use Case: GitHub → Fivetran → Snowflake → dbt → metadata-service

A complete, live end-to-end reference build proving the metadata-service joins
real Fivetran replication metadata with real dbt transformation metadata and
produces actionable Data Quality output.

## Summary

| | |
|---|---|
| **Source** | GitHub repo [`fivetran/dbt_github`](https://github.com/fivetran/dbt_github) |
| **Replication** | Fivetran GitHub connector → Snowflake (`ERICC_TEST_DB.github`) |
| **Transformation** | dbt project [`github-dq-dbt`](https://github.com/fivetran-ericcarr/github-dq-dbt): 7 staging models + 1 mart + DQ tests + source freshness |
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

### Multi-hop lineage: downstream mart

A mart, `github__repository_issue_summary` (one row per repo, aggregating issues
and pull requests from three staging models), gives the join real model→model
lineage. The metadata-service traverses **source → staging → mart**, so each
matched object lists two downstream models:

```text
source.github_dq.github.issue
  → model.github_dq.stg_github__issue            (source → model)
  → model.github_dq.github__repository_issue_summary   (model → model)
```

Rebuilding after the mart landed produced **18 `dbt_test_added` drift records**
(severity `low`) on `issue`, `pull_request`, and `repository` — the mart's tests
now appear in those objects' downstream coverage. (Note: a downstream mart's
tests are attributed to every upstream source object via lineage, so this is a
"downstream coverage" view, not strictly tests defined on that table.)

## Agent hook (MCP)

An agentic DQ application consumes this through the MCP server. The full snapshot
is ~407 KB, so agents triage via compact tools instead of pulling everything.
`get_dq_summary()` over this exact build returns ~0.5 KB:

```json
{
  "object_count": 76, "matched": 7, "unmatched": 69,
  "risk_levels": {"low": 3, "medium": 73, "high": 0},
  "objects_with_failing_tests": 0,
  "objects_missing_dbt_coverage": 69,
  "objects_with_freshness": 7,
  "recommendations": {"total": 223, "by_type": {"dbt_test": 157, "risk": 66},
                       "by_confidence": {"heuristic": 157},
                       "by_risk": {"missing_dbt_coverage": 66}},
  "drift": {"total": 18, "by_severity": {"low": 18}}
}
```

Typical flow: `get_dq_summary()` → `list_warehouse_objects(missing_coverage=true)`
(20 KB for 69 rows vs 194 KB full) → `get_warehouse_object()` /
`get_dq_recommendations()` to act. Served over stdio (local) or HTTP (hosted) —
see the [MCP Usage](../../README.md#7-mcp-usage) section.

## Authoritative primary keys (Platform Connector)

GitHub's config API exposes **no** primary keys (0 across 585 columns). Enabling the
optional Snowflake warehouse reader (`WAREHOUSE_TYPE=snowflake` + `WAREHOUSE_*`) adds
a Fivetran **Platform Connector** to the destination and reads PKs from its
`fivetran_metadata` schema. Re-running the build:

```text
Enriched 75 primary-key columns from fivetran_metadata
```

`github.issue` → PK `id`; composite keys now surface (`branch_commit_relation` =
`branch_name`+`commit_sha`, `commit_file` = `filename`+`commit_sha`), which drove **27
`dbt_utils.unique_combination_of_columns`** recommendations. Enriched columns are
tagged `key_source: "fivetran_platform"`. This is the modern, authoritative PK/lineage
source (the standalone Metadata REST API is deprecated).

## dbt Platform value-adds (Enterprise data trust)

The same build also demonstrates four dbt Platform capabilities that turn
table-level QA into a business-impact-aware data-trust layer — the differentiator
for FinServ / HCLS / MFG buyers and their auditors. All are live on this build.

- **Exposures → blast radius.** The project defines two exposures (a *Repo Health*
  dashboard and an *Issue Triage* ML model). The service attaches them to every
  upstream object's `dbt.exposures`, and `get_impact(github, issue)` returns the
  downstream models **and** those two business consumers. If a DQ problem hits an
  object feeding an exposure, an `impacts_exposure` (high) risk fires.
- **Semantic Layer metric trust.** Three governed metrics (`total_open_issues`,
  `total_issues`, `open_issue_rate`) are resolved to their upstream models (including
  the ratio metric, through its constituent metrics). A `metric_quality` rollup scores
  each metric's trust from its upstream DQ posture — here `watch`, because the inputs
  are tested but still carry open recommendations.
- **Column-level lineage.** dbt's API exposes only node-level lineage, so the service
  parses compiled SQL with sqlglot into **74 column→column edges**.
  `get_column_impact(github, repository, id)` traces `repository.id` →
  `stg_github__repository.repository_id` → `…__repository_issue_summary.repository_id`
  → all three metrics + the dashboard.
- **Model governance.** The mart carries an **enforced contract**, `access: public`,
  a `group`, and an owner. The service surfaces `dbt.governance` per object and flags
  the gaps: **4 `missing_model_contract`** and **4 `unowned_object`** risks on the
  staging-only paths.

## Example agent questions (on the demo data)

How a Capgemini DQ agent answers real questions using the MCP tools over this exact
snapshot (110 objects, 7 modeled, 0 failing tests). The service locates and explains
the risk; for raw row counts the agent reads a failing test's `failures` count or
queries the warehouse.

| Question | Tools used | Answer on the demo data |
|---|---|---|
| **What are the biggest data-quality problems right now?** | `get_dq_summary` | Coverage: **103 of 110 replicated tables have no dbt tests** (unknown quality) — the dominant risk. Then **4 models without an enforced contract**, **4 without an owner**, and **6 columns name-flagged as potential PII**. Zero tests are failing. |
| **How many users signed up with incomplete/invalid info?** | `get_warehouse_object(github, user)` → `dbt.tests` | Completeness *is* asserted: `user_login` has a `not_null` test and the id is `unique` — both **pass**, so no missing logins or duplicate users. (If a rule failed, its `failures` field = the count of bad rows. No `email` test exists yet → recommend one; note `user.email` is also flagged PII.) |
| **If we drop or hash `issue.user_id`, what breaks?** | `get_column_impact(github, issue, user_id)` | It feeds `stg_github__issue.user_id`, which feeds **both** business consumers: the *Repo Health* dashboard and the *Issue Triage* ML model. |
| **Can we trust the `open_issue_rate` metric?** | `get_metric_quality(open_issue_rate)` | **Watch** — its upstream objects (issue, pull_request, repository) are tested with no failures, but still carry open test recommendations, so it isn't fully `trusted` yet. Nothing failing → no hard breakage. |
| **Which sensitive columns are exposed un-hashed?** | `get_dq_recommendations(recommendation_type=signal)` | `commit.author_email`, `commit.committer_email`, `user.email`, `user.phone`, `user_email.email` — review for masking. (Heuristic; `user.email_disabled` is an over-match.) |
| **Is anything feeding the Repo Health dashboard at risk?** | `get_impact` / `list_warehouse_objects(failing_tests=true)` | The dashboard's inputs (issue, pull_request, repository) all pass tests and aren't stale — **healthy**. Any failure would raise `impacts_exposure`. |
| **Which models violate governance?** | `get_dq_recommendations(risk=missing_model_contract)` / `unowned_object` | The four staging-only paths (e.g. `label`, `pull_request_review`) — no enforced contract, no owner. The mart path is contracted, public, and owned. |

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
