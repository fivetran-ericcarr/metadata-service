# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `CHANGELOG.md` to track changes as they are made.
- `ARTIFACTS.md` documenting every JSON artifact the service produces.
- `docs/use-cases/github-snowflake-dbt.md` — a live end-to-end reference build
  (GitHub → Fivetran → Snowflake → dbt → metadata-service) with real results.
- `.gitignore` now excludes private keys (`*.pem`, `*.p8`, `*.key`, `rsa_key*`).
- MCP agent-triage tools: `get_dq_summary` (account rollup) and
  `list_warehouse_objects` (compact, filterable index). `get_dq_recommendations`
  gained cross-snapshot filtering (`recommendation_type`/`confidence`/`risk`/`limit`)
  and per-object args are now optional. Lets agents orient and triage without
  pulling the full ~400 KB snapshot.
- MCP HTTP transport: `serve-mcp --transport http|sse --host --port` (plus
  `MCP_TRANSPORT`/`MCP_HOST`/`MCP_PORT`) for hosted/remote agents, alongside stdio.
- `docs/capgemini-quickstart.md` — solution guide (what it does, agent triage flow,
  where it fits in DQaaS, 5-minute quickstart) with architecture + sequence diagrams
  and `docs/assets/architecture.svg`.
- `examples/` — runnable `agent_quickstart.py` (MCP triage flow, stdio + http),
  `rest_quickstart.sh`, and a self-contained offline `demo.sh`.
- uv support: committed `uv.lock` + `.python-version` (3.11) for a reproducible
  reference environment. README and quickstart lead with `uv sync` / `uv run`;
  pip + venv retained as a documented fallback (same `pyproject.toml`). Tests pass
  on the locked Python 3.11 environment.

### Added
- **Fivetran Activations (reverse ETL) readiness gate** — ingests Activations
  (Census-based) syncs via a new `ActivationsClient`/`ActivationsExtractor`/
  `ActivationsNormalizer` (`ACTIVATIONS_API_TOKEN`, scoped to `WAREHOUSE_DATABASE`).
  A new `dq/activation_gate.py` traverses dbt lineage *upstream* from each sync's
  source model and returns a verdict per sync: **allow | warn | block | unknown**.
  Policy: **block** on any upstream failing test, a warn-severity test with
  failures > 0 (a soft test actually firing on data headed to prod), failing source
  freshness, or a stale upstream Fivetran sync; **warn** on a source model with no
  enforced contract or an unmatched upstream source; **allow** otherwise. Syncs land
  in a top-level `activations` block (with per-sync `readiness` + a verdict rollup),
  are attached to the warehouse objects they consume (`warehouse_objects[].activations`),
  and raise an `activates_bad_data` risk (high when blocking). `get_impact` now
  includes activations; `get_column_impact` maps an affected column to the
  destination fields it feeds; `get_dq_summary` reports the verdict rollup. New MCP
  tools `list_activations` / `get_activation_readiness`, REST `/metadata/activations`
  + `/dq/activation-readiness`, and CLI `activations extract` + `build
  --no-activations`. **Verified live**: the churn→Salesforce Contact sync returns
  `block` — matched to `model.…customer_churn`, upstream warn-severity uniqueness
  test firing (143 duplicate rows) on data headed to a system of record.
- `examples/agent_transcript.md` — a read-through DQ-agent chat (tool calls +
  answers) over the demo data.
- `.github/workflows/refresh.yml` — scheduled (daily) + on-demand snapshot refresh
  via GitHub Actions, with optional warehouse-reader and S3 publishing; documented in
  the Capgemini quickstart alongside cron/Airflow/dbt-webhook alternatives.
- **dbt model governance**: extract per-model contract/access/group/version/owner
  from the manifest; aggregate `dbt.governance` onto warehouse objects (enforced
  contract, owners, groups, access levels); `missing_model_contract` risk (high when
  a downstream model is public + uncontracted) and `unowned_object` risk. Verified
  live on the GitHub build.
- **Column-level lineage** (sqlglot): parses compiled model SQL into column→column
  edges (resolving `SELECT *` via the catalog schema and tables via each leaf's
  source expression), exposed as `dbt.column_lineage_edges` plus a
  `get_column_impact(schema, table, column)` MCP tool that returns the downstream
  columns, metrics, and exposures a Fivetran column feeds. Optional `lineage` extra.
  Verified live: `github.repository.id` → mart → 3 metrics + dashboard exposure.
- **dbt Semantic Layer metric trust**: extract metrics + semantic models (resolving
  each metric to its upstream models, incl. ratio metrics via constituent metrics);
  attach `dbt.metrics` to warehouse objects; a top-level `metric_quality` rollup with a
  per-metric `trust_level` (trusted|watch|at_risk) from upstream DQ posture; a
  `metric_at_risk` risk; and `list_metrics` / `get_metric_quality` MCP tools. Verified
  live (3 metrics incl. a ratio).
- **dbt exposures → blast radius**: the dbt normalizer extracts exposures
  (type/maturity/owner/url); warehouse objects gain `dbt.exposures` (the
  dashboards/ML/apps they feed via lineage); an `impacts_exposure` risk (high) fires
  when a DQ problem reaches a consumer; new `get_impact(schema, table)` MCP tool
  returns blast radius. Verified live (GitHub dashboard + ML exposures).
- **Warehouse metadata reader** (`warehouse/`) — reads authoritative primary keys
  from the Fivetran Platform Connector's `fivetran_metadata` schema in a Snowflake
  destination and overrides PK flags during `build` (incl. composite keys), tagging
  columns `key_source: "fivetran_platform"`. Optional extra
  `warehouse-snowflake`; config via `WAREHOUSE_TYPE=snowflake` + `WAREHOUSE_*`; toggle
  with `build --warehouse-metadata/--no-warehouse-metadata`. Verified live: recovered
  75 PKs (GitHub exposed 0 via the config API) and drove 27 composite-key recs.

### Changed
- `build --aliases-file <json>` activates the `configured_alias` match tier from a
  `{"<dest_schema>.<dest_table>": "<dbt_schema>.<dbt_table>"}` map (see
  `examples/aliases.example.json`) — previously reachable only in code.
- DQ recommendation rules: `potential_pii` signal (PII-suggestive, non-hashed
  column names), `unique` on natural-key names (`email`/`username`/`slug`/`uuid`/…),
  `accepted_values [true,false]` on `is_`/`has_` columns, and an `untested_dbt_object`
  risk (matched to dbt but zero tests). Verified live: flagged commit author/committer
  emails as PII on the GitHub build.
- Fivetran connection filters `--connected-only` / `--skip-paused` on `fivetran
  extract` and `build` (`--connected-only` filters on `setup_state`; paused-but-
  connected connectors are excluded only by `--skip-paused`).
- Added `key_constraint` (`primary_key` | `primary_or_foreign_key` | `null`) to
  normalized Fivetran columns and warehouse-object columns.
- dbt extraction scoping (`--project-id`/`--job-id`, `--dbt-project-id`/`--dbt-job-id`)
  and Admin API pagination for `list_projects`/`list_environments`/`list_jobs`
  (previously truncated at 100).
- Pinned `starlette>=0.46,<1.0` to silence the Starlette `TestClient` httpx2
  deprecation warning (FastAPI only requires `starlette>=0.46`).

### Fixed
- `Settings(field_name=...)` was silently ignored for alias'd fields (`extra=ignore`),
  so programmatic overrides (e.g. a test's `metadata_local_path`) became no-ops.
  Enabled `populate_by_name=True`.
- **dbt artifact download returned 406** — the run-artifact endpoint rejects
  `Accept: application/json`; `get_run_artifact` now sends `Accept: */*`. Verified live.
- **Primary key detection** — Fivetran's config API omits `is_primary_key`; the
  normalizer derives keys from `enabled_patch_settings` (`SYSTEM_COLUMN` + a
  primary-key reason). Confident PKs set `is_primary_key`; ambiguous PK/FK set
  `key_constraint: primary_or_foreign_key`. Verified live (18 PKs, 52 ambiguous).

## [1.0.0] - 2026-06-25

### Added
- Initial release of the Fivetran + dbt Platform metadata service.
- **Clients**: `FivetranClient` (HTTP Basic auth, `Accept: application/json;version=2`,
  cursor pagination, 429/`Retry-After` handling, 5xx retries, URL-encoded path params)
  and `DbtClient` (dbt Cloud Admin API v2 + optional Discovery/Metadata GraphQL,
  token auth encapsulated in one place).
- **Extractors**: `FivetranExtractor` and `DbtExtractor` that collect partial
  results plus an `errors[]` array instead of aborting on a single failure.
- **Normalizers**: `FivetranNormalizer`, `DbtNormalizer` (defensive artifact
  parsing), and `CombinedNormalizer` (deterministic Fivetran↔dbt matching →
  `warehouse_objects`, `dq_summary`, `match_confidence`).
- **Data Quality**: `dq/recommendations.py` (primary keys, freshness, accepted
  values, relationships, hashed columns, missing coverage, failing tests, stale
  sync), `dq/drift.py` (schema/test/freshness drift between snapshots), and
  `dq/lineage.py` (lineage graph helper).
- **Storage**: `MetadataStorage` Protocol with `local` and `s3` backends.
- **Interfaces**: Typer CLI (`fivetran extract`, `dbt extract`, `build`, `drift`,
  `recommendations`, `serve-api`, `serve-mcp`), FastAPI REST service, and an
  optional MCP server (SDK-independent tool functions with a clean fallback when
  the MCP SDK is not installed).
- **Offline mode**: `build --fixtures-dir` builds a full snapshot from local JSON
  fixtures with no credentials.
- **Tests**: 31 pytest tests over JSON fixtures (no live API calls) covering
  Fivetran/dbt normalization, matching, recommendations, drift, storage, and the
  API health route.
- Documentation: `README.md` with setup, environment variables, CLI/REST/MCP
  usage, the JSON output contract, storage options, limitations, and extension
  points.

[Unreleased]: https://example.com/compare/v1.0.0...HEAD
[1.0.0]: https://example.com/releases/v1.0.0
