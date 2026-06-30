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

### Changed
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
