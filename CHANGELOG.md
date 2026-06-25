# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `CHANGELOG.md` to track changes as they are made.
- `ARTIFACTS.md` documenting every JSON artifact the service produces.
- Fivetran connection filters `--connected-only` (skip broken/incomplete setups)
  and `--skip-paused` (skip paused connections), exposed on both
  `fivetran extract` and `build` and threaded through the extractor and pipeline.
  Note: `--connected-only` filters on `setup_state`, so paused-but-connected
  connectors are excluded only by `--skip-paused`.
- Column `key_constraint` field (`primary_key` | `primary_or_foreign_key` | `null`)
  on normalized Fivetran columns and warehouse-object columns.

### Fixed
- **Primary key detection.** Fivetran's config API does not return `is_primary_key`;
  key columns are locked via `enabled_patch_settings` (`reason_code: SYSTEM_COLUMN`
  with a primary-key reason). The normalizer now derives PKs from this, so
  `not_null`/`unique` recommendations fire again. Confident PKs (reason names only a
  primary key, e.g. Postgres `ctid`) set `is_primary_key: true`; SaaS/SDK connectors
  that lump "primary key or a foreign key" set `key_constraint: primary_or_foreign_key`
  and get a `not_null` (medium) recommendation only. Verified live: 18 confident PKs,
  52 ambiguous keys across the test account.

### Changed
- Pinned `starlette>=0.46,<1.0` to silence the Starlette `TestClient` deprecation
  warning that recommends `httpx2`. FastAPI only requires `starlette>=0.46`, so the
  pin is well within its supported range.

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
