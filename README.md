# Fivetran + dbt Platform Metadata Service

A Python service that extracts, normalizes, stores, and serves metadata from
**Fivetran** and **dbt Platform** for use by an agentic **Data Quality**
application. It replaces a manually uploaded JSON metadata file with an automated
metadata pipeline plus optional REST and MCP interfaces.

> **New here?** Start with the **[Capgemini DQaaS Quickstart & Solution Guide](docs/capgemini-quickstart.md)**
> — what it does, how agents use it, and where it fits — then the runnable
> **[examples/](examples/)** (agent client, REST, offline demo).
>
> **Live reference builds:**
> - [docs/use-cases/github-snowflake-dbt.md](docs/use-cases/github-snowflake-dbt.md)
>   walks a full GitHub → Fivetran → Snowflake → dbt → metadata-service end-to-end
>   (110 warehouse objects, 7 joined to dbt, 455 recommendations) — with exposures /
>   blast radius, Semantic Layer metric trust, column-level lineage, and dbt governance.
> - [docs/use-cases/reverse-etl-churn-salesforce.md](docs/use-cases/reverse-etl-churn-salesforce.md)
>   walks the **reverse-ETL activation readiness gate** — a retail churn model synced
>   back to Salesforce is **blocked** because a `severity: warn` test is firing on 143
>   duplicate rows headed to prod.

## 1. Project Purpose

Capgemini (and similar teams) build agentic Data Quality solutions on top of
Fivetran + dbt. This service produces a single normalized JSON snapshot that lets
a DQ agent answer questions like:

- What source generated this table, and which Fivetran connection replicated it?
- Which columns are primary keys? Which are hashed/sensitive?
- Which dbt models depend on this source? Which dbt tests exist, and which fail?
- Is source freshness passing? Did schema drift occur since the last snapshot?
- Which tables are enabled in Fivetran but missing dbt tests?
- What dbt tests should be recommended from Fivetran metadata?
- If a column is dropped or hashed, which downstream columns, metrics, and
  dashboards break? (column-level lineage + blast radius)
- Can we trust a given governed Semantic Layer metric?
- Which modeled objects lack an enforced contract or an owner?
- Is it **safe to push this data back to prod** via a reverse-ETL Activation, or
  does something upstream (a failing/firing test, a stale sync) make it unsafe?

Fivetran is the system of record for replicated source metadata; dbt Platform is
the system of record for transformation/analytics metadata. This service joins
the two into `warehouse_objects`, then layers on DQ recommendations, drift,
business-impact (exposures), metric trust, column-level lineage, governance, and
a reverse-ETL **activation readiness gate**.

## 2. Architecture

```text
Fivetran REST API      dbt Platform APIs / artifacts
        |                         |
        v                         v
  Fivetran Extractor        dbt Extractor
        |                         |
        v                         v
  Fivetran Normalizer       dbt Normalizer
        \                         /
         \                       /
          v                     v
          Combined Metadata Builder
                    |
                    v
        Normalized JSON Snapshot Store
          /          |             \
         v           v              v
       CLI       FastAPI           MCP Server
                    |
                    v
       Capgemini Agentic DQ Application
```

Module layout (`src/` layout):

```text
src/metadata_service/
  config.py logging_config.py exceptions.py pipeline.py cli.py
  clients/      fivetran_client.py  dbt_client.py
  extractors/   fivetran_extractor.py  dbt_extractor.py
  models/       common.py fivetran.py dbt.py normalized.py
  normalizers/  fivetran_normalizer.py dbt_normalizer.py combined_normalizer.py
  storage/      base.py local_storage.py s3_storage.py
  dq/           recommendations.py drift.py lineage.py
  api/          main.py routes.py
  mcp/          server.py tools.py
```

## 3. Setup

Recommended — [**uv**](https://docs.astral.sh/uv/) (reproducible: installs from the
committed `uv.lock` on the pinned Python in `.python-version`):

```bash
uv sync --all-extras                  # creates .venv + installs everything from the lock
uv run metadata-service build ...     # run without activating

cp .env.example .env                  # then fill in credentials
```

Pick specific extras instead of `--all-extras` with `--extra dev --extra mcp` (s3, mcp).

<details>
<summary>Fallback — pip + venv (no uv)</summary>

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"        # add ,s3 and/or ,mcp extras as needed:
pip install -e ".[dev,s3]"     # S3 storage backend (boto3)
pip install -e ".[dev,mcp]"    # MCP server (official Python MCP SDK)

cp .env.example .env
```

The package is standard `pyproject.toml`; uv and pip read the same metadata, so
either workflow works. The `uv.lock` pins the reference environment.
</details>

## 4. Environment Variables

| Variable | Purpose | Default |
|---|---|---|
| `FIVETRAN_API_KEY` / `FIVETRAN_API_SECRET` | Fivetran HTTP Basic auth | — |
| `FIVETRAN_GROUP_ID` | Default group filter | — |
| `FIVETRAN_BASE_URL` | Fivetran API base | `https://api.fivetran.com/v1` |
| `DBT_ACCOUNT_ID` | dbt Cloud account id | — |
| `DBT_SERVICE_TOKEN` | dbt Cloud service token | — |
| `DBT_BASE_URL` | dbt Cloud API base | `https://cloud.getdbt.com/api` |
| `DBT_METADATA_API_URL` | Discovery/Metadata GraphQL URL (optional) | — |
| `METADATA_STORAGE_BACKEND` | `local` or `s3` | `local` |
| `METADATA_LOCAL_PATH` | Local snapshot dir | `./metadata_snapshots` |
| `METADATA_S3_BUCKET` / `METADATA_S3_PREFIX` | S3 target | — / `metadata` |
| `LOG_LEVEL` | Logging level | `INFO` |
| `API_HOST` / `API_PORT` | FastAPI bind | `0.0.0.0` / `8080` |
| `WAREHOUSE_TYPE` | Prefix for object ids (`warehouse://...`) | `warehouse` |
| `WAREHOUSE_DATABASE` | Warehouse DB name (also scopes Activations to syncs reading it) | — |
| `STALE_SYNC_THRESHOLD_HOURS` | Stale-sync risk threshold | `24` |
| `ACTIVATIONS_API_TOKEN` | Fivetran Activations (Census) workspace token — enables the reverse-ETL readiness gate | — |
| `ACTIVATIONS_BASE_URL` | Activations API base | `https://app.getcensus.com/api/v1` |

Secrets are read from the environment only and are never logged.

## 5. CLI Usage

```bash
metadata-service fivetran extract --group-id <group>   # -> fivetran_raw_latest.json
metadata-service dbt extract                            # -> dbt_raw_latest.json
metadata-service activations extract                    # -> activations_raw_latest.json (reverse ETL)

metadata-service build --group-id <group>               # full build -> latest.json snapshot
metadata-service build --no-activations                 # skip the reverse-ETL readiness gate
metadata-service build --fixtures-dir tests/fixtures    # OFFLINE build from fixtures (no creds)

# Connection filters (apply to `fivetran extract` and `build`):
metadata-service fivetran extract --connected-only      # skip broken/incomplete setups
metadata-service fivetran extract --skip-paused         # skip paused connections
metadata-service build --group-id <group> --connected-only --skip-paused

metadata-service drift                                  # compare latest vs previous snapshot
metadata-service recommendations --schema salesforce --table account

metadata-service serve-api                              # start FastAPI
metadata-service serve-mcp                              # start MCP server (needs mcp extra)
```

The offline `--fixtures-dir` mode is the quickest way to see a real `latest.json`
without any credentials.

## 6. REST API

```bash
metadata-service serve-api
```

| Method & path | Description |
|---|---|
| `GET /health` | Liveness check |
| `POST /metadata/refresh` | Run extraction + write a new snapshot |
| `GET /metadata/latest` | Full normalized document |
| `GET /metadata/fivetran` | Fivetran section |
| `GET /metadata/dbt` | dbt section |
| `GET /metadata/warehouse-objects?schema=&table=` | Filterable list |
| `GET /metadata/warehouse-objects/{object_id}` | Single object |
| `GET /dq/recommendations?schema=&table=` | DQ recommendations |
| `GET /dq/drift?severity=` | Drift records |
| `GET /metadata/activations?verdict=` | Reverse-ETL syncs + readiness verdicts |
| `GET /dq/activation-readiness?sync_id=&label=` | Readiness detail for one activation |

`POST /metadata/refresh` body:

```json
{ "fivetran_group_id": "optional", "include_dbt": true, "include_fivetran": true }
```

Response:

```json
{ "status": "success", "snapshot_uri": "...", "generated_at": "...", "object_count": 123, "error_count": 0 }
```

## 7. MCP Usage

The MCP server is the drop-in hook for an agentic Data Quality application: a
narrow, token-efficient tool surface over the normalized metadata.

```bash
uv sync --extra mcp        # or: pip install -e ".[mcp]"
uv run metadata-service serve-mcp                          # stdio (local subprocess agents)
uv run metadata-service serve-mcp --transport http --port 8765   # hosted / remote agents
```

### Tools (designed for agent triage → drill-down)

| Tool | Use | Typical size |
|---|---|---|
| `get_dq_summary()` | **Start here.** Account rollup: counts by risk, missing coverage, failing tests, stale syncs, recommendations by type/confidence, drift. | ~0.5 KB |
| `list_warehouse_objects(schema, risk_level, missing_coverage, failing_tests, stale, limit)` | Compact, filterable index for triage (small rows, no columns/tests). | ~0.3 KB/row |
| `get_warehouse_object(schema, table)` | Full detail for one object (origin, dbt, columns, tests, exposures, metrics, governance, dq_summary). | ~few KB |
| `get_impact(schema, table)` | **Blast radius**: downstream dbt models, exposures (dashboards/ML/apps), and reverse-ETL activations. | small |
| `get_column_impact(schema, table, column)` | **Column-level** blast radius: downstream columns, affected metrics/exposures, and the destination fields it feeds. | small |
| `list_metrics()` / `get_metric_quality(metric)` | Governed Semantic Layer metrics with a **trust level** from upstream DQ posture. | small |
| `list_activations(verdict)` / `get_activation_readiness(sync_id, label)` | Reverse-ETL syncs with a readiness **verdict** (allow\|warn\|block) — 'is it safe to push this data back to prod?' | small |
| `get_dq_recommendations(schema, table, recommendation_type, confidence, risk, limit)` | Per-object **or** cross-snapshot recommendation filtering. | scales with filter |
| `get_schema_drift(schema, table, severity)` | Drift records since the previous snapshot. | scales with filter |
| `get_latest_metadata(scope)` | Full snapshot (large — prefer the tools above). | up to ~400 KB |
| `refresh_metadata(fivetran_group_id, include_fivetran, include_dbt)` | Trigger a new extraction + snapshot. | small |

Recommended agent flow: `get_dq_summary()` to orient → `list_warehouse_objects(...)`
to find the objects that matter (e.g. `missing_coverage=true` or `failing_tests=true`)
→ `get_warehouse_object()` / `get_dq_recommendations()` to act. This keeps payloads
small instead of pulling the whole 400 KB snapshot.

### Connecting

**Local (stdio)** — e.g. Claude Desktop / a local agent runtime:

```json
{
  "mcpServers": {
    "fivetran-dbt-metadata": {
      "command": "metadata-service",
      "args": ["serve-mcp"],
      "env": { "FIVETRAN_API_KEY": "...", "FIVETRAN_API_SECRET": "...",
               "DBT_ACCOUNT_ID": "...", "DBT_SERVICE_TOKEN": "..." }
    }
  }
}
```

With uv (no activated env): set `"command": "uv"`, `"args": ["run", "metadata-service",
"serve-mcp"]`, and add `"cwd": "/path/to/metadata-service"`.

**Hosted (HTTP)** — for a remote agent in a DQaaS platform:

```bash
metadata-service serve-mcp --transport http --host 0.0.0.0 --port 8765
# agent connects to the streamable-http endpoint at http://<host>:8765/mcp
```

Transport and bind can also be set via `MCP_TRANSPORT` / `MCP_HOST` / `MCP_PORT`.

The tool logic lives in `mcp/tools.py` as plain functions (SDK-independent and
unit-tested). `mcp/server.py` binds them to the official MCP SDK; if the SDK is
not installed it raises a clear error with install instructions.

## 8. JSON Output Contract

Top-level snapshot shape (`latest.json`):

```json
{
  "generated_at": "2026-06-25T00:00:00Z",
  "version": "1.0",
  "sources": {
    "fivetran": { "extracted_at": "...", "connections": [] },
    "dbt": { "extracted_at": "...", "projects": [], "environments": [], "jobs": [],
             "runs": [], "models": [], "sources": [], "tests": [], "exposures": [],
             "metrics": [], "semantic_models": [], "lineage_edges": [], "column_lineage_edges": [] }
  },
  "warehouse_objects": [],
  "dq_recommendations": [],
  "metric_quality": [],
  "schema_drift": [],
  "errors": []
}
```

Each `warehouse_objects` entry joins Fivetran + dbt with `origin`, `dbt`,
`columns`, a `dq_summary`, and a `match_confidence` of one of:
`exact_relation`, `exact_schema_table`, `case_insensitive_schema_table`,
`configured_alias`, `unmatched`. See `models/normalized.py` for the full model.

Recommendations carry a `confidence` of `high`, `medium`, or `heuristic`, keeping
explicit recommendations separate from heuristic ones.

## 9. Storage Options

- **local** (default): `metadata_snapshots/latest.json` plus
  `YYYY/MM/DD/<timestamp>.json` history.
- **s3**: `s3://bucket/prefix/latest.json` plus dated history. Requires the `s3`
  extra (`boto3`); raises a clear error if it is not installed.

The interface is `storage/base.py:MetadataStorage`; add a backend by implementing
that Protocol and wiring it into `get_storage`.

## 10. Known Limitations

- Object ids are warehouse-agnostic (`warehouse://db/schema/table`). Fivetran
  does not expose the destination database, so it is recorded as `unknown` and
  matching uses schema + table. `exact_relation` is reserved for when a database
  is known.
- Matching is intentionally **deterministic** — no fuzzy matching (it produces
  dangerous false positives). Use the `aliases` map for known exceptions.
- dbt metadata is derived from run **artifacts** (manifest/catalog/run_results/
  sources). The Discovery GraphQL API is supported but optional.
- Relationship and accepted-values recommendations are heuristic and labeled as
  such; they are suggestions, not assertions.

## 11. Extension Points

- **Matching**: supply an aliases map via `build --aliases-file <json>` (a
  `{"<dest_schema>.<dest_table>": "<dbt_schema>.<dbt_table>"}` map — see
  [`examples/aliases.example.json`](examples/aliases.example.json)) to activate the
  `configured_alias` tier, or extend `_match` for warehouse-aware matching.
- **Recommendations**: add rules in `dq/recommendations.py` (PII, natural keys,
  boolean accepted-values, untested objects, etc. ship today).
- **Authoritative PKs**: read the Fivetran Platform Connector's `fivetran_metadata`
  schema from a Snowflake destination — set `WAREHOUSE_TYPE=snowflake` + `WAREHOUSE_*`
  and install `pip install 'metadata-service[warehouse-snowflake]'`. The build then
  overrides PK flags (incl. composite keys) from `SOURCE_COLUMN`/lineage; columns are
  tagged `key_source: "fivetran_platform"`. Extend `warehouse/` for BigQuery/Databricks.
- **Storage**: implement `MetadataStorage` (e.g., GCS, Azure Blob).
- **Drift**: add change types + severities in `dq/drift.py`.
- **dbt Discovery API**: use `DbtClient.query_discovery` for richer lineage.

## How Capgemini Plugs This In

1. Schedule `metadata-service build` (cron/Airflow) to publish `latest.json` to
   shared storage (local volume or S3).
2. Point the DQ agent at either the JSON snapshot directly, the REST API
   (`GET /metadata/latest`), or the MCP tools.
3. The agent consumes `warehouse_objects`, `dq_recommendations`, and
   `schema_drift` to reason about coverage, freshness, and risk.

## Running Tests

```bash
pytest
```

All tests run against JSON fixtures in `tests/fixtures/` — no live API calls.
