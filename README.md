# Fivetran + dbt Platform Metadata Service

A Python service that extracts, normalizes, stores, and serves metadata from
**Fivetran** and **dbt Platform** for use by an agentic **Data Quality**
application. It replaces a manually uploaded JSON metadata file with an automated
metadata pipeline plus optional REST and MCP interfaces.

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

Fivetran is the system of record for replicated source metadata; dbt Platform is
the system of record for transformation/analytics metadata. This service joins
the two into `warehouse_objects` and layers DQ recommendations + drift on top.

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

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"        # add ,s3 and/or ,mcp extras as needed

cp .env.example .env           # then fill in credentials
```

Optional extras:

```bash
pip install -e ".[dev,s3]"     # S3 storage backend (boto3)
pip install -e ".[dev,mcp]"    # MCP server (official Python MCP SDK)
```

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
| `STALE_SYNC_THRESHOLD_HOURS` | Stale-sync risk threshold | `24` |

Secrets are read from the environment only and are never logged.

## 5. CLI Usage

```bash
metadata-service fivetran extract --group-id <group>   # -> fivetran_raw_latest.json
metadata-service dbt extract                            # -> dbt_raw_latest.json

metadata-service build --group-id <group>               # full build -> latest.json snapshot
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

`POST /metadata/refresh` body:

```json
{ "fivetran_group_id": "optional", "include_dbt": true, "include_fivetran": true }
```

Response:

```json
{ "status": "success", "snapshot_uri": "...", "generated_at": "...", "object_count": 123, "error_count": 0 }
```

## 7. MCP Usage

The MCP server exposes narrow, task-focused tools:
`refresh_metadata`, `get_latest_metadata`, `get_warehouse_object`,
`get_dq_recommendations`, `get_schema_drift`.

```bash
pip install -e ".[mcp]"
metadata-service serve-mcp
```

Example Claude Desktop config entry:

```json
{
  "mcpServers": {
    "fivetran-dbt-metadata": {
      "command": "metadata-service",
      "args": ["serve-mcp"],
      "env": { "FIVETRAN_API_KEY": "...", "DBT_SERVICE_TOKEN": "..." }
    }
  }
}
```

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
             "runs": [], "models": [], "sources": [], "tests": [], "lineage_edges": [] }
  },
  "warehouse_objects": [],
  "dq_recommendations": [],
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

- **Storage**: implement `MetadataStorage` (e.g., GCS, Azure Blob).
- **Matching**: pass an `aliases` dict to `CombinedNormalizer`, or extend
  `_match` for warehouse-aware relation matching.
- **Recommendations**: add rules in `dq/recommendations.py`.
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
