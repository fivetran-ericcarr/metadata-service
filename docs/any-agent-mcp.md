# Any-agent MCP: semantic context for Claude, ChatGPT, Gemini, Cortex and Genie

metadata-service serves one MCP endpoint that any agent can call to ground
SQL and caveats before it answers: governed dbt Semantic Layer metrics,
descriptions and synonyms, Fivetran provenance, DQ trust, and where each table
physically lives on every engine (Snowflake, Polaris, Glue, Unity). This page
covers the remote deployment and how each agent connects.

- [What an agent gets](#what-an-agent-gets)
- [Run it remotely](#run-it-remotely)
- [Authentication by agent](#authentication-by-agent)
- [Connect each agent](#connect-each-agent)
- [Deploy to Cloud Run](#deploy-to-cloud-run)
- [Curated vocabulary (overrides file)](#curated-vocabulary-overrides-file)

## What an agent gets

The remote server exposes the **read-only** tool set by default
(`MCP_TOOLS=auto`: all tools on stdio, read-only on HTTP). `refresh_metadata`
(billed extraction, snapshot writes) is never exposed remotely unless you set
`MCP_TOOLS=all`. Every tool carries `readOnlyHint: true`, so clients that
honor annotations (ChatGPT, Claude) skip write confirmations.

| Tool | Use it for |
|---|---|
| `get_semantic_context(query, kind="auto", top_k=2, max_columns=15)` | **The grounding call.** `query` is a metric, dbt model, `schema.table`, an id from `search` (`metric:bind_rate`), or a topic ("written premium by state"). |
| `list_semantic_metrics(trust_level, search, limit, offset)` | Browse governed metrics with type, inputs, models, synonyms, trust. |
| `search(query)` / `fetch(id)` | The `search`/`fetch` pair ChatGPT company knowledge and deep research call. `fetch` returns the semantic context JSON as the document text. |
| `get_dq_summary`, `list_warehouse_objects`, `get_warehouse_object`, `get_impact`, `get_column_impact`, `list_metrics`, `get_metric_quality`, `list_activations`, `get_activation_readiness`, `get_dq_recommendations`, `get_schema_drift`, `get_latest_metadata` | The existing DQ tools (unchanged). |

The same payload is available over REST (`GET /semantic/context?query=...`,
`GET /semantic/metrics`) and the CLI (`metadata-service semantic-context
"<query>"`, `metadata-service semantic-metrics`).

### `get_semantic_context` response

```text
{
  query, resolved_as: metric|model|table|null,
  snapshot: {generated_at, version, data_mode: live|fixtures, destination: {service, warehouse_type, database}},
  subjects: [                      # top_k expanded matches, best first
    # kind = metric
    {kind, name, label, type, description (+ curated note),
     definition: {measures: [{role: measure|numerator|denominator|input,
                              name | metric, agg, expr, semantic_model, filter, measures[]}],
                  expr, metrics[], window, grain_to_date, conversion,
                  filter: [where_sql_template], time_grain, agg_time_dimension},
     query_with: {metrics: [name], group_by: ["metric_time", "<entity>__<dimension>", ...]},
     dimensions: [{name, group_by, type, time_granularity, semantic_model}], dimensions_truncated,
     entities: [{name, type, semantic_model}], semantic_models[], models[],
     synonyms[], trust: {level, failing_tests, unverified_tests, upstream_object_count},
     locations: [{catalog, fqn, engines}], match_score},
    # kind = model
    {kind, unique_id, name, description, materialized, grain, owner, synonyms[],
     columns: [{name, data_type, description, synonyms[],
                source_columns: [{column, source_table, connector_service, connection_id}]}],
     columns_truncated, tests: {total, by_status}, locations[], status, match_score},
    # kind = table (a Fivetran-synced destination table)
    {kind, object_id, name, risk_level, dbt_source, downstream_models[],
     columns: [{name, data_type, nullable, is_primary_key, description, source_name}],
     locations: [{catalog: polaris|glue|unity|snowflake|..., fqn, engines, ...}],
     lake: {table_format, format_version, partition_spec, last_updated, snapshot_at,
            total_records, catalogs}, match_score}
  ],
  other_matches: [{id, name, kind, score}],      # pass an id back as `query`
  provenance: [{connection_id, connector_service, last_successful_sync, sync_state,
                setup_state, data_mode, tables: [{destination, source}], stale_tables, table_count}],
  trust: {level: trusted|watch|at_risk|unknown,
          caveats: [{kind: failing_test|stale_sync|warn_test|quarantine|unverified_test,
                     id, message, test, status, failures, severity, attached, tests, rule}],
          caveats_truncated},
  guidance: [string],              # from the overrides file, when set
  message                          # only when nothing matched
}
```

Empty and null fields are dropped to keep payloads small. Caps: `top_k`
(1-10), `max_columns` (1-100), 12 dimensions, 15 caveats, 8 tables per
connection, each with a `*_truncated` count.

How the pieces are derived:

- **Definitions** come from the dbt manifest's `metrics` and `semantic_models`
  (the snapshot now keeps `type_params`, filters, entities, dimensions and
  measure `agg`/`expr`). A ratio's numerator and denominator are resolved to
  their metrics and those metrics' measures.
- **Synonyms**: dbt `meta.synonyms` on the metric/model/column, then the
  overrides file, then the **legacy source column name** found through
  column-level lineage when a column is a pure rename of exactly one Fivetran
  source column (`risk_grade_code` ← `RISKGRADECD`). Columns derived from
  several source columns list them under `source_columns` but do not get
  synonyms.
- **Trust** walks the subject's dbt ancestors: failing/warn/unverified
  (`reused` without a prior result) tests, stale Fivetran syncs grouped per
  connection, and any quarantine register (a model named or tagged
  `quarantine`) sharing those ancestors. A metric's snapshot `trust_level` can
  only make the result worse.
- **Locations** come from destination facts only: no catalog call is needed to
  tell an agent `<destination_id>.salesforce.account` (Polaris),
  `awsdatacatalog.salesforce.account` (Glue/Athena), or
  `<uc_catalog>.salesforce.account` (Unity, Delta).

Example (`query="active rate by status"`, `top_k=1`, from the test project,
about 3.4 KB):

```json
{
  "query": "active rate by status",
  "resolved_as": "metric",
  "snapshot": {"generated_at": "2026-11-11T22:16:37Z", "version": "1.0", "data_mode": "fixtures",
               "destination": {"service": "snowflake", "warehouse_type": "snowflake", "database": "ANALYTICS"}},
  "subjects": [{
    "kind": "metric", "name": "active_rate", "label": "Active Rate", "type": "ratio",
    "description": "Share of accounts that are active. Test accounts are excluded.",
    "definition": {
      "measures": [
        {"role": "numerator", "metric": "active_accounts", "type": "simple",
         "filter": ["{{ Dimension('account__account_status_code') }} != 'Test'"],
         "measures": [{"name": "active_account_count", "agg": "sum",
                       "expr": "case when is_active then 1 else 0 end", "semantic_model": "accounts"}]},
        {"role": "denominator", "metric": "total_accounts", "type": "simple",
         "measures": [{"name": "account_count", "agg": "count", "expr": "account_id", "semantic_model": "accounts"}]}
      ],
      "time_grain": "day", "agg_time_dimension": "created_date"
    },
    "query_with": {"metrics": ["active_rate"], "group_by": ["metric_time", "account__account_status_code"]},
    "dimensions": [
      {"name": "account_status_code", "group_by": "account__account_status_code", "type": "categorical", "semantic_model": "accounts"},
      {"name": "created_date", "group_by": "account__created_date", "type": "time", "time_granularity": "day", "semantic_model": "accounts"}
    ],
    "models": ["dim_account"], "synonyms": ["activation ratio"],
    "trust": {"level": "at_risk", "failing_tests": 0, "unverified_tests": 0, "upstream_object_count": 1},
    "locations": [{"catalog": "dbt_relation", "fqn": "analytics.marts.dim_account", "engines": ["snowflake"]}]
  }],
  "other_matches": [{"id": "metric:active_accounts", "name": "active_accounts", "kind": "metric", "score": 5.6}],
  "provenance": [{"connection_id": "conn_sf_001", "connector_service": "salesforce",
                  "last_successful_sync": "2026-06-25T12:34:56Z", "data_mode": "fixtures",
                  "tables": [{"destination": "salesforce.account", "source": "salesforce.Account"}],
                  "stale_tables": 1, "table_count": 1}],
  "trust": {"level": "at_risk", "caveats": [
    {"kind": "stale_sync", "id": "STALE_SYNC:conn_sf_001",
     "message": "Fivetran connection conn_sf_001 (salesforce) last synced 2026-06-25T12:34:56Z; 1 upstream table(s) flagged stale: Account."},
    {"kind": "warn_test", "id": "BAD_STATUS", "failures": 3, "attached": "stg_account.account_status_code",
     "message": "3 accounts carry an unknown status code", "rule": true},
    {"kind": "quarantine", "id": "QUARANTINE:dq_quarantine",
     "message": "Records failing DQ rules upstream are registered in analytics.silver.dq_quarantine; ..."}
  ]},
  "guidance": ["Prefer governed metrics."]
}
```

## Run it remotely

```bash
uv sync --extra remote
export METADATA_API_KEY=$(openssl rand -hex 32)
metadata-service serve-mcp --transport http --host 0.0.0.0 --port 8765
# endpoint: http://<host>:8765/mcp     health: /health (no auth)
```

The server speaks **Streamable HTTP** at `/mcp`, stateless by default
(`MCP_STATELESS=true`: JSON responses, no server-side session, so any
instance can serve any request). `serve-mcp` refuses a public bind unless auth
is configured. SSE (`--transport sse`) has no auth hook and still needs an
authenticating proxy.

| Setting | Purpose | Default |
|---|---|---|
| `MCP_TOOLS` | `auto`, `read-only`, `all`, or a comma list of tool names | `auto` |
| `MCP_AUTH` | `auto` (oauth if `MCP_OAUTH_ISSUER`, else api_key if a key is set), `api_key`, `oauth`, `none` | `auto` |
| `METADATA_API_KEY` / `MCP_API_KEYS` | Static bearer keys (comma list for one key per agent) | — |
| `MCP_OAUTH_ISSUER` | OAuth authorization server issuer URL (JWTs validated against its JWKS) | — |
| `MCP_OAUTH_AUDIENCE` | Expected `aud` (your API identifier or the resource URL) | — |
| `MCP_OAUTH_JWKS_URL` | JWKS URL if not discoverable from the issuer | discovered |
| `MCP_OAUTH_REQUIRED_SCOPES` | Space-separated scopes every token must carry | — |
| `MCP_PUBLIC_URL` | Public `https://.../mcp` URL: OAuth resource id, Host allowlist, `search` citation URLs | — |
| `MCP_ALLOWED_HOSTS` | Extra Host header allowlist entries (DNS-rebinding protection) | — |
| `MCP_STATELESS` | Stateless Streamable HTTP | `true` |
| `SEMANTIC_OVERRIDES_FILE` | Curated synonyms/notes/rule text (below) | — |

## Authentication by agent

Checked against each vendor's documentation in November 2026. Two server
modes cover every client:

- **`api_key`**: static bearer key in `Authorization: Bearer <key>` (or
  `X-API-Key`).
- **`oauth`**: OAuth 2.1 resource server per the MCP authorization spec. The
  server returns `401` with `WWW-Authenticate: Bearer resource_metadata=...`,
  publishes RFC 9728 metadata at `/.well-known/oauth-protected-resource/mcp`
  naming your authorization server, and validates its JWT access tokens
  (signature via JWKS, `iss`, `exp`, `aud`, scopes). metadata-service is not an
  authorization server: bring one that supports **Dynamic Client Registration
  (DCR)** or **Client ID Metadata Documents (CIMD)**, such as Auth0, Okta,
  Microsoft Entra ID, AWS Cognito, WorkOS, Stytch or Descope. Static keys
  are still accepted in this mode, so one deployment serves both kinds of
  client.

| Agent | What it supports | Use |
|---|---|---|
| **Claude Code** | Remote HTTP servers with custom headers | `api_key` |
| **Claude (claude.ai / Desktop custom connector)** | OAuth (Claude's published identity via CIMD, DCR, or your own client) or **No sign-in + Request headers** for an API key. Request headers are in beta and not available to every organization. | `api_key` via Request headers where available, else `oauth` |
| **ChatGPT (developer mode / apps & connectors)** | OAuth (CIMD, DCR or a predefined client, Authorization Code + PKCE) or **No authentication**. No static-header option. Company knowledge and deep research call `search` and `fetch`. | `oauth` |
| **Gemini (Google ADK / Vertex AI Agent Engine, Gemini CLI)** | Streamable HTTP with arbitrary headers (ADK `StreamableHTTPConnectionParams(headers=...)`, or a `header_provider` for per-user tokens) | `api_key` (or forward a user's OAuth token) |
| **Snowflake Cortex Agents / Snowflake Intelligence** | External MCP servers through an `API_PROVIDER = EXTERNAL_MCP` API integration. **OAuth only** (`OAUTH2` client id/secret, `OAUTH_DYNAMIC_CLIENT`, or a public client). Tools only. | `oauth`, or the generated semantic view (below) |
| **Databricks (Mosaic AI agents / AI Gateway MCP services)** | External MCP servers through a Unity Catalog HTTP connection with **Bearer token**, DCR or OAuth M2M. Genie spaces cannot be wrapped as, or call, an external MCP service. | `api_key` |

Never use `MCP_AUTH=none` on a public URL: the snapshot is a full warehouse
inventory, including PII-flagged columns.

## Connect each agent

Examples use `https://metadata-mcp-xxxx.run.app/mcp`; replace with your URL.

### Claude Code

```bash
claude mcp add --transport http fivetran-dbt-metadata \
  https://metadata-mcp-xxxx.run.app/mcp \
  --header "Authorization: Bearer $METADATA_API_KEY"
```

Local alternative (no network): `claude mcp add fivetran-dbt-metadata -- uv
run --directory /path/to/metadata-service metadata-service serve-mcp`.

### Claude (claude.ai, Desktop, Team/Enterprise)

**Customize > Connectors > Add custom connector** (Team/Enterprise: an Owner
uses **Organization settings > Connectors**). Enter the `/mcp` URL, then either:

- **No sign-in**, and under **Request headers** add `authorization` =
  `Bearer <key>` (include `Bearer `), or
- with `MCP_AUTH=oauth`, **Sign in now**, OAuth client **Use Claude's
  published identity** (CIMD) or **Register automatically** (DCR).

### ChatGPT

1. Deploy with `MCP_AUTH=oauth`, `MCP_OAUTH_ISSUER=https://<tenant>.<idp>/`,
   `MCP_OAUTH_AUDIENCE=<api identifier>`, `MCP_PUBLIC_URL=https://.../mcp`.
   In the authorization server, enable DCR (or CIMD) and refresh tokens, and
   allow ChatGPT's redirect URI.
2. In ChatGPT, turn on **Developer mode** (under Settings, Apps & Connectors,
   Advanced at the time of writing; menu names, plan availability and workspace
   admin controls change, so check OpenAI's developer-mode guide).
3. **Create** a connector with the `/mcp` URL and authentication **OAuth**.
4. In a chat, enable the connector. Ask "What is our bind rate and can I trust
   it?"; ChatGPT calls `get_semantic_context` (or `search` then `fetch` in
   deep research). `fetch` URLs point at `GET /semantic/context` on
   `MCP_PUBLIC_URL` for citations.

### Gemini: Google ADK / Vertex AI Agent Engine

```python
import os
from google.adk.agents import LlmAgent
from google.adk.tools.mcp_tool.mcp_toolset import McpToolset
from google.adk.tools.mcp_tool.mcp_session_manager import StreamableHTTPConnectionParams

metadata = McpToolset(
    connection_params=StreamableHTTPConnectionParams(
        url="https://metadata-mcp-xxxx.run.app/mcp",
        headers={"Authorization": f"Bearer {os.environ['METADATA_API_KEY']}"},
    ),
    tool_filter=["get_semantic_context", "list_semantic_metrics", "get_metric_quality"],
)
agent = LlmAgent(model="<gemini model id>", name="analyst",
                 instruction="Call get_semantic_context before writing SQL; state its caveats.",
                 tools=[metadata])
```

Class names have been spelled `MCPToolset` and `McpToolset` across ADK
releases, so check your installed version. On Agent Engine, read the key from
Secret Manager. **Gemini CLI**: add to `~/.gemini/settings.json`:

```json
{"mcpServers": {"fivetran-dbt-metadata": {
  "httpUrl": "https://metadata-mcp-xxxx.run.app/mcp",
  "headers": {"Authorization": "Bearer <key>"}}}}
```

### Snowflake Cortex Agents / Snowflake Intelligence

Cortex calls external MCP servers over OAuth only, so deploy with
`MCP_AUTH=oauth`, then (ACCOUNTADMIN or a role with CREATE INTEGRATION):

```sql
CREATE API INTEGRATION metadata_mcp_integration
  API_PROVIDER = EXTERNAL_MCP
  API_ALLOWED_PREFIXES = ('https://metadata-mcp-xxxx.run.app/mcp')
  API_USER_AUTHENTICATION = (
    TYPE = OAUTH_DYNAMIC_CLIENT
    OAUTH_RESOURCE_URL = 'https://metadata-mcp-xxxx.run.app/mcp'
  )
  ENABLED = TRUE;

CREATE EXTERNAL MCP SERVER metadata_mcp
  WITH DISPLAY_NAME = 'Fivetran + dbt semantic context'
  URL = 'https://metadata-mcp-xxxx.run.app/mcp'
  API_INTEGRATION = metadata_mcp_integration;
```

If discovery from the resource URL is not enough for your authorization
server, add `OAUTH_DISCOVERY_URL` (or the token/authorization endpoints). Add
`metadata_mcp` to the agent specification, and grant USAGE on both the
server and the integration (plus per-tool grants) to the agent's users. If
your authorization server has no DCR, use `TYPE = OAUTH2` with
`OAUTH_CLIENT_ID`/`OAUTH_CLIENT_SECRET`/`OAUTH_TOKEN_ENDPOINT`/
`OAUTH_AUTHORIZATION_ENDPOINT`.

**Without OAuth**: generate a Snowflake semantic view from the same context
(the Guidewire demo's `semantic/build_semantic_view.py` pattern: metrics,
synonyms, caveats in comments, verified queries) and point Cortex Analyst at
it. The semantic view is a build artifact, so its trust caveats are as of the
last build, while the MCP path is live per question.

### Databricks

1. In **Unity Gateway > MCPs**, choose **+ MCP > Connect an existing MCP
   server**, pick a catalog and schema, name it (for example
   `metadata_mcp`), and **Create new connection**: the `/mcp` URL,
   authentication **Bearer token**, the key.
2. **Create & load tools**, keep the read-only tools, **Create MCP Service**.
3. Agents (Mosaic AI Agent Framework, Agent Bricks) reference it as
   `<catalog>.<schema>.metadata_mcp`. Clients that need a URL use
   `https://<workspace>/ai-gateway/mcp-services/<catalog>.<schema>.metadata_mcp`.

**Genie**: a Genie space cannot call an external MCP server. Two options:
(a) build a Databricks agent that calls the Genie space *and*
`metadata_mcp`, grounding Genie's SQL with `get_semantic_context`; or
(b) paste `list_semantic_metrics` / `get_semantic_context` output (metric
definitions, synonyms, caveats) into the Genie space's instructions and
example SQL. That copy goes stale like the Snowflake semantic view, so
regenerate it with each build.

## Deploy to Cloud Run

The image is the repo's [`Dockerfile`](../Dockerfile); the script is
[`deploy/cloudrun/deploy.sh`](../deploy/cloudrun/deploy.sh). It follows the
pattern of the ODI trust demo's `fivetran-code` MCP service: Secret Manager
for secrets, `--allow-unauthenticated` at the Cloud Run IAM layer (external
agents can't mint Google ID tokens), and per-request auth in the app.

```bash
bash deploy/cloudrun/deploy.sh --init-secrets   # API key secret + snapshot bucket
bash deploy/cloudrun/deploy.sh                  # build from source + deploy
# then redeploy once with MCP_PUBLIC_URL=<url>/mcp (OAuth, citations, Host allowlist)
```

- Snapshots: a GCS bucket mounted read-only at `/snapshots`
  (`METADATA_LOCAL_PATH`). Build snapshots elsewhere on a schedule (CI, a
  Cloud Run job, cron) and copy `latest.json` there. The service only reads.
- OAuth: export `MCP_OAUTH_ISSUER` and `MCP_OAUTH_AUDIENCE` before running.
- Curated vocabulary: bake the overrides JSON into the image or the bucket and
  set `SEMANTIC_OVERRIDES_FILE`.
- Health: `GET /health` (no auth) returns the registered tool count.
- Cloud Run serves the app on two hostnames (`*.run.app` with and without
  the project number). With `MCP_PUBLIC_URL` set, Host validation is on, so add
  the other hostname to `MCP_ALLOWED_HOSTS` if clients use it.

## Curated vocabulary (overrides file)

The manifest carries what dbt knows; domain vocabulary goes in
`SEMANTIC_OVERRIDES_FILE`. See the docstring in
`src/metadata_service/semantic/overrides.py` for the schema, and
[`examples/semantic_overrides/guidewire_underwriting.json`](../examples/semantic_overrides/guidewire_underwriting.json)
for a full example generated from the Guidewire demo's
`semantic/curated_context.py`: column and metric synonyms, metric notes, DQ
rule patterns with plain-English impact (`{n}` = failing rows), an
out-of-scope list (keeps ClaimCenter caveats out of underwriting answers), and
agent guidance. Prefer dbt `meta.synonyms` where you own the dbt project. Use
the file for what does not belong in dbt.
