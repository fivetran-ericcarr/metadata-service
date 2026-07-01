# Claude Code Build Prompt: Fivetran Activations (Reverse ETL) for the DQ Lifecycle

```text
You are Claude Code, a senior Python platform engineer, extending the existing
`metadata-service` project (Fivetran + dbt metadata for agentic Data Quality).

Work on the branch `feat/activations-reverse-etl` (already created). Do NOT break
existing behavior; all current tests must keep passing. Use `uv` (uv.lock,
.python-version 3.11): `uv sync --all-extras`, `uv run pytest`, `uv run metadata-service ...`.

## Objective

Close the data-quality loop into a full **EL → T → Reverse ETL** lifecycle by adding
**Fivetran Activations** (reverse ETL / the Census-based product) as the RETL leg:

  Fivetran (EL) → dbt (T) → [DQ gate] → Fivetran Activations (RETL → CRM/ops)

Deliver two capabilities, plus a reverse-ETL-appropriate demo dataset:

1. **DQ activation gate** — "is this data safe to push back to a production system?"
   A verdict (`allow | warn | block`) for any warehouse object, computed from the
   quality signals the service already has (failing tests, freshness, stale sync,
   missing PK, metric trust, contract/coverage). Reverse ETL writes to operational
   systems, so activating bad data has real consequences; this gate is the headline.

2. **Activation lineage (operational blast radius)** — ingest Activations metadata
   (sync → source model → destination object/field mappings) and attach it to
   `warehouse_objects` as `activations`, the write-side mirror of `exposures`. Extend
   blast radius end-to-end: Fivetran source column → dbt model → activation →
   `Salesforce.Account.<field>`.

3. **Demo dataset** — a customer-churn → CRM reverse-ETL example (see Phase 5).

## CRITICAL: verify before you build

This project's history is full of APIs that don't behave as documented (Fivetran
Metadata API deprecated; config API omits PKs; dbt artifacts need `Accept: */*`;
Discovery API has no column-level lineage). Do NOT assume the Activations API shape.

The Fivetran **Activations REST API is a SEPARATE surface** from
`api.fivetran.com/v1` (it's the Census-based product: workspaces → syncs → mappings,
e.g. `PATCH /api/v1/syncs/{id}`). Before writing the client:

- Read the docs: https://fivetran.com/docs/activations/rest-api and its API reference.
- Determine the real **base URL**, **auth scheme** (workspace API token — how it's
  passed), and the endpoints for listing **syncs**, **sources/models**,
  **destinations/connections**, and **mappings**.
- Probe the live workspace (credentials will be provided as env vars — see Config):
  list syncs, fetch one sync's detail + mappings, inspect the exact JSON field names
  and how a sync references (a) its source model/table and (b) its destination object
  + field mappings. Print the shapes. Build the normalizer against the REAL shapes,
  not assumptions. If a probe 404s or the token lacks scope, capture it and adapt.

Only after the shapes are confirmed should you implement extraction/normalization.

## Configuration (add to config.py + .env.example)

New settings (optional; the feature is inert unless configured):
- `ACTIVATIONS_ENABLED` (bool; or derive from token presence)
- `ACTIVATIONS_BASE_URL` (confirm from docs; default the documented Activations host)
- `ACTIVATIONS_API_TOKEN` (workspace API token — SECRET)
- `ACTIVATIONS_WORKSPACE_ID` (if the API is workspace-scoped)

Add `settings.activations_enabled()` mirroring `warehouse_reader_enabled()`. Never
log the token. Add an optional extra in pyproject if a new dependency is needed
(prefer httpx, already present).

## Feature 1 — Activations client + ingestion

- `clients/activations_client.py` — `ActivationsClient` over the Activations REST API:
  typed methods `list_syncs()`, `get_sync(id)`, `get_sync_mappings(id)`,
  `list_destinations()` (names per the real API). Reuse the retry/typed-error pattern
  from `FivetranClient`/`DbtClient`. New typed exceptions in `exceptions.py`
  (`ActivationsAuthError`, `ActivationsError`, ...).
- `extractors/activations_extractor.py` — `ActivationsExtractor.extract()` → raw
  payload `{extracted_at, source:"activations", syncs:[...], errors:[...]}`, capturing
  per-sync failures without aborting (same resilience pattern as the others).
- `normalizers/` — normalize each sync to a stable record:
  `{sync_id, name, status, paused, source_ref (schema/table or dbt model unique_id),
    destination (service/name), destination_object, field_mappings:[{source_column,
    destination_field, is_primary_identifier}], last_synced_at}`.
- Add `activations` to the normalized document `sources` section
  (`sources.activations` alongside `fivetran`/`dbt`), and thread through the pipeline
  (extract only when configured; gracefully skip otherwise, like the warehouse reader).

## Feature 2 — DQ activation gate

- `dq/activation_gate.py` — pure function
  `activation_readiness(warehouse_object, *, policy=DEFAULT_POLICY) -> dict` returning
  `{verdict: "allow"|"warn"|"block", reasons:[...], blocking_signals:[...]}`.
  Default policy (make it a small, documented, overridable dict):
    - **block**: failing dbt tests; stale Fivetran sync; unmatched/`missing_dbt_coverage`
      when the object is actually activated.
    - **warn**: no enforced contract; missing PK; freshness only `warn`; metric the
      object feeds is `at_risk`/`watch`; heuristic-only test coverage.
    - **allow**: none of the above.
  Keep it deterministic and reason-annotated (each verdict lists the exact signals).
- This reads signals already on the warehouse object (`dq_summary`, `dbt.tests`,
  `dbt.freshness`, `origin`, `dbt.governance`, `dbt.metrics`) + `metric_quality`. It
  does NOT require the Activations ingestion to function (a gate can be requested for
  any object), but when ingestion is present, gate results should be surfaced on
  objects that are actually activated.

## Feature 3 — Activation lineage / operational blast radius

- In `combined_normalizer.py`, attach `dbt.activations` (or top-level per object) =
  the activation syncs whose source resolves to this object's matched model/source,
  with `{sync_id, name, destination, destination_object, mapped_fields}`. Match a sync's
  source_ref to a warehouse object via the same model/source resolution already used
  for exposures/metrics (by dbt model unique_id or schema+table).
- Add a risk in `dq/recommendations.py`: `activates_bad_data` (severity **high**) when
  an object is **activated** AND the gate verdict is `block` (a DQ problem is being
  written to a production system). Include the destination(s) in `details`.
- Extend `mcp/tools.py:get_impact` and `get_column_impact` to include the operational
  writes (activation destinations + mapped fields) in the blast radius, so
  "if this column is wrong" now reaches `Salesforce.Account.<field>`.

## MCP tools + REST + CLI

- MCP (`mcp/server.py` + `tools.py`), narrow and task-focused:
  - `get_activation_readiness(schema, table)` → verdict + reasons.
  - `list_activations()` → compact list of syncs with destination + readiness verdict.
  - `get_activation(sync_id)` (or by schema/table) → full detail incl. mappings.
  - Fold activation destinations into `get_impact` / `get_column_impact` output.
- Extend `get_dq_summary` with an activations rollup: counts of activated objects by
  verdict (allow/warn/block).
- CLI: `metadata-service activations extract` (raw dump), and ensure `build` ingests
  activations when configured (add `--no-activations` opt-out, mirroring
  `--no-warehouse-metadata`).
- REST (`api/routes.py`): `GET /activations`, `GET /dq/activation-readiness?schema=&table=`.

## Phase 5 — Demo dataset (customer churn → CRM)

Reverse ETL's canonical shape is warehouse → CRM/ops; the existing GitHub demo has no
natural activation target, so add a customer-churn example that reuses data already
replicated in this account (Postgres finserv/retail: `fpr_records`/`rdp_records` with
customer fields in Snowflake `ERICC_TEST_DB`).

Prerequisites (the human will provide / confirm — pause and ask if missing):
- A dbt project (extend `github-dq-dbt` or a new `customer-dq-dbt`) with staging over
  the customer tables and a `customer_churn` model exposing `customer_id`,
  `churn_score`, `lifecycle_stage`, with tests (not_null/unique on `customer_id`,
  `accepted_values` on `lifecycle_stage`, a freshness source) and ideally a contract.
- A Fivetran **Activations** sync: source = the `customer_churn` model, destination =
  a CRM (Salesforce/HubSpot **sandbox**), mapping `customer_id` → external id and
  `churn_score`/`lifecycle_stage` → CRM fields. Confirm the workspace + destination
  exist; if not, ask the human to set up the sandbox destination (do not invent creds).

Then run `metadata-service build` scoped appropriately and demonstrate end-to-end:
- the churn object shows `dbt.activations` → the CRM destination + mapped fields,
- `get_activation_readiness("<schema>","customer_churn")` returns a reasoned verdict,
- `get_column_impact` on a churn input column reaches the CRM field,
- if a churn test is made to fail, `activates_bad_data` fires and the gate returns `block`.

If live Activations wiring is blocked (no sandbox/destination), fall back to the
fixtures (below) for validation and clearly document the live step as pending.

## Tests

- Add fixtures under `tests/fixtures/` modeled on the REAL Activations API shapes you
  discovered: `activations_syncs.json` (+ mappings). Wire them into the fixtures
  loader so `build --fixtures-dir` produces activation records offline.
- Unit tests:
  - Activations normalizer (sync → normalized record, mappings).
  - `activation_gate` verdicts (allow/warn/block) across signal combinations.
  - Combined: `dbt.activations` attached; `activates_bad_data` fires only when
    activated + block.
  - MCP tools (`get_activation_readiness`, `list_activations`), and blast-radius
    extension reaching a destination field.
  - Client behavior with a mocked transport (auth header, error mapping) — no live calls.
- All existing tests must still pass. Keep coverage of the offline `--fixtures-dir` path.

## Documentation

- Extend `docs/use-cases/github-snowflake-dbt.md` (or a new
  `docs/use-cases/churn-reverse-etl.md`) with the EL→T→gate→RETL narrative + example
  agent questions ("Is the churn score safe to push to Salesforce?", "If this column
  is wrong, which CRM fields are corrupted?").
- Update `README.md` (MCP tools table, JSON contract shape: add `sources.activations`,
  object `activations`, and the summary rollup), `ARTIFACTS.md` (new fields + the
  `activates_bad_data` risk + the readiness verdict shape), and the Capgemini
  quickstart (lifecycle diagram → add the RETL leg + gate).
- Append to `CHANGELOG.md`.

## Constraints

- Verify-before-build (probe the live Activations API and print real shapes first).
- Never commit secrets; `.env`, `*.pem`, keys stay gitignored. Read secrets from env.
- Activations is an OPTIONAL, gracefully-degrading feature: `build` must still work
  with it unconfigured (no calls, no errors). Never fail the build on activation errors.
- Deterministic, boring, well-typed code matching the existing modules' style.
- Commit in logical phases; keep the branch green (`uv run pytest`).

## Acceptance criteria

1. `uv run pytest` passes (existing + new), including the offline `--fixtures-dir` path.
2. With Activations configured, `metadata-service build` ingests syncs and attaches
   `activations` to the matched warehouse objects; unconfigured, it's a clean no-op.
3. `get_activation_readiness(schema, table)` returns a deterministic, reason-annotated
   `allow|warn|block` verdict.
4. An object that is activated AND fails the gate produces an `activates_bad_data`
   (high) risk; `get_impact`/`get_column_impact` include the destination field(s).
5. `get_dq_summary` reports activated-objects-by-verdict.
6. Docs (README, ARTIFACTS, use-case, quickstart, CHANGELOG) updated to match.
7. The customer-churn → CRM demo is demonstrated live, or documented as pending with
   fixtures proving the code path.

After implementation, print: files changed, how to run tests, how to run a build with
Activations, the live demo result (or the pending-setup note), and any assumptions/TODOs.
```
