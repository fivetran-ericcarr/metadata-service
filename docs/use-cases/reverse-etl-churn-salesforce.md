# Use Case: Retail (Postgres) → dbt churn model → Fivetran Activation → Salesforce

A complete, live end-to-end reference build proving the metadata-service's
**reverse-ETL activation readiness gate** — the "is it safe to push this data
back into a system of record?" check. It answers a question exposures and metric
trust can't: a bad dashboard is embarrassing; **bad data written back into
Salesforce corrupts the operational source of truth.**

## Summary

| | |
|---|---|
| **Source** | Retail Postgres (customers, orders, tickets) replicated by Fivetran |
| **Replication** | Fivetran Postgres connector `google_cloud_postgresql` → Snowflake (`ERICC_TEST_DB.retail`) |
| **Transformation** | dbt project [`github-dq-dbt`](https://github.com/fivetran-ericcarr/github-dq-dbt) (project 467825): retail staging models + a `customer_churn` mart (enforced contract) + DQ tests |
| **Activation** | Fivetran Activation (Census) sync `3580269`: `customer_churn` → **Salesforce Contact** |
| **Gate result** | `get_activation_readiness` → **`block`** — an upstream `severity: warn` uniqueness test is firing on **143 duplicate rows** |

```text
Retail Postgres  (customers, orders, tickets)
        │  Fivetran Postgres connector  (google_cloud_postgresql)
        ▼
Snowflake  ERICC_TEST_DB.retail  ── raw tables (lowercase-quoted identifiers)
        │  dbt (Snowflake key-pair, deployment job)
        ▼
DBT_ERICC_staging  stg_retail__ret_customers / __orders / __tickets / __customers
        │            (stg_retail__customers.customer_id  unique test = severity: warn)
        ▼
DBT_ERICC_marts  customer_churn  ── enforced contract; depends on stg_retail__customers
        │  Fivetran Activation (Census)  → Salesforce
        ▼
Salesforce Contact   Email · LastName · Churn_Score__c · Lifecycle_Stage__c
        │  metadata-service build (Activations API + dbt lineage)
        ▼
activations.readiness = BLOCK   (soft warn test firing on data headed to prod)
```

## Components provisioned

| Layer | Object | How |
|---|---|---|
| Fivetran | Postgres connector `google_cloud_postgresql` (Sales_Demo_Sandbox account) → `ERICC_TEST_DB.retail` | Fivetran UI |
| Snowflake | destination `ERICC_TEST_DB`, schema `retail` (lowercase-quoted identifiers) | existing Fivetran destination |
| dbt Cloud | retail staging models + `customer_churn` mart in project 467825 (`github-dq-dbt` repo) | added to the same repo/job as the GitHub use-case |
| Activations (Census) | Source `sf_postgres_retail_dq_test`, Destination `sf_dq_activations_ericc` (Salesforce), sync `3580269` | Census workspace tied to Sales_Demo_Sandbox |
| Salesforce | custom Contact fields `Churn_Score__c`, `Lifecycle_Stage__c` | created in a dev SFDC org |

The churn mart is engineered so the demo has a **real** upstream defect on the
path to production:

- `stg_retail__customers` carries a `unique` test on `customer_id` set to
  **`severity: warn`** — a soft test, so a failure leaves the dbt run green.
- `customer_churn` **depends on `stg_retail__customers`** (via a `region_features`
  CTE that joins region-level customer counts onto the churn spine), so the
  warn test sits directly upstream of the activation's source model.
- The source data contains **143 duplicate `customer_id` rows**, so that soft test
  is actually *firing* — the exact situation a status-only check would wave through.

## Reproduce

```bash
# 0. .env: FIVETRAN_API_KEY/SECRET (Sales_Demo_Sandbox), DBT_ACCOUNT_ID=3643,
#    DBT_SERVICE_TOKEN, ACTIVATIONS_API_TOKEN=<Census workspace token>,
#    WAREHOUSE_DATABASE=ERICC_TEST_DB  (scopes Activations to syncs reading this DB)

# 1. Fivetran: sync the retail Postgres connector into ERICC_TEST_DB.retail.
#    Postgres stores identifiers lowercase-quoted, so the dbt source needs
#    quoting: {schema: true, identifier: true} and quoted column refs
#    (e.g. "id" as customer_id).  See "Lessons learned" below.

# 2. dbt Cloud (project 467825): build the retail staging + customer_churn mart.
#    stg_retail__customers.customer_id unique test is severity: warn.
#    customer_churn has an enforced contract (owner + group + access: public).
#    Run the deployment job so run_results/manifest/catalog are published.

# 3. Census: create the Activation source (sf_postgres_retail_dq_test) + Salesforce
#    destination (sf_dq_activations_ericc); sync customer_churn -> Contact with:
#      EMAIL           -> Email  (primary identifier)
#      CUSTOMER_NAME   -> LastName
#      CHURN_SCORE     -> Churn_Score__c
#      LIFECYCLE_STAGE -> Lifecycle_Stage__c

# 4. metadata-service: build with Activations enabled.
metadata-service build --dbt-project-id 467825          # activations auto-included when the token is set
metadata-service serve-mcp                              # or query the snapshot directly
```

## Results

`get_activation_readiness(sync_id=3580269)` on the live build:

```json
{
  "sync_id": 3580269,
  "label": "customer_churn -> Salesforce Contact (DQ demo)",
  "paused": true,
  "source_object": { "table_catalog": "ERICC_TEST_DB", "table_schema": "DBT_ERICC_MARTS", "table_name": "CUSTOMER_CHURN" },
  "destination_name": "sf_dq_activations_ericc",
  "destination_type": "salesforce",
  "destination_object": "Contact",
  "mappings": [
    { "source_column": "EMAIL", "destination_field": "Email", "is_primary_identifier": true },
    { "source_column": "CUSTOMER_NAME", "destination_field": "LastName", "is_primary_identifier": false },
    { "source_column": "CHURN_SCORE", "destination_field": "Churn_Score__c", "is_primary_identifier": false },
    { "source_column": "LIFECYCLE_STAGE", "destination_field": "Lifecycle_Stage__c", "is_primary_identifier": false }
  ],
  "readiness": {
    "verdict": "block",
    "source_node_unique_id": "model.github_dq.customer_churn",
    "reasons": [
      { "code": "upstream_warn_test_failures", "severity": "high",
        "message": "1 upstream warn-severity test(s) have failing rows (soft test firing on data headed to prod)." }
    ],
    "upstream": { "node_count": 9, "failing_tests": 0, "warn_tests_with_failures": 1, "tests_seen": 18, "tests_with_results": 18,
                  "stale_objects": 0, "missing_contract": false, "unmatched_upstream": 0 }
  }
}
```

The gate resolved the sync's source object to `model.github_dq.customer_churn`,
walked its **9 upstream nodes** (5 models + 4 sources), and found the firing test:

| Upstream node | Test | Status | Severity | Failing rows |
|---|---|---|---|---|
| `stg_retail__customers` | `unique_stg_retail__customers_customer_id` | warn | **warn** | **143** |

Note `failing_tests: 0` — nothing *errored*, so the dbt run is green and a naive
status check passes. The gate's **warn-with-failures** rule is what turns this into
a **`block`**: a soft test that is actually firing on data headed to a system of
record is exactly what should stop a reverse-ETL push.

Because the churn object feeds a blocking activation, the build also raises an
**`activates_bad_data`** (high) risk, and `get_column_impact` maps the offending
`customer_id`/`churn_score` columns to the **Salesforce Contact fields** they would
have overwritten (`Churn_Score__c`, `Lifecycle_Stage__c`) — not a generic field.

## Example agent questions (on the demo data)

| Question | Tool(s) | Answer |
|---|---|---|
| **Is it safe to sync churn scores back to Salesforce?** | `get_activation_readiness(sync_id=3580269)` | **No — `block`.** A `severity: warn` uniqueness test on `stg_retail__customers.customer_id` (upstream of `customer_churn`) is firing on **143 duplicate rows**. Fix the duplicates, re-run the job, then re-check. |
| **What in Salesforce would this have corrupted?** | `get_column_impact(retail, customers, customer_id)` | The affected columns feed `customer_churn`, whose activation writes `Churn_Score__c` and `Lifecycle_Stage__c` on the Contact — those fields would receive values keyed off duplicated customers. |
| **Which objects push data to operational systems, and are any unsafe?** | `list_activations(verdict="block")` | The churn→Salesforce Contact sync — blocked. (Scope with `WAREHOUSE_DATABASE`; a shared Census workspace also returns syncs reading other sources, correctly reported as `unknown`.) |
| **Why did the gate block if all dbt tests passed?** | `get_activation_readiness` → `readiness.reasons` | The run passed because the uniqueness test is `severity: warn` (soft). The gate blocks on warn-with-failures precisely because status-only checks miss it. |

## Lessons learned (real findings from this build)

1. **Warn-severity tests are the reverse-ETL trap.** A `severity: warn` test with
   failing rows leaves the dbt run green, so any status-only gate syncs it straight
   to prod. The activation gate blocks on **warn-with-failures** for exactly this
   reason — the single most important rule in `dq/activation_gate.py`.
2. **Fivetran + Postgres store identifiers lowercase-quoted in Snowflake.** The dbt
   source needs `quoting: {schema: true, identifier: true}` **and** quoted column
   references (`"id" as customer_id`); unquoted refs raise "Database Error". (The
   GitHub SaaS connector, by contrast, is upper-case-resolvable.)
3. **Enforce a contract on the activation's source model.** `customer_churn` has an
   enforced contract, so the gate does *not* add a `source_model_no_contract` warning
   — schema changes can't silently reshape the Salesforce payload. A contract type
   mismatch (`churn_score` FIXED vs REAL, `last_order_at` TIMESTAMP_NTZ vs _TZ) is a
   compile error, so match the declared types to the warehouse.
4. **Scope Activations with `WAREHOUSE_DATABASE`.** The shared Census workspace
   returned 25 syncs; 24 read sources outside this project and are honestly reported
   as `unknown` (no lineage to assess). Scoping to `ERICC_TEST_DB` narrows to the one
   sync that matters.
5. **Census tests run on the unsaved draft.** A mapping can "test successfully" in the
   Census editor while the *saved* sync config still points at the old fields —
   the API (and therefore the gate) only sees the saved mappings. Click **Save** on
   the sync before relying on `get_activation_readiness`.

---

See the companion [GitHub → Snowflake → dbt use case](./github-snowflake-dbt.md)
for the upstream DQ, exposures, metric-trust, and column-lineage story, and the
[reverse-ETL build prompt](../prompts/reverse-etl-activations.md) for how this
feature was scoped and built. Full service docs: project [README](../../README.md).
