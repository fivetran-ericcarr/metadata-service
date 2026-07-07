# DQ Policy Middleware (example consumer)

A toy — functionally complete, deliberately small — **policy middleware** that
consumes the metadata-service snapshot contract and turns its *facts* into
*organizational decisions*. This is the layer a DQaaS platform (the "Capgemini
box" in the architecture diagram) builds between the metadata-service and its
CI/orchestration:

```text
metadata-service            THIS EXAMPLE                     consumers
  latest.json     ──►   policy engine + waivers   ──►   CI gate (exit code)
  (facts:               (decisions: pass/fail,          orchestrator pre-flight
   tests, risks,         allow/deny, waived-with-       (allow/deny per sync)
   gate verdicts,        attribution)                   decision API (JSON)
   activations)
```

Zero dependencies beyond what the metadata-service repo already ships
(`tomllib` is stdlib; `httpx`/`fastapi`/`typer` come with the service).

## What it demonstrates

| Concern | Where | Contract fields consumed |
|---|---|---|
| Loading the snapshot (file or REST + `X-API-Key`), schema-version guard | `dq_middleware/snapshot.py` | top-level `version`, `warehouse_objects` |
| Declarative policy + rules | `policy.toml`, `dq_middleware/engine.py` | `dq_summary.*` (incl. `warn_tests_with_failures_count`), `match_confidence`, `dq_recommendations[].risk/signal`, `activations.syncs[].readiness` |
| **Fail-closed gating** | `engine.py` | stale snapshot → FAIL; unknown sync → deny; can't read facts → HTTP 503, never "pass" |
| **Waivers**: explicit, targeted, attributed, expiring | `engine.py` | expired waivers stop working and are reported as `[STALE]` |
| CI gate (exit codes) | `dq_middleware/cli.py` | `evaluate` → 0/1, `gate-activation <sync>` → 0/1 |
| Decision API for orchestrators | `dq_middleware/api.py` | `/gate/deploy`, `/gate/activations/{sync}`, `/decisions` |

## Run it (offline, no credentials)

```bash
# from the metadata-service repo root
uv run metadata-service build --fixtures-dir tests/fixtures --write-latest   # seed a snapshot
uv run python examples/dq_middleware/run.py evaluate \
    --snapshot metadata_snapshots/latest.json \
    --policy examples/dq_middleware/policy.toml
```

The fixture data ships known defects on purpose, so the gate **fails loudly**:

```text
policy: example-prod-gate   snapshot: 2026-...
  [PASS ] snapshot_freshness
  [FAIL ] no_failing_tests
          - salesforce.account: 0 failing test(s), 1 warn-severity test(s) firing.
  [FAIL ] max_high_risk_objects
          - salesforce.account: risk_level=high
  [PASS ] min_dbt_coverage
  [FAIL ] no_stale_objects
          - salesforce.account: Last successful Fivetran sync is older than 24h.
  [FAIL ] activation_gate
          - 900: dim_account -> Salesforce Account ... verdict=block. ...
  [  -  ] no_unwaived_pii
verdict: FAIL          # exit code 1 -> the CI job stops here
```

Pre-flight a single reverse-ETL sync (what an orchestrator calls before
triggering it):

```bash
uv run python examples/dq_middleware/run.py gate-activation dim_account \
    --snapshot metadata_snapshots/latest.json --policy examples/dq_middleware/policy.toml
# {"decision": "deny", "verdict": "block", "reasons": ["1 upstream warn-severity test(s) ..."]}
```

To accept a known exception, add a waiver to `policy.toml` — attributed and
expiring, so it cleans itself up:

```toml
[[waivers]]
rule = "activation_gate"
target = "900"
reason = "JIRA-123: dupes fixed upstream, backfill lands Friday"
expires = 2026-08-01
```

## Serve the decision API

```bash
cd examples/dq_middleware
DQMW_SNAPSHOT_SOURCE=../../metadata_snapshots/latest.json \
DQMW_POLICY_PATH=policy.toml \
uv run --project ../.. uvicorn dq_middleware.api:app --port 8900

curl -s localhost:8900/gate/deploy               # {"verdict": "fail", "failed_rules": [...]}
curl -s localhost:8900/gate/activations/900      # {"decision": "deny", ...}
```

`DQMW_SNAPSHOT_SOURCE` can also be the metadata-service base URL
(`http://127.0.0.1:8080`) with `DQMW_METADATA_API_KEY` set — the middleware then
pulls `/metadata/latest` live instead of reading the file.

## Tests

```bash
uv run pytest examples/dq_middleware/tests -q     # also run by the main suite
```

The tests build a real snapshot from the repo's fixtures through the actual
pipeline, so they double as a **consumer-side contract guard**: if the snapshot
shape drifts, this example fails before a real integration would.

## What a real implementation would add

Notification/ticketing sinks, per-team policy resolution, decision persistence
and audit history, OPA/rego-style policy composition, and a remediation
workqueue. The contract consumption pattern — load, verify version, evaluate,
fail closed — stays the same.
