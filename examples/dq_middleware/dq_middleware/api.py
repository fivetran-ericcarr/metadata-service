"""Decision API — the surface an orchestrator or platform calls.

    DQMW_SNAPSHOT_SOURCE=../../metadata_snapshots/latest.json \\
    DQMW_POLICY_PATH=policy.toml \\
    uvicorn dq_middleware.api:app --port 8900

Endpoints:
    GET /health
    GET /decisions                     full policy report
    GET /gate/deploy                   {"verdict": "pass"|"fail", "failed_rules": [...]}
    GET /gate/activations/{sync_ref}   allow/deny for one reverse-ETL sync
"""

from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException

from .engine import FAIL, Policy, evaluate, gate_activation
from .snapshot import SnapshotError, load_snapshot

app = FastAPI(title="DQ Policy Middleware (example)", version="0.1.0")


def _config() -> tuple[str, str, str | None]:
    return (os.environ.get("DQMW_SNAPSHOT_SOURCE", "./latest.json"),
            os.environ.get("DQMW_POLICY_PATH", "policy.toml"),
            os.environ.get("DQMW_METADATA_API_KEY"))


def _report() -> dict:
    source, policy_path, api_key = _config()
    try:
        snapshot = load_snapshot(source, api_key=api_key)
        policy = Policy.from_toml(policy_path)
    except (SnapshotError, OSError, KeyError) as exc:
        # A gate that cannot read its facts must not answer "pass".
        raise HTTPException(status_code=503, detail=f"Cannot evaluate: {exc}") from exc
    return evaluate(snapshot, policy)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/decisions")
def decisions() -> dict:
    return _report()


@app.get("/gate/deploy")
def gate_deploy() -> dict:
    report = _report()
    return {"verdict": report["verdict"],
            "failed_rules": [r["rule"] for r in report["results"] if r["status"] == FAIL],
            "snapshot_generated_at": report["snapshot_generated_at"]}


@app.get("/gate/activations/{sync_ref}")
def gate_activation_endpoint(sync_ref: str) -> dict:
    source, policy_path, api_key = _config()
    try:
        snapshot = load_snapshot(source, api_key=api_key)
        policy = Policy.from_toml(policy_path)
    except (SnapshotError, OSError, KeyError) as exc:
        raise HTTPException(status_code=503, detail=f"Cannot evaluate: {exc}") from exc
    return gate_activation(snapshot, policy, sync_ref)
