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

import logging
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException

from .engine import FAIL, Policy, PolicyError, evaluate, gate_activation
from .snapshot import SnapshotError, load_snapshot

logger = logging.getLogger(__name__)
app = FastAPI(title="DQ Policy Middleware (example)", version="0.1.0")

# Anchor the policy default to the example directory so launching from any cwd
# works; the snapshot has no in-repo default location, so it stays cwd-relative.
_DEFAULT_POLICY = str(Path(__file__).resolve().parents[1] / "policy.toml")


def _load_facts() -> tuple[dict, Policy]:
    """Load snapshot + policy, or answer 503 — a gate that cannot read its
    facts must not answer \"pass\". The client gets a generic message; the
    specifics (paths, URLs) stay in the server log."""
    source = os.environ.get("DQMW_SNAPSHOT_SOURCE", "./latest.json")
    policy_path = os.environ.get("DQMW_POLICY_PATH", _DEFAULT_POLICY)
    api_key = os.environ.get("DQMW_METADATA_API_KEY")
    try:
        return load_snapshot(source, api_key=api_key), Policy.from_toml(policy_path)
    except (SnapshotError, PolicyError, OSError) as exc:
        logger.error("Cannot evaluate: %s", exc)
        raise HTTPException(status_code=503,
                            detail="Cannot evaluate: snapshot or policy unavailable "
                                   "(see server log).") from exc


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/decisions")
def decisions() -> dict:
    snapshot, policy = _load_facts()
    return evaluate(snapshot, policy)


@app.get("/gate/deploy")
def gate_deploy() -> dict:
    snapshot, policy = _load_facts()
    report = evaluate(snapshot, policy)
    return {"verdict": report["verdict"],
            "failed_rules": [r["rule"] for r in report["results"] if r["status"] == FAIL],
            "snapshot_generated_at": report["snapshot_generated_at"]}


@app.get("/gate/activations/{sync_ref}")
def gate_activation_endpoint(sync_ref: str) -> dict:
    snapshot, policy = _load_facts()
    return gate_activation(snapshot, policy, sync_ref)
