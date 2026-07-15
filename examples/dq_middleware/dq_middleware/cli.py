"""CI-gate CLI: exit 0 = safe to proceed, exit 1 = policy failed,
exit 2 = facts or policy unreadable (fail closed — treat as not-safe).

    python run.py evaluate --snapshot ../../metadata_snapshots/latest.json
    python run.py gate-activation customer_churn --snapshot http://127.0.0.1:8080
"""

from __future__ import annotations

import json
from pathlib import Path

import typer

from .engine import FAIL, PASS, SKIPPED, WAIVED, Policy, PolicyError, evaluate, gate_activation
from .snapshot import SnapshotError, load_snapshot

app = typer.Typer(help="Toy DQ policy middleware over metadata-service snapshots.",
                  no_args_is_help=True)

_STATUS_MARK = {PASS: "PASS ", FAIL: "FAIL ", WAIVED: "WAIVE", SKIPPED: "  -  "}
# Anchored to the example dir so the documented invocations work from any cwd.
_DEFAULT_POLICY = str(Path(__file__).resolve().parents[1] / "policy.toml")
_API_KEY_ENVVARS = ["DQMW_METADATA_API_KEY", "METADATA_API_KEY"]


def _load(snapshot: str, policy: str, api_key: str | None) -> tuple[dict, Policy]:
    try:
        return load_snapshot(snapshot, api_key=api_key), Policy.from_toml(policy)
    except (SnapshotError, PolicyError, OSError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=2) from exc


@app.command("evaluate")
def evaluate_cmd(
    snapshot: str = typer.Option("./latest.json", "--snapshot",
                                 help="Snapshot file path, or metadata-service base URL."),
    policy: str = typer.Option(_DEFAULT_POLICY, "--policy",
                               show_default="policy.toml beside the example"),
    api_key: str | None = typer.Option(None, "--api-key", envvar=_API_KEY_ENVVARS),
    as_json: bool = typer.Option(False, "--json", help="Emit the full report as JSON."),
) -> None:
    """Evaluate the policy against the snapshot.
    Exit 0 on pass, 1 on fail, 2 if the snapshot or policy can't be read."""
    doc, pol = _load(snapshot, policy, api_key)
    report = evaluate(doc, pol)

    if as_json:
        typer.echo(json.dumps(report, indent=2, default=str))
    else:
        typer.echo(f"policy: {report['policy']}   snapshot: {report['snapshot_generated_at']}")
        for r in report["results"]:
            typer.echo(f"  [{_STATUS_MARK[r['status']]}] {r['rule']}")
            for e in r["evidence"][:5]:
                typer.echo(f"          - {e['target']}: {e['detail']}")
            if len(r["evidence"]) > 5:
                typer.echo(f"          ... and {len(r['evidence']) - 5} more")
            for w in r["waived"]:
                typer.echo(f"          ~ waived {w['target']}: {w['waiver_reason']}")
        for w in report["expired_waivers"]:
            typer.echo(f"  [STALE] expired waiver {w['rule']}/{w['target']} — remove or renew it")
        typer.echo(f"verdict: {report['verdict'].upper()}")
    raise typer.Exit(code=0 if report["verdict"] == PASS else 1)


@app.command("gate-activation")
def gate_activation_cmd(
    sync: str = typer.Argument(..., help="Sync id or the source table name it reads."),
    snapshot: str = typer.Option("./latest.json", "--snapshot"),
    policy: str = typer.Option(_DEFAULT_POLICY, "--policy",
                               show_default="policy.toml beside the example"),
    api_key: str | None = typer.Option(None, "--api-key", envvar=_API_KEY_ENVVARS),
) -> None:
    """Pre-flight one reverse-ETL sync.
    Exit 0 = allow, 1 = deny, 2 if the snapshot or policy can't be read (fail closed)."""
    doc, pol = _load(snapshot, policy, api_key)
    decision = gate_activation(doc, pol, sync)
    typer.echo(json.dumps(decision, indent=2, default=str))
    raise typer.Exit(code=0 if decision["decision"] == "allow" else 1)
