"""End-to-end tests for the example middleware over a real fixture snapshot.

The fixture data ships known defects on purpose (a warn-firing test, a stale
sync, a blocked activation), so the default policy MUST fail — that's the demo.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from dq_middleware.engine import FAIL, PASS, WAIVED, Policy, Waiver, evaluate, gate_activation
from dq_middleware.snapshot import SnapshotError, load_snapshot


def _now(snapshot) -> datetime:
    # Evaluate "just after" the snapshot so snapshot_freshness passes and the
    # report exercises the interesting rules.
    gen = datetime.fromisoformat(snapshot["generated_at"].replace("Z", "+00:00"))
    return gen + timedelta(minutes=5)


def _result(report, rule):
    return next(r for r in report["results"] if r["rule"] == rule)


# -- the demo path -------------------------------------------------------------
def test_default_policy_fails_on_fixture_defects(snapshot, policy_path):
    report = evaluate(snapshot, Policy.from_toml(policy_path), now=_now(snapshot))
    assert report["verdict"] == FAIL

    # warn-firing test on salesforce.account is counted (the reverse-ETL trap)
    failing = _result(report, "no_failing_tests")
    assert failing["status"] == FAIL
    assert any(e["target"] == "salesforce.account" for e in failing["evidence"])

    # the blocked activation (dim_account -> Salesforce) fails the gate rule
    gate = _result(report, "activation_gate")
    assert gate["status"] == FAIL
    assert any(e["target"] == "900" for e in gate["evidence"])

    # the stale Fivetran sync is surfaced
    assert _result(report, "no_stale_objects")["status"] == FAIL


def test_waiver_converts_fail_to_waived(snapshot, policy_path):
    policy = Policy.from_toml(policy_path)
    policy.waivers = [
        Waiver(rule="activation_gate", target="900", reason="JIRA-1 fix landing",
               expires=_now(snapshot).date() + timedelta(days=7)),
    ]
    report = evaluate(snapshot, policy, now=_now(snapshot))
    gate = _result(report, "activation_gate")
    assert gate["status"] == WAIVED
    assert gate["waived"][0]["waiver_reason"] == "JIRA-1 fix landing"
    # other rules still fail, so the overall verdict stays FAIL
    assert report["verdict"] == FAIL


def test_expired_waiver_does_not_apply_and_is_reported(snapshot, policy_path):
    policy = Policy.from_toml(policy_path)
    policy.waivers = [
        Waiver(rule="activation_gate", target="900", reason="lapsed",
               expires=_now(snapshot).date() - timedelta(days=1)),
    ]
    report = evaluate(snapshot, policy, now=_now(snapshot))
    assert _result(report, "activation_gate")["status"] == FAIL  # waiver lapsed
    assert report["expired_waivers"] and report["expired_waivers"][0]["target"] == "900"


def test_snapshot_freshness_fails_closed_on_old_metadata(snapshot, policy_path):
    report = evaluate(snapshot, Policy.from_toml(policy_path),
                      now=_now(snapshot) + timedelta(days=3))
    fresh = _result(report, "snapshot_freshness")
    assert fresh["status"] == FAIL
    assert "refresh before gating" in fresh["evidence"][0]["detail"]


# -- activation pre-flight -------------------------------------------------------
def test_gate_activation_denies_blocked_sync_by_id_and_table(snapshot, policy_path):
    policy = Policy.from_toml(policy_path)
    by_id = gate_activation(snapshot, policy, "900")
    assert by_id["decision"] == "deny" and by_id["verdict"] == "block"
    assert by_id["reasons"]  # carries the gate's why
    by_table = gate_activation(snapshot, policy, "dim_account")
    assert by_table["decision"] == "deny"


def test_gate_activation_fails_closed_on_unknown_sync(snapshot, policy_path):
    decision = gate_activation(snapshot, Policy.from_toml(policy_path), "no_such_sync")
    assert decision["decision"] == "deny"
    assert "fail closed" in decision["reason"]


def test_gate_activation_waiver_allows_with_attribution(snapshot, policy_path):
    policy = Policy.from_toml(policy_path)
    policy.waivers = [Waiver(rule="activation_gate", target="900",
                             reason="JIRA-2", expires=None)]
    decision = gate_activation(snapshot, policy, "900", now=_now(snapshot))
    assert decision == {"decision": "allow", "sync": "900", "verdict": "block",
                        "sync_id": 900, "waived": True, "waiver_reason": "JIRA-2"}


# -- snapshot loader (contract guards) --------------------------------------------
def test_loader_rejects_wrong_schema_version(tmp_path):
    bad = tmp_path / "latest.json"
    bad.write_text('{"version": "9.9", "warehouse_objects": []}')
    with pytest.raises(SnapshotError, match="schema version"):
        load_snapshot(str(bad))


def test_loader_rejects_non_snapshot_payload(tmp_path):
    bad = tmp_path / "latest.json"
    bad.write_text('{"hello": "world"}')
    with pytest.raises(SnapshotError, match="warehouse_objects"):
        load_snapshot(str(bad))


# -- surfaces -----------------------------------------------------------------------
def test_decision_api(snapshot_path, policy_path, monkeypatch):
    from fastapi.testclient import TestClient

    monkeypatch.setenv("DQMW_SNAPSHOT_SOURCE", str(snapshot_path))
    monkeypatch.setenv("DQMW_POLICY_PATH", str(policy_path))
    from dq_middleware.api import app

    client = TestClient(app)
    assert client.get("/health").json() == {"status": "ok"}

    deploy = client.get("/gate/deploy").json()
    assert deploy["verdict"] == FAIL  # fixture defects
    assert "activation_gate" in deploy["failed_rules"]

    act = client.get("/gate/activations/900").json()
    assert act["decision"] == "deny" and act["verdict"] == "block"

    decisions = client.get("/decisions").json()
    assert {r["rule"] for r in decisions["results"]} >= {"no_failing_tests", "activation_gate"}

    # a gate that cannot read its facts answers 503, never "pass"
    monkeypatch.setenv("DQMW_SNAPSHOT_SOURCE", "/nonexistent/latest.json")
    assert client.get("/gate/deploy").status_code == 503


def test_cli_exit_codes(snapshot_path, policy_path):
    from typer.testing import CliRunner

    from dq_middleware.cli import app

    runner = CliRunner()
    result = runner.invoke(app, ["evaluate", "--snapshot", str(snapshot_path),
                                 "--policy", str(policy_path)])
    assert result.exit_code == 1  # fixture defects -> gate fails
    assert "verdict: FAIL" in result.output

    result = runner.invoke(app, ["gate-activation", "900", "--snapshot", str(snapshot_path),
                                 "--policy", str(policy_path)])
    assert result.exit_code == 1
    assert '"deny"' in result.output
