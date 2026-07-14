"""End-to-end tests for the example middleware over a real fixture snapshot.

The fixture data ships known defects on purpose (a warn-firing test, a stale
sync, a blocked activation), so the default policy MUST fail — that's the demo.
"""

from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone

import pytest

from dq_middleware.engine import (
    FAIL,
    PASS,
    WAIVED,
    Policy,
    PolicyError,
    Waiver,
    evaluate,
    gate_activation,
)
from dq_middleware.snapshot import EXPECTED_VERSION, SnapshotError, load_snapshot


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

    # unreadable facts are a distinct exit code (2), not a fake FAIL
    result = runner.invoke(app, ["evaluate", "--snapshot", "/nonexistent/latest.json",
                                 "--policy", str(policy_path)])
    assert result.exit_code == 2


# -- policy validation (a config mistake must not silently change the gate) -------
@pytest.mark.parametrize("toml_text, message", [
    ('[rules.no_failing_test]\nenabled = true\n', "Unknown rule"),
    ('[rules.no_failing_tests]\nenabled = true\ninclude_warn = true\n', "Unknown option"),
    ('[rules.max_high_risk_objects]\nenabled = true\nschemas = "retail"\n',
     "list of schema names"),
    ('[rules.snapshot_freshness]\nenabled = true\nmax_age_hours = "one day"\n',
     "number of hours"),
    ('[rules]\nno_failing_tests = true\n', "must be a table"),
    ('[rules.no_failing_tests]\nenabled = "yes"\n', "true or false"),
    ('[rules.no_failing_tests]\nenabled = false\n', "enables no rules"),
    ('[rules.no_failing_tests]\nenabled = true\n'
     '[[waivers]]\nrule = "activation_gate"\ntarget = "900"\nexpires = "2026-08-01"\n',
     "unquoted TOML date"),
    ('[rules.no_failing_tests]\nenabled = true\n'
     '[[waivers]]\nrule = "no_such_rule"\ntarget = "*"\n', "not a known rule"),
    ('rules = [unclosed\n', "Invalid TOML"),
])
def test_policy_validation_fails_closed(tmp_path, toml_text, message):
    bad = tmp_path / "policy.toml"
    bad.write_text(toml_text)
    with pytest.raises(PolicyError, match=message):
        Policy.from_toml(bad)


def test_default_policy_toml_is_valid(policy_path):
    assert Policy.from_toml(policy_path).rules  # the shipped example passes its own validation


# -- freshness hardening ------------------------------------------------------------
def test_freshness_accepts_naive_timestamp_as_utc(policy_path):
    doc = {"generated_at": "2026-06-25T12:34:56", "warehouse_objects": []}  # no tz
    report = evaluate(doc, Policy.from_toml(policy_path),
                      now=datetime(2026, 6, 25, 13, 0, tzinfo=timezone.utc))
    assert _result(report, "snapshot_freshness")["status"] == PASS  # 25min old, no crash


def test_freshness_rejects_future_timestamp(policy_path):
    doc = {"generated_at": "2026-08-01T00:00:00Z", "warehouse_objects": []}
    report = evaluate(doc, Policy.from_toml(policy_path),
                      now=datetime(2026, 7, 1, tzinfo=timezone.utc))
    fresh = _result(report, "snapshot_freshness")
    assert fresh["status"] == FAIL
    assert "future" in fresh["evidence"][0]["detail"]


# -- waiver/threshold semantics ------------------------------------------------------
def _high_risk_doc(*names):
    return {"generated_at": "2026-07-01T00:00:00Z",
            "warehouse_objects": [{"schema": "s", "name": n,
                                   "dq_summary": {"risk_level": "high"}} for n in names]}


def test_threshold_counts_only_unwaived_objects():
    now = datetime(2026, 7, 1, 1, 0, tzinfo=timezone.utc)
    rules = {"max_high_risk_objects": {"enabled": True, "threshold": 1}}
    doc = _high_risk_doc("a", "b")

    # 2 unwaived > threshold 1 -> FAIL
    assert _result(evaluate(doc, Policy(rules=rules), now=now),
                   "max_high_risk_objects")["status"] == FAIL

    # 1 waived + 1 tolerated by the threshold -> not a failure, waiver consumed
    policy = Policy(rules=rules,
                    waivers=[Waiver(rule="max_high_risk_objects", target="s.a", reason="JIRA-3")])
    report = evaluate(doc, policy, now=now)
    assert _result(report, "max_high_risk_objects")["status"] == WAIVED
    assert not report["unused_waivers"]  # the waiver did real work; don't tell anyone to delete it


def test_targeted_waiver_beats_wildcard(snapshot, policy_path):
    policy = Policy.from_toml(policy_path)
    policy.waivers = [Waiver(rule="activation_gate", target="*", reason="broad freeze"),
                      Waiver(rule="activation_gate", target="900", reason="JIRA-9")]
    report = evaluate(snapshot, policy, now=_now(snapshot))
    gate = _result(report, "activation_gate")
    assert gate["waived"][0]["waiver_reason"] == "JIRA-9"  # attribution stays targeted
    assert not any(u["target"] == "900" for u in report["unused_waivers"])


# -- activation pre-flight hardening ---------------------------------------------------
def test_gate_activation_denies_on_stale_snapshot(snapshot, policy_path):
    decision = gate_activation(snapshot, Policy.from_toml(policy_path), "900",
                               now=_now(snapshot) + timedelta(days=3))
    assert decision["decision"] == "deny"
    assert "fail closed" in decision["reason"]


def test_gate_activation_disabled_rule_stands_down(snapshot):
    policy = Policy(rules={"activation_gate": {"enabled": False}})
    decision = gate_activation(snapshot, policy, "900")
    assert decision["decision"] == "allow"
    assert "disabled" in decision["note"]


def test_gate_activation_waiver_matches_source_table(snapshot, policy_path):
    policy = Policy.from_toml(policy_path)
    policy.waivers = [Waiver(rule="activation_gate", target="dim_account", reason="JIRA-4")]
    decision = gate_activation(snapshot, policy, "900", now=_now(snapshot))
    assert decision["decision"] == "allow" and decision["waived"]


def test_gate_activation_reports_expired_waiver_on_deny(snapshot, policy_path):
    policy = Policy.from_toml(policy_path)
    policy.waivers = [Waiver(rule="activation_gate", target="900", reason="lapsed",
                             expires=_now(snapshot).date() - timedelta(days=1))]
    decision = gate_activation(snapshot, policy, "900", now=_now(snapshot))
    assert decision["decision"] == "deny"
    assert decision["expired_waiver"]["reason"] == "lapsed"


def test_gate_activation_denies_ambiguous_table_reference(snapshot, policy_path):
    syncs = snapshot["activations"]["syncs"]
    original = next(s for s in syncs
                    if (s.get("source_object") or {}).get("table_name") == "dim_account")
    clone = copy.deepcopy(original)
    clone["sync_id"] = 999901
    doc = {**snapshot,
           "activations": {**snapshot["activations"], "syncs": [*syncs, clone]}}
    decision = gate_activation(doc, Policy.from_toml(policy_path), "dim_account",
                               now=_now(snapshot))
    assert decision["decision"] == "deny"
    assert "Ambiguous" in decision["reason"]


# -- loader hardening --------------------------------------------------------------
def test_loader_wraps_non_json_http_body(monkeypatch):
    import httpx

    def fake_get(url, **kwargs):
        return httpx.Response(200, text="<html>gateway</html>",
                              request=httpx.Request("GET", url))

    monkeypatch.setattr("dq_middleware.snapshot.httpx.get", fake_get)
    with pytest.raises(SnapshotError, match="non-JSON"):
        load_snapshot("http://127.0.0.1:9/metadata/latest")


def test_expected_version_matches_service_schema_version():
    from metadata_service.models.common import SCHEMA_VERSION

    assert EXPECTED_VERSION == SCHEMA_VERSION, (
        "The metadata-service snapshot SCHEMA_VERSION changed: update the example's "
        "EXPECTED_VERSION and re-verify the contract fields its rules consume."
    )
