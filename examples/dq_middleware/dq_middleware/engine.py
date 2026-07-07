"""The policy engine: snapshot facts + a TOML policy -> a gate decision.

Rules read only documented snapshot-contract fields (ARTIFACTS.md). Each rule
produces PASS / FAIL / WAIVED with evidence rows; the report verdict is FAIL if
any rule fails. Waivers are explicit, targeted, attributed, and expiring —
an exception someone owns, not a silent suppression.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

PASS, FAIL, WAIVED, SKIPPED = "pass", "fail", "waived", "skipped"


# -- policy ----------------------------------------------------------------
@dataclass
class Waiver:
    rule: str
    target: str          # object "schema.table", a sync_id, or "*"
    reason: str
    expires: date | None = None

    def is_active(self, today: date) -> bool:
        return self.expires is None or today <= self.expires

    def matches(self, rule: str, target: str) -> bool:
        return self.rule == rule and (self.target == "*" or self.target.lower() == str(target).lower())


@dataclass
class Policy:
    name: str = "default"
    rules: dict = field(default_factory=dict)
    waivers: list[Waiver] = field(default_factory=list)

    @classmethod
    def from_toml(cls, path: str | Path) -> "Policy":
        raw = tomllib.loads(Path(path).read_text(encoding="utf-8"))
        waivers = []
        for w in raw.get("waivers", []):
            expires = w.get("expires")
            if isinstance(expires, datetime):
                expires = expires.date()
            waivers.append(Waiver(rule=w["rule"], target=str(w["target"]),
                                  reason=w.get("reason", ""), expires=expires))
        return cls(name=(raw.get("policy") or {}).get("name", "default"),
                   rules=raw.get("rules", {}), waivers=waivers)


# -- evaluation --------------------------------------------------------------
def evaluate(snapshot: dict, policy: Policy, *, now: datetime | None = None) -> dict:
    """Evaluate every enabled rule and return the decision report."""
    now = now or datetime.now(timezone.utc)
    today = now.date()
    results: list[dict] = []
    used_waivers: set[int] = set()

    def waive(rule: str, target: str) -> Waiver | None:
        for i, w in enumerate(policy.waivers):
            if w.matches(rule, target) and w.is_active(today):
                used_waivers.add(i)
                return w
        return None

    for rule_name, rule_fn in _RULES.items():
        cfg = policy.rules.get(rule_name, {})
        if not cfg.get("enabled", False):
            results.append({"rule": rule_name, "status": SKIPPED, "evidence": [], "waived": []})
            continue
        evidence = rule_fn(snapshot, cfg, now)
        failing, waived = [], []
        for item in evidence:
            w = waive(rule_name, item["target"])
            if w:
                waived.append({**item, "waiver_reason": w.reason,
                               "waiver_expires": str(w.expires) if w.expires else None})
            else:
                failing.append(item)
        status = FAIL if failing else (WAIVED if waived else PASS)
        results.append({"rule": rule_name, "status": status,
                        "evidence": failing, "waived": waived})

    stale_waivers = [
        {"rule": w.rule, "target": w.target, "reason": w.reason, "expires": str(w.expires)}
        for i, w in enumerate(policy.waivers)
        if i not in used_waivers and not w.is_active(today)
    ]
    unused_waivers = [
        {"rule": w.rule, "target": w.target, "reason": w.reason}
        for i, w in enumerate(policy.waivers)
        if i not in used_waivers and w.is_active(today)
    ]

    verdict = FAIL if any(r["status"] == FAIL for r in results) else PASS
    return {
        "policy": policy.name,
        "evaluated_at": now.isoformat(),
        "snapshot_generated_at": snapshot.get("generated_at"),
        "verdict": verdict,
        "results": results,
        "expired_waivers": stale_waivers,   # exceptions that lapsed — clean them up
        "unused_waivers": unused_waivers,   # exceptions nothing needed — reconsider them
    }


def gate_activation(snapshot: dict, policy: Policy, sync_ref: str,
                    *, now: datetime | None = None) -> dict:
    """Allow/deny one reverse-ETL sync — the pre-flight call an orchestrator
    makes before triggering it. Fails closed: an unknown sync is a deny."""
    now = now or datetime.now(timezone.utc)
    cfg = policy.rules.get("activation_gate", {})
    allowed = set(cfg.get("allowed_verdicts", ["allow"]))

    sync = None
    for s in snapshot.get("activations", {}).get("syncs", []):
        obj = s.get("source_object") or {}
        if str(s.get("sync_id")) == str(sync_ref) or \
                (obj.get("table_name") or "").lower() == str(sync_ref).lower():
            sync = s
            break
    if sync is None:
        return {"decision": "deny", "sync": sync_ref,
                "reason": "Sync not present in the snapshot — cannot verify readiness (fail closed)."}

    readiness = sync.get("readiness") or {}
    verdict = readiness.get("verdict", "unknown")
    if verdict in allowed:
        return {"decision": "allow", "sync": sync_ref, "verdict": verdict,
                "sync_id": sync.get("sync_id")}
    for w in policy.waivers:
        if w.matches("activation_gate", str(sync.get("sync_id"))) and w.is_active(now.date()):
            return {"decision": "allow", "sync": sync_ref, "verdict": verdict,
                    "sync_id": sync.get("sync_id"), "waived": True, "waiver_reason": w.reason}
    return {"decision": "deny", "sync": sync_ref, "verdict": verdict,
            "sync_id": sync.get("sync_id"),
            "reasons": [r.get("message") for r in readiness.get("reasons", [])]}


# -- rules ---------------------------------------------------------------------
# Each rule: (snapshot, cfg, now) -> list of evidence dicts, each with a "target"
# the waiver mechanism can address.
def _obj_target(o: dict) -> str:
    return f"{o.get('schema')}.{o.get('name')}"


def _rule_snapshot_freshness(snapshot: dict, cfg: dict, now: datetime) -> list[dict]:
    """A gate is only as good as its facts: refuse to decide on stale metadata."""
    max_age = float(cfg.get("max_age_hours", 24))
    generated = snapshot.get("generated_at")
    try:
        gen_dt = datetime.fromisoformat(str(generated).replace("Z", "+00:00"))
    except ValueError:
        return [{"target": "*", "detail": f"Unparseable generated_at: {generated!r}"}]
    age_hours = (now - gen_dt).total_seconds() / 3600.0
    if age_hours > max_age:
        return [{"target": "*",
                 "detail": f"Snapshot is {age_hours:.1f}h old (max {max_age:g}h) — refresh before gating."}]
    return []


def _rule_no_failing_tests(snapshot: dict, cfg: dict, now: datetime) -> list[dict]:
    include_warn = cfg.get("include_warn_firing", True)
    out = []
    for o in snapshot.get("warehouse_objects", []):
        s = o.get("dq_summary") or {}
        failing = s.get("failing_tests_count") or 0
        warn = (s.get("warn_tests_with_failures_count") or 0) if include_warn else 0
        if failing or warn:
            out.append({"target": _obj_target(o),
                        "detail": f"{failing} failing test(s), {warn} warn-severity test(s) firing."})
    return out


def _rule_max_high_risk(snapshot: dict, cfg: dict, now: datetime) -> list[dict]:
    threshold = int(cfg.get("threshold", 0))
    schemas = {s.lower() for s in cfg.get("schemas", [])}
    high = [o for o in snapshot.get("warehouse_objects", [])
            if (o.get("dq_summary") or {}).get("risk_level") == "high"
            and (not schemas or (o.get("schema") or "").lower() in schemas)]
    if len(high) <= threshold:
        return []
    return [{"target": _obj_target(o), "detail": "risk_level=high"} for o in high]


def _rule_min_dbt_coverage(snapshot: dict, cfg: dict, now: datetime) -> list[dict]:
    min_pct = float(cfg.get("min_matched_pct", 0))
    objs = snapshot.get("warehouse_objects", [])
    if not objs:
        return [{"target": "*", "detail": "Snapshot has no warehouse objects to measure coverage on."}]
    matched = sum(1 for o in objs if o.get("match_confidence") != "unmatched")
    pct = 100.0 * matched / len(objs)
    if pct >= min_pct:
        return []
    return [{"target": "*",
             "detail": f"dbt coverage {pct:.1f}% ({matched}/{len(objs)}) below required {min_pct:g}%."}]


def _rule_no_stale_objects(snapshot: dict, cfg: dict, now: datetime) -> list[dict]:
    schemas = {s.lower() for s in cfg.get("schemas", [])}
    out = []
    for r in snapshot.get("dq_recommendations", []):
        if r.get("risk") != "stale_fivetran_sync":
            continue
        target = r.get("target") or {}
        if schemas and (target.get("schema") or "").lower() not in schemas:
            continue
        out.append({"target": f"{target.get('schema')}.{target.get('table')}",
                    "detail": r.get("reason", "stale Fivetran sync")})
    return out


def _rule_activation_gate(snapshot: dict, cfg: dict, now: datetime) -> list[dict]:
    allowed = set(cfg.get("allowed_verdicts", ["allow"]))
    out = []
    for s in snapshot.get("activations", {}).get("syncs", []):
        verdict = (s.get("readiness") or {}).get("verdict", "unknown")
        if verdict in allowed:
            continue
        reasons = "; ".join(r.get("message", "") for r in (s.get("readiness") or {}).get("reasons", []))
        out.append({"target": str(s.get("sync_id")),
                    "detail": f"{s.get('label')} -> {s.get('destination_name')}/"
                              f"{s.get('destination_object')}: verdict={verdict}. {reasons}"})
    return out


def _rule_no_unwaived_pii(snapshot: dict, cfg: dict, now: datetime) -> list[dict]:
    out = []
    for r in snapshot.get("dq_recommendations", []):
        if r.get("signal") != "potential_pii":
            continue
        target = r.get("target") or {}
        out.append({"target": f"{target.get('schema')}.{target.get('table')}",
                    "detail": f"column {target.get('column')!r} flagged as potential PII."})
    return out


_RULES = {
    "snapshot_freshness": _rule_snapshot_freshness,
    "no_failing_tests": _rule_no_failing_tests,
    "max_high_risk_objects": _rule_max_high_risk,
    "min_dbt_coverage": _rule_min_dbt_coverage,
    "no_stale_objects": _rule_no_stale_objects,
    "activation_gate": _rule_activation_gate,
    "no_unwaived_pii": _rule_no_unwaived_pii,
}
