"""The policy engine: snapshot facts + a TOML policy -> a gate decision.

Rules read only documented snapshot-contract fields (ARTIFACTS.md). Each rule
produces PASS / FAIL / WAIVED with evidence rows; the report verdict is FAIL if
any rule fails. Waivers are explicit, targeted, attributed, and expiring —
an exception someone owns, not a silent suppression.

Fail-closed applies to the policy file too: unknown rule names, mistyped
option values, or a policy that enables nothing raise PolicyError instead of
silently gating on less than the author intended.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

PASS, FAIL, WAIVED, SKIPPED = "pass", "fail", "waived", "skipped"

# A snapshot stamped this far in the future is clock skew or a corrupted
# timestamp, not fresher-than-fresh facts.
_FUTURE_SKEW_HOURS = 0.25


class PolicyError(RuntimeError):
    """The policy file is invalid — refuse to gate on it (fail closed)."""


# -- policy ----------------------------------------------------------------
@dataclass
class Waiver:
    rule: str
    target: str          # "schema.table", a sync_id, a source table name, or "*"
    reason: str
    expires: date | None = None   # active through this calendar date, in UTC

    def is_active(self, today: date) -> bool:
        return self.expires is None or today <= self.expires

    def matches(self, rule: str, target: str) -> bool:
        return self.rule == rule and (self.target == "*" or self.target.lower() == str(target).lower())


def _is_number(v: object) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _is_str_list(v: object) -> bool:
    return isinstance(v, list) and all(isinstance(s, str) for s in v)


def _is_count(v: object) -> bool:
    return isinstance(v, int) and not isinstance(v, bool) and v >= 0


# Per-rule config options and their validators. A value TOML can mistype —
# a quoted number, a bare string where a list belongs — must error here, not
# silently change what the gate checks.
_RULE_OPTIONS: dict[str, dict] = {
    "snapshot_freshness": {"max_age_hours": (_is_number, "a number of hours")},
    "no_failing_tests": {"include_warn_firing": (lambda v: isinstance(v, bool), "a boolean")},
    "max_high_risk_objects": {"threshold": (_is_count, "a non-negative integer"),
                              "schemas": (_is_str_list, "a list of schema names")},
    "min_dbt_coverage": {"min_matched_pct": (_is_number, "a number (percent)")},
    "no_stale_objects": {"schemas": (_is_str_list, "a list of schema names")},
    "activation_gate": {"allowed_verdicts": (_is_str_list, "a list of verdicts")},
    "no_unwaived_pii": {},
}


def _validate_rules(rules: object) -> dict:
    if not isinstance(rules, dict):
        raise PolicyError("[rules] must be a table of rule tables.")
    for name, cfg in rules.items():
        if name not in _RULE_OPTIONS:
            raise PolicyError(
                f"Unknown rule [rules.{name}] — known rules: {', '.join(_RULE_OPTIONS)}.")
        if not isinstance(cfg, dict):
            raise PolicyError(
                f"[rules.{name}] must be a table (e.g. a [rules.{name}] section with "
                f"enabled = true), got {type(cfg).__name__}.")
        for key, value in cfg.items():
            if key == "enabled":
                if not isinstance(value, bool):
                    raise PolicyError(f"[rules.{name}] enabled must be true or false.")
                continue
            option = _RULE_OPTIONS[name].get(key)
            if option is None:
                raise PolicyError(
                    f"Unknown option {key!r} in [rules.{name}] — known options: "
                    f"{', '.join(['enabled', *_RULE_OPTIONS[name]]) }.")
            check, expected = option
            if not check(value):
                raise PolicyError(f"[rules.{name}] {key} must be {expected}, got {value!r}.")
    if not any(isinstance(cfg, dict) and cfg.get("enabled") for cfg in rules.values()):
        raise PolicyError(
            "The policy enables no rules — a gate with nothing to check would always "
            "pass. Enable at least one rule.")
    return rules


def _validate_waiver(w: object) -> Waiver:
    if not isinstance(w, dict):
        raise PolicyError("Each [[waivers]] entry must be a table.")
    rule = w.get("rule")
    if rule not in _RULE_OPTIONS:
        raise PolicyError(f"Waiver rule {rule!r} is not a known rule.")
    if "target" not in w:
        raise PolicyError(f"Waiver for rule {rule!r} is missing a target.")
    expires = w.get("expires")
    if isinstance(expires, datetime):
        expires = expires.date()
    elif expires is not None and not isinstance(expires, date):
        raise PolicyError(
            f"Waiver expires must be an unquoted TOML date (expires = 2026-08-01), "
            f"got {expires!r}.")
    return Waiver(rule=rule, target=str(w["target"]),
                  reason=str(w.get("reason", "")), expires=expires)


@dataclass
class Policy:
    name: str = "default"
    rules: dict = field(default_factory=dict)
    waivers: list[Waiver] = field(default_factory=list)

    @classmethod
    def from_toml(cls, path: str | Path) -> "Policy":
        try:
            raw = tomllib.loads(Path(path).read_text(encoding="utf-8"))
        except tomllib.TOMLDecodeError as exc:
            raise PolicyError(f"Invalid TOML in {path}: {exc}") from exc
        waivers_raw = raw.get("waivers", [])
        if not isinstance(waivers_raw, list):
            raise PolicyError("waivers must be an array of tables ([[waivers]]).")
        return cls(name=(raw.get("policy") or {}).get("name", "default"),
                   rules=_validate_rules(raw.get("rules", {})),
                   waivers=[_validate_waiver(w) for w in waivers_raw])


# -- evaluation --------------------------------------------------------------
def evaluate(snapshot: dict, policy: Policy, *, now: datetime | None = None) -> dict:
    """Evaluate every enabled rule and return the decision report."""
    now = now or datetime.now(timezone.utc)
    today = now.date()
    results: list[dict] = []
    used_waivers: set[int] = set()

    def waive(rule: str, targets: tuple[str, ...]) -> Waiver | None:
        # Most-specific match wins: a targeted waiver beats "*", so a broad
        # freeze can't steal attribution from (or hide) a targeted exception.
        exact = wildcard = None
        for i, w in enumerate(policy.waivers):
            if not w.is_active(today):
                continue
            if any(w.matches(rule, t) for t in targets):
                if w.target == "*":
                    wildcard = i if wildcard is None else wildcard
                else:
                    exact = i if exact is None else exact
        pick = exact if exact is not None else wildcard
        if pick is None:
            return None
        used_waivers.add(pick)
        return policy.waivers[pick]

    for rule_name, rule_fn in _RULES.items():
        cfg = policy.rules.get(rule_name, {})
        if not cfg.get("enabled", False):
            results.append({"rule": rule_name, "status": SKIPPED, "evidence": [], "waived": []})
            continue
        evidence = rule_fn(snapshot, cfg, now)
        failing, waived = [], []
        for item in evidence:
            alt_targets = tuple(item.pop("alt_targets", ()))
            w = waive(rule_name, (item["target"], *alt_targets))
            if w:
                waived.append({**item, "waiver_reason": w.reason,
                               "waiver_expires": str(w.expires) if w.expires else None})
            else:
                failing.append(item)
        # Waivers first, threshold second: an org tolerating N exceptions means
        # N *unwaived* ones, and waived objects never count against the budget.
        threshold = cfg.get("threshold", 0)
        status = FAIL if len(failing) > threshold else (WAIVED if waived else PASS)
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
    makes before triggering it. Fails closed: an unknown or ambiguous sync is
    a deny, and so is a stale snapshot (when snapshot_freshness is enabled)."""
    now = now or datetime.now(timezone.utc)
    today = now.date()
    cfg = policy.rules.get("activation_gate", {})
    if not cfg.get("enabled", False):
        # The policy explicitly stood the gate down; say so instead of gating
        # with defaults the deploy report would ignore.
        return {"decision": "allow", "sync": sync_ref,
                "note": "activation_gate is disabled in the policy — no gating applied."}

    fresh_cfg = policy.rules.get("snapshot_freshness", {})
    if fresh_cfg.get("enabled", False):
        stale = _rule_snapshot_freshness(snapshot, fresh_cfg, now)
        if stale:
            return {"decision": "deny", "sync": sync_ref,
                    "reason": f"Snapshot not trustworthy (fail closed): {stale[0]['detail']}"}

    allowed = set(cfg.get("allowed_verdicts", ["allow"]))

    sync = None
    table_matches = []
    for s in snapshot.get("activations", {}).get("syncs", []):
        if str(s.get("sync_id")) == str(sync_ref):
            sync = s          # sync ids are unique — an exact id match wins
            break
        obj = s.get("source_object") or {}
        if (obj.get("table_name") or "").lower() == str(sync_ref).lower():
            table_matches.append(s)
    if sync is None:
        if len(table_matches) > 1:
            ids = [s.get("sync_id") for s in table_matches]
            return {"decision": "deny", "sync": sync_ref,
                    "reason": f"Ambiguous — {len(ids)} syncs read {sync_ref!r} "
                              f"(sync_ids {ids}); pre-flight each by sync_id (fail closed)."}
        if not table_matches:
            return {"decision": "deny", "sync": sync_ref,
                    "reason": "Sync not present in the snapshot — cannot verify readiness (fail closed)."}
        sync = table_matches[0]

    readiness = sync.get("readiness") or {}
    verdict = readiness.get("verdict", "unknown")
    if verdict in allowed:
        return {"decision": "allow", "sync": sync_ref, "verdict": verdict,
                "sync_id": sync.get("sync_id")}

    # Waivers address the sync by id or by its source table; targeted beats "*".
    targets = (str(sync.get("sync_id")),
               (sync.get("source_object") or {}).get("table_name") or "")
    exact = wildcard = expired = None
    for w in policy.waivers:
        if not any(w.matches("activation_gate", t) for t in targets if t):
            continue
        if not w.is_active(today):
            expired = expired or w
        elif w.target == "*":
            wildcard = wildcard or w
        else:
            exact = exact or w
    active = exact or wildcard
    if active:
        return {"decision": "allow", "sync": sync_ref, "verdict": verdict,
                "sync_id": sync.get("sync_id"), "waived": True, "waiver_reason": active.reason}
    deny = {"decision": "deny", "sync": sync_ref, "verdict": verdict,
            "sync_id": sync.get("sync_id"),
            "reasons": [r.get("message") for r in readiness.get("reasons", [])]}
    if expired:
        deny["expired_waiver"] = {"target": expired.target, "reason": expired.reason,
                                  "expires": str(expired.expires)}
    return deny


# -- rules ---------------------------------------------------------------------
# Each rule: (snapshot, cfg, now) -> list of evidence dicts, each with a "target"
# the waiver mechanism can address (plus optional "alt_targets" aliases).
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
    if gen_dt.tzinfo is None:
        gen_dt = gen_dt.replace(tzinfo=timezone.utc)
    age_hours = (now - gen_dt).total_seconds() / 3600.0
    if age_hours > max_age:
        return [{"target": "*",
                 "detail": f"Snapshot is {age_hours:.1f}h old (max {max_age:g}h) — refresh before gating."}]
    if age_hours < -_FUTURE_SKEW_HOURS:
        return [{"target": "*",
                 "detail": f"Snapshot generated_at is {-age_hours:.1f}h in the future — "
                           "clock skew or a corrupted timestamp; refusing to trust it."}]
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
    # Returns every high-risk object; evaluate() applies waivers first and the
    # threshold to what remains, so waived objects never eat the budget.
    schemas = {s.lower() for s in cfg.get("schemas", [])}
    return [{"target": _obj_target(o), "detail": "risk_level=high"}
            for o in snapshot.get("warehouse_objects", [])
            if (o.get("dq_summary") or {}).get("risk_level") == "high"
            and (not schemas or (o.get("schema") or "").lower() in schemas)]


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
        table = (s.get("source_object") or {}).get("table_name")
        out.append({"target": str(s.get("sync_id")),
                    "alt_targets": [table] if table else [],
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
