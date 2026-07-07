"""Activation readiness gate — 'is it safe to push this data back to prod?'

A Fivetran Activation (reverse ETL) sync reads a warehouse object (usually a dbt
model) and writes it to an operational system (Salesforce, etc.). Pushing bad
data back into a system of record is worse than a stale dashboard: it corrupts
the business's source of truth.

This module answers, per sync: **allow | warn | block**, by traversing the dbt
lineage *upstream* from the sync's source model and inspecting the DQ posture of
everything that feeds it.

Policy (deterministic, fail-closed):
  block  — any upstream dbt test is failing (fail/error), OR a warn-severity test
           has failures > 0 (a soft test that is actually firing), OR a source
           freshness check is failing, OR an upstream Fivetran object is stale.
  warn   — the source model has no enforced contract, OR an upstream object is
           unmatched to dbt (a coverage blind spot on data headed to prod), OR
           the gate lacks evidence: no upstream tests exist, tests exist but have
           no run results, or Fivetran coverage data is absent from the build.
           Absence of evidence is never treated as "allow".
  allow  — upstream tests exist, ran, and are clean; no stale syncs or gaps.
  unknown— the sync's source object could not be matched to a dbt model/source
           (no lineage to reason over). Combined with a named source object this
           raises an ``activates_unverified_data`` risk downstream.
"""

from __future__ import annotations

from .lineage import LineageGraph

_FAILING_STATUSES = {"fail", "error", "runtime error"}


def _is_failing(test: dict) -> bool:
    return (test.get("status") or "").lower() in _FAILING_STATUSES


def _is_warn_with_failures(test: dict) -> bool:
    severity = (test.get("severity") or "").lower()
    status = (test.get("status") or "").lower()
    if severity != "warn" and status != "warn":
        return False
    failures = test.get("failures")
    # A warn *status* means the test fired; if the artifact omits the failures
    # count, fail closed and treat it as firing rather than assuming zero.
    if failures is None:
        return status == "warn"
    try:
        return int(failures) > 0
    except (TypeError, ValueError):
        return status == "warn"


# Public: shared with the combined normalizer's dq_summary so triage counts and
# the gate agree on what a "firing warn test" is.
is_warn_with_failures = _is_warn_with_failures


def evaluate_syncs(
    syncs: list[dict],
    *,
    models: list[dict],
    sources: list[dict],
    lineage: LineageGraph,
    warehouse_objects: list[dict] | None = None,
    stale_object_ids: set[str] | None = None,
) -> list[dict]:
    """Return the syncs with a ``readiness`` block attached to each."""
    models_by_uid = {m["unique_id"]: m for m in models or []}
    sources_by_uid = {s["unique_id"]: s for s in sources or []}
    stale_object_ids = stale_object_ids or set()

    # (schema.lower(), (alias|name).lower()) -> model unique_id
    model_lookup: dict[tuple[str, str], str] = {}
    for m in models or []:
        schema = (m.get("schema") or "").lower()
        for key in {(m.get("alias") or "").lower(), (m.get("name") or "").lower()}:
            if schema and key:
                model_lookup.setdefault((schema, key), m["unique_id"])
    source_lookup: dict[tuple[str, str], str] = {}
    for s in sources or []:
        schema = (s.get("schema") or "").lower()
        for key in {(s.get("identifier") or "").lower(), (s.get("table_name") or "").lower(),
                    (s.get("name") or "").lower()}:
            if schema and key:
                source_lookup.setdefault((schema, key), s["unique_id"])

    # source_unique_id -> whether its Fivetran object is stale
    stale_sources: set[str] = set()
    for obj in warehouse_objects or []:
        suid = (obj.get("dbt") or {}).get("source_unique_id")
        if suid and obj.get("object_id") in stale_object_ids:
            stale_sources.add(suid)
    # source_unique_id -> whether it is matched to a Fivetran object at all
    matched_source_uids = {
        (obj.get("dbt") or {}).get("source_unique_id")
        for obj in warehouse_objects or []
        if (obj.get("dbt") or {}).get("source_unique_id")
    }

    has_coverage_data = bool(warehouse_objects)

    out: list[dict] = []
    for sync in syncs or []:
        enriched = dict(sync)
        enriched["readiness"] = _evaluate_one(
            sync, model_lookup, source_lookup, models_by_uid, sources_by_uid,
            lineage, stale_sources, matched_source_uids, has_coverage_data,
        )
        out.append(enriched)
    return out


def _evaluate_one(sync, model_lookup, source_lookup, models_by_uid, sources_by_uid,
                  lineage, stale_sources, matched_source_uids, has_coverage_data) -> dict:
    obj = sync.get("source_object") or {}
    schema = (obj.get("table_schema") or "").lower()
    name = (obj.get("table_name") or "").lower()

    source_model_uid = model_lookup.get((schema, name))
    source_kind = "model"
    if source_model_uid is None:
        source_model_uid = source_lookup.get((schema, name))
        source_kind = "source" if source_model_uid else None

    if source_model_uid is None:
        return {
            "verdict": "unknown",
            "source_node_unique_id": None,
            "reasons": [{
                "code": "source_not_matched",
                "severity": "medium",
                "message": "Activation source object is not matched to a dbt model or source; "
                           "no lineage available to assess readiness. This sync pushes "
                           "unverified data if the source is in scope.",
            }],
            "upstream": {"node_count": 0, "failing_tests": 0, "warn_tests_with_failures": 0,
                         "tests_seen": 0, "tests_with_results": 0,
                         "stale_objects": 0, "missing_contract": False, "unmatched_upstream": 0},
        }

    upstream = {source_model_uid} | set(lineage.ancestors(source_model_uid))
    # Snapshots appear in lineage (a model can depend on snapshot.*); include
    # them in the model-style walk so their tests aren't silently dropped.
    # (The dbt normalizer currently only extracts model tests, so an absent
    # snapshot entry contributes nothing — but it is no longer filtered out.)
    upstream_models = [u for u in upstream if u.startswith(("model.", "snapshot."))]
    upstream_sources = [u for u in upstream if u.startswith("source.")]

    reasons: list[dict] = []
    failing = warn_failing = stale = unmatched = 0
    tests_seen = tests_with_results = 0
    failing_detail: list[dict] = []

    def _tally(test: dict) -> None:
        nonlocal tests_seen, tests_with_results
        tests_seen += 1
        if test.get("status") is not None:
            tests_with_results += 1

    for uid in upstream_models:
        model = models_by_uid.get(uid) or {}
        for test in model.get("tests") or []:
            _tally(test)
            if _is_failing(test):
                failing += 1
                failing_detail.append({"node": uid, "test": test.get("name"),
                                       "status": test.get("status"), "severity": "error"})
            elif _is_warn_with_failures(test):
                warn_failing += 1
                failing_detail.append({"node": uid, "test": test.get("name"),
                                       "status": "warn", "failures": test.get("failures")})

    for uid in upstream_sources:
        src = sources_by_uid.get(uid) or {}
        fr = src.get("freshness_result") or {}
        if (fr.get("status") or "").lower() in _FAILING_STATUSES:
            failing += 1
            failing_detail.append({"node": uid, "test": "source_freshness", "status": fr.get("status")})
        for test in src.get("tests") or []:
            _tally(test)
            if _is_failing(test):
                failing += 1
                failing_detail.append({"node": uid, "test": test.get("name"), "status": test.get("status")})
            elif _is_warn_with_failures(test):
                warn_failing += 1
                failing_detail.append({"node": uid, "test": test.get("name"),
                                       "status": "warn", "failures": test.get("failures")})
        if uid in stale_sources:
            stale += 1
        if uid not in matched_source_uids and matched_source_uids:
            unmatched += 1

    # Evidence gaps — the gate must not say "allow" when it simply has no data.
    no_evidence = False
    if tests_seen == 0:
        no_evidence = True
        reasons.append({"code": "no_upstream_tests", "severity": "medium",
                        "message": "No dbt tests exist anywhere upstream of this activation; "
                                   "there is no quality evidence to clear it on."})
    elif tests_with_results == 0:
        no_evidence = True
        reasons.append({"code": "no_test_results", "severity": "medium",
                        "message": f"{tests_seen} upstream test(s) are defined but none have a "
                                   "run result (run_results.json missing or stale); the tests "
                                   "may be failing without the gate seeing it."})
    if not has_coverage_data:
        no_evidence = True
        reasons.append({"code": "coverage_checks_skipped", "severity": "medium",
                        "message": "No Fivetran coverage data in this build; the stale-sync and "
                                   "unmatched-upstream checks could not run."})

    # Contract on the source model itself (only if it is a model).
    source_model = models_by_uid.get(source_model_uid) or {}
    governance = source_model.get("governance") or {}
    missing_contract = source_kind == "model" and not governance.get("contract_enforced")

    if failing:
        reasons.append({"code": "upstream_failing_tests", "severity": "high",
                        "message": f"{failing} upstream dbt test(s) failing.", "detail": failing_detail})
    if warn_failing:
        reasons.append({"code": "upstream_warn_test_failures", "severity": "high",
                        "message": f"{warn_failing} upstream warn-severity test(s) have failing rows "
                                   "(soft test firing on data headed to prod)."})
    if stale:
        reasons.append({"code": "stale_upstream_sync", "severity": "high",
                        "message": f"{stale} upstream Fivetran object(s) are stale."})
    if missing_contract:
        reasons.append({"code": "source_model_no_contract", "severity": "medium",
                        "message": "Activation source model has no enforced dbt contract; "
                                   "schema changes can silently corrupt the destination."})
    if unmatched:
        reasons.append({"code": "unmatched_upstream", "severity": "medium",
                        "message": f"{unmatched} upstream source(s) have no Fivetran/dbt coverage match."})

    if failing or warn_failing or stale:
        verdict = "block"
    elif missing_contract or unmatched or no_evidence:
        verdict = "warn"
    else:
        verdict = "allow"
        reasons.append({"code": "clean", "severity": "info",
                        "message": "Upstream tests ran clean; no stale syncs or governance gaps."})

    return {
        "verdict": verdict,
        "source_node_unique_id": source_model_uid,
        "source_node_kind": source_kind,
        "reasons": reasons,
        "upstream": {
            "node_count": len(upstream),
            "failing_tests": failing,
            "warn_tests_with_failures": warn_failing,
            "tests_seen": tests_seen,
            "tests_with_results": tests_with_results,
            "stale_objects": stale,
            "missing_contract": missing_contract,
            "unmatched_upstream": unmatched,
        },
    }
