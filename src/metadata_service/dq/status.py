"""Shared dbt test-status semantics.

Kept in one leaf module so the dq_summary counts (combined normalizer), the
recommendation engine, and the activation gate agree on what a "failing test"
is — a token drift (e.g. `runtime error` vs `runtime_error`) fixed in one place
otherwise makes the gate and triage disagree.
"""

from __future__ import annotations

# A dbt test/freshness result in one of these states means the run is red.
FAILING_STATUSES = frozenset({"fail", "error", "runtime error"})


# A dbt State ``reused`` result whose last executed status could not be found
# (see DbtNormalizer._index_run_results). It is evidence of NOTHING: not passing,
# not failing — consumers must treat it like a missing result, and the activation
# gate must never clear on it.
UNVERIFIED_STATUSES = frozenset({"reused"})


def is_failing_status(status) -> bool:
    return (status or "").lower() in FAILING_STATUSES


def is_unverified_status(status) -> bool:
    return (status or "").lower() in UNVERIFIED_STATUSES


def has_result(status) -> bool:
    """True when a status is real evidence (an executed or carried-forward result)."""
    return status is not None and not is_unverified_status(status)
