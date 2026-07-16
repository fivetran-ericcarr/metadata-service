"""Shared dbt test-status semantics.

Kept in one leaf module so the dq_summary counts (combined normalizer), the
recommendation engine, and the activation gate agree on what a "failing test"
is — a token drift (e.g. `runtime error` vs `runtime_error`) fixed in one place
otherwise makes the gate and triage disagree.
"""

from __future__ import annotations

# A dbt test/freshness result in one of these states means the run is red.
FAILING_STATUSES = frozenset({"fail", "error", "runtime error"})


def is_failing_status(status) -> bool:
    return (status or "").lower() in FAILING_STATUSES
