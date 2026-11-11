"""Curated semantic vocabulary the dbt manifest does not carry.

dbt-native values come first — ``meta.synonyms`` on a metric, model or column
is read straight from the manifest. This optional JSON file
(``SEMANTIC_OVERRIDES_FILE``) holds the rest: business synonyms for a whole
project, notes appended to a metric's definition, and the plain-English impact
of known DQ rules, matched against dbt test names. Shape::

    {
      "synonyms":        {"written_premium": ["GWP", "gross written premium"]},
      "metric_synonyms": {"bind_rate": ["hit ratio", "conversion rate"]},
      "table_synonyms":  {"fct_quote": ["quotes", "priced submissions"]},
      "metric_notes":    {"written_premium": "Excludes quarantined quotes."},
      "dq_rules": [
        {"id": "NEGATIVE_PREMIUM", "pattern": "accepted_range_gw_quote_total_premium",
         "short": "{n} negative-premium quotes are quarantined", "metrics": ["written_premium"]}
      ],
      "out_of_scope": {"source_schemas": ["guidewire_cda"], "model_prefixes": ["cc_"],
                       "model_names": ["fct_claim"]},
      "guidance": ["Prefer governed dbt metrics over re-deriving them."]
    }

Keys are matched case-insensitively. Every key is optional.
"""

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path

logger = logging.getLogger(__name__)

_CACHE: dict[str, tuple[float, dict]] = {}


def _lower_keys(d) -> dict:
    return {str(k).lower(): v for k, v in (d or {}).items()} if isinstance(d, dict) else {}


def normalize_overrides(raw: dict | None) -> dict:
    raw = raw if isinstance(raw, dict) else {}
    rules = []
    for r in raw.get("dq_rules") or []:
        if not isinstance(r, dict) or not r.get("pattern"):
            continue
        try:
            rules.append({**r, "_re": re.compile(r["pattern"])})
        except re.error:
            logger.warning("Ignoring dq_rule %r: invalid pattern", r.get("id"))
    scope = raw.get("out_of_scope") or {}
    return {
        "synonyms": _lower_keys(raw.get("synonyms")),
        "metric_synonyms": _lower_keys(raw.get("metric_synonyms")),
        "table_synonyms": _lower_keys(raw.get("table_synonyms")),
        "metric_notes": _lower_keys(raw.get("metric_notes")),
        "dq_rules": rules,
        "out_of_scope": {
            "source_schemas": {s.lower() for s in scope.get("source_schemas") or []},
            "model_prefixes": tuple(p.lower() for p in scope.get("model_prefixes") or []),
            "model_names": {n.lower() for n in scope.get("model_names") or []},
        },
        "guidance": [g for g in raw.get("guidance") or [] if isinstance(g, str)],
    }


def load_overrides(path: str | None) -> dict:
    """Load + normalize the overrides file (cached on mtime). Missing or
    malformed files degrade to empty overrides, never an error."""
    if not path:
        return normalize_overrides(None)
    try:
        mtime = os.stat(path).st_mtime
    except OSError:
        logger.warning("SEMANTIC_OVERRIDES_FILE not readable; ignoring.")
        return normalize_overrides(None)
    cached = _CACHE.get(path)
    if cached and cached[0] == mtime:
        return cached[1]
    try:
        data = normalize_overrides(json.loads(Path(path).read_text(encoding="utf-8")))
    except (OSError, ValueError) as exc:
        logger.warning("SEMANTIC_OVERRIDES_FILE could not be parsed (%s); ignoring.", type(exc).__name__)
        data = normalize_overrides(None)
    _CACHE[path] = (mtime, data)
    return data


def is_out_of_scope(unique_id: str | None, overrides: dict) -> bool:
    """True for a dbt node the overrides exclude from the agent's scope."""
    if not unique_id:
        return False
    scope = overrides.get("out_of_scope") or {}
    parts = unique_id.split(".")
    if parts[0] == "source":
        return len(parts) > 2 and parts[2].lower() in scope.get("source_schemas", set())
    name = parts[-1].lower()
    prefixes = scope.get("model_prefixes") or ()
    return bool(prefixes and name.startswith(prefixes)) or name in scope.get("model_names", set())


def match_rule(test_name: str | None, overrides: dict) -> dict | None:
    for r in overrides.get("dq_rules") or []:
        if test_name and r["_re"].search(test_name):
            return r
    return None
