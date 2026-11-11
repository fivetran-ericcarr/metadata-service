"""Semantic context for agents: one call to ground SQL and caveats.

``semantic_context(doc, query)`` resolves a metric, dbt model, warehouse/lake
table or a natural-language topic against a metadata snapshot and returns,
for the top matches:

* dbt Semantic Layer metric definitions — type, measures (agg + expr), ratio
  numerator/denominator, derived expressions, filters, time grain, and the
  dimensions/entities a query can group by;
* model and column descriptions, data types and synonyms — dbt ``meta.synonyms``,
  curated overrides, and legacy source column names found through column-level
  lineage (a gold ``risk_grade_code`` fed by the source's ``RISKGRADECD``);
* Fivetran provenance of every upstream source (connection, connector service,
  last sync, staleness, and whether the snapshot is live or fixture-built);
* DQ trust — metric trust level, failing/warn/unverified tests, stale syncs,
  quarantine registers, with curated plain-English impact when configured;
* the physical location per engine/catalog (Snowflake FQN, Polaris
  catalog.namespace.table, Glue database.table, Unity catalog.schema.table).

Payloads stay small: ``top_k`` matches are expanded, the rest are listed;
columns, dimensions and caveats are capped with ``truncated`` counts.

The logic generalizes what the Guidewire demo's ``semantic/build_semantic_view.py``
assembled for one Snowflake semantic view, without its project specifics
(those move to the overrides file, see :mod:`.overrides`).
"""

from __future__ import annotations

import re
from collections import defaultdict

from ..catalog.locations import physical_locations
from .overrides import is_out_of_scope, match_rule, normalize_overrides

_TOKEN = re.compile(r"[a-z0-9]+")
_STOP = {"a", "an", "the", "of", "by", "for", "in", "on", "and", "or", "to", "what", "is",
         "are", "how", "many", "much", "per", "with", "show", "me", "give", "which", "was",
         "were", "do", "does", "did", "our", "my", "we", "last", "this", "that", "from"}
_FAILING = {"fail", "error", "runtime error"}
_UNVERIFIED = {"reused", "skipped", None, ""}
KINDS = ("metric", "model", "table")


def _tokens(text: str | None) -> set[str]:
    out = set()
    for t in _TOKEN.findall((text or "").lower()):
        if t in _STOP:
            continue
        out.add(t[:-1] if len(t) > 3 and t.endswith("s") else t)
    return out


def _squash(name: str | None) -> str:
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def _short(text: str | None, n: int = 300) -> str | None:
    if not text:
        return None
    text = re.sub(r"\s+", " ", text).strip()
    return text if len(text) <= n else text[: n - 1] + "…"


def _meta_synonyms(meta) -> list[str]:
    syn = (meta or {}).get("synonyms") if isinstance(meta, dict) else None
    if isinstance(syn, str):
        return [syn]
    return [s for s in syn or [] if isinstance(s, str)]


def _dedupe(items) -> list:
    seen, out = set(), []
    for i in items:
        k = i.lower() if isinstance(i, str) else i
        if i and k not in seen:
            seen.add(k)
            out.append(i)
    return out


class SemanticIndex:
    """Lookup structures over one snapshot (built once per snapshot)."""

    def __init__(self, doc: dict, overrides: dict | None = None) -> None:
        self.doc = doc or {}
        self.ov = overrides or normalize_overrides(None)
        dbt = (self.doc.get("sources") or {}).get("dbt") or {}
        self.models = {m["unique_id"]: m for m in dbt.get("models") or [] if m.get("unique_id")}
        self.dbt_sources = {s["unique_id"]: s for s in dbt.get("sources") or [] if s.get("unique_id")}
        self.tests = dbt.get("tests") or []
        self.metrics = {(m.get("name") or "").lower(): m for m in dbt.get("metrics") or [] if m.get("name")}
        self.semantic_models = {s["unique_id"]: s for s in dbt.get("semantic_models") or []
                                if s.get("unique_id")}
        self.exposures = dbt.get("exposures") or []
        self.metric_quality = {(m.get("metric") or "").lower(): m
                               for m in self.doc.get("metric_quality") or []}
        self.objects = self.doc.get("warehouse_objects") or []
        self.obj_by_source = {(o.get("dbt") or {}).get("source_unique_id"): o for o in self.objects
                              if (o.get("dbt") or {}).get("source_unique_id")}
        self.obj_by_name: dict[tuple[str, str], dict] = {
            ((o.get("schema") or "").lower(), (o.get("name") or "").lower()): o for o in self.objects}
        self.destination = ((self.doc.get("sources") or {}).get("fivetran") or {}).get("destination") or {}
        scope = self.doc.get("build_scope") or {}
        self.data_mode = "fixtures" if (scope.get("fixtures") or scope.get("fivetran_fixtures")) else "live"
        self.stale_ids = {r.get("object_id") for r in self.doc.get("dq_recommendations") or []
                          if r.get("risk") == "stale_fivetran_sync"}

        self.parents: dict[str, set[str]] = defaultdict(set)
        self.children: dict[str, set[str]] = defaultdict(set)
        for e in dbt.get("lineage_edges") or []:
            self.parents[e["to_unique_id"]].add(e["from_unique_id"])
            self.children[e["from_unique_id"]].add(e["to_unique_id"])
        self.col_up: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
        for e in dbt.get("column_lineage_edges") or []:
            self.col_up[(e["to_unique_id"], (e["to_column"] or "").lower())].append(
                (e["from_unique_id"], (e["from_column"] or "").lower()))
        self.tests_by_node: dict[str, list[dict]] = defaultdict(list)
        for t in self.tests:
            if t.get("attached_node"):
                self.tests_by_node[t["attached_node"]].append(t)
        self.model_by_name = {}
        for uid, m in self.models.items():
            for key in (m.get("name"), m.get("alias")):
                if key:
                    self.model_by_name.setdefault(key.lower(), uid)
        self.quarantine_models = [
            uid for uid, m in self.models.items()
            if "quarantine" in (m.get("name") or "").lower()
            or any("quarantine" in str(t).lower() for t in m.get("tags") or [])
        ]

    # -- lineage -----------------------------------------------------------
    def ancestors(self, uid: str) -> set[str]:
        out, stack = set(), [uid]
        while stack:
            for p in self.parents.get(stack.pop(), ()):
                if p not in out:
                    out.add(p)
                    stack.append(p)
        return out

    def in_scope(self, uid: str) -> bool:
        return not is_out_of_scope(uid, self.ov)

    def column_sources(self, uid: str, col: str) -> list[tuple[str, str]]:
        out, stack, seen = [], [(uid, col.lower())], set()
        while stack:
            for p in self.col_up.get(stack.pop(), ()):
                if p in seen:
                    continue
                seen.add(p)
                if p[0].startswith("source."):
                    out.append(p)
                else:
                    stack.append(p)
        return sorted(out)

    # -- resolution --------------------------------------------------------
    def candidates(self):
        """(kind, key, display name, texts) for every addressable thing."""
        for key, m in self.metrics.items():
            syn = _meta_synonyms(m.get("meta")) + list(self.ov["metric_synonyms"].get(key, []))
            yield "metric", key, m.get("name"), m.get("label"), m.get("description"), syn
        for uid, m in self.models.items():
            if not self.in_scope(uid):
                continue
            syn = _meta_synonyms(m.get("meta")) + list(self.ov["table_synonyms"].get(
                (m.get("name") or "").lower(), []))
            yield "model", uid, m.get("name"), None, m.get("description"), syn
        for o in self.objects:
            src = (o.get("dbt") or {}).get("source_unique_id")
            if src and not self.in_scope(src):
                continue
            key = f"{o.get('schema')}.{o.get('name')}"
            yield "table", key.lower(), key, None, None, []

    def resolve(self, query: str, kind: str = "auto") -> list[dict]:
        """Ranked matches ``[{kind, key, name, score}]`` for a query."""
        q = (query or "").strip()
        if ":" in q and q.split(":", 1)[0] in KINDS:  # fetch-style id, e.g. "metric:bind_rate"
            kind, q = q.split(":", 1)
        ql = q.lower()
        kinds = KINDS if kind in ("auto", "topic", None, "") else (kind,)
        exact = []
        if "metric" in kinds and ql in self.metrics:
            exact.append({"kind": "metric", "key": ql, "name": self.metrics[ql]["name"], "score": 100.0})
        if "model" in kinds:
            uid = ql if ql in self.models else None
            if uid is None:
                parts = ql.split(".")
                cand = self.model_by_name.get(parts[-1])
                if cand and (len(parts) == 1 or (len(parts) == 2 and
                                                 (self.models[cand].get("schema") or "").lower() == parts[0])):
                    uid = cand
            if uid:
                exact.append({"kind": "model", "key": uid, "name": self.models[uid]["name"], "score": 90.0})
        if "table" in kinds:
            parts = ql.split(".")
            if len(parts) >= 2 and (parts[-2], parts[-1]) in self.obj_by_name:
                o = self.obj_by_name[(parts[-2], parts[-1])]
                exact.append({"kind": "table", "key": f"{parts[-2]}.{parts[-1]}",
                              "name": f"{o.get('schema')}.{o.get('name')}", "score": 95.0})
            elif len(parts) == 1:
                for (s, t), o in self.obj_by_name.items():
                    if t == ql:
                        exact.append({"kind": "table", "key": f"{s}.{t}",
                                      "name": f"{o.get('schema')}.{o.get('name')}", "score": 80.0})
        if exact and kind != "topic":
            return sorted(exact, key=lambda m: -m["score"])

        qt = _tokens(q)
        if not qt:
            return []
        scored = []
        for ckind, key, name, label, desc, syn in self.candidates():
            if ckind not in kinds:
                continue
            score = 0.0
            nt = _tokens((name or "").replace("_", " "))
            if nt:
                score += 6.0 * len(qt & nt) / len(nt)
            if label:
                lt = _tokens(label)
                score += 3.0 * len(qt & lt) / max(len(lt), 1)
            for s in syn:
                st = _tokens(s)
                if st and st <= qt:
                    score += 5.0
                    break
            if desc:
                score += 1.5 * len(qt & _tokens(desc)) / max(len(qt), 1)
            if ckind == "model":
                score += self._column_score(key, qt)
            if score > 0.5:
                scored.append({"kind": ckind, "key": key, "name": name,
                               "score": round(score + {"metric": 0.6, "model": 0.3}.get(ckind, 0), 2)})
        return sorted(scored, key=lambda m: (-m["score"], m["kind"], m["name"] or ""))

    def _column_score(self, uid: str, qt: set[str]) -> float:
        best = 0.0
        for c in self.models[uid].get("columns") or []:
            ct = _tokens((c.get("name") or "").replace("_", " "))
            legacy = self.legacy_columns(uid, c["name"]) if c.get("name") else []
            syn = self.column_synonyms(uid, c.get("name"), legacy=legacy)
            hit = 2.0 * len(qt & ct) / max(len(ct), 1) if ct else 0.0
            if any(_tokens(s) and _tokens(s) <= qt for s in syn):
                hit = max(hit, 2.5)
            best = max(best, hit)
        return best

    # -- columns -----------------------------------------------------------
    def legacy_columns(self, uid: str, col: str) -> list[dict]:
        """Upstream source columns (via column lineage) whose names differ from
        ``col`` — the legacy vocabulary an agent may be asked in."""
        out = []
        for s_uid, s_col in self.column_sources(uid, col):
            if not self.in_scope(s_uid):
                continue
            obj = self.obj_by_source.get(s_uid)
            origin = (obj or {}).get("origin") or {}
            physical = s_col
            if obj:
                for c in obj.get("columns") or []:
                    if (c.get("name") or "").lower() == s_col:
                        physical = c.get("source_name") or c.get("name") or s_col
                        break
            if _squash(physical) == _squash(col):
                continue
            src = self.dbt_sources.get(s_uid) or {}
            out.append({
                "column": physical,
                "source_table": origin.get("source_table") or src.get("identifier") or src.get("table_name"),
                "connector_service": origin.get("connector_service"),
                "connection_id": origin.get("connection_id"),
            })
        return out

    def column_synonyms(self, uid: str, col: str | None, *, legacy: list[dict] | None = None) -> list[str]:
        if not col:
            return []
        meta = {}
        for c in self.models.get(uid, {}).get("columns") or []:
            if (c.get("name") or "").lower() == col.lower():
                meta = c.get("meta") or {}
        syn = _meta_synonyms(meta) + list(self.ov["synonyms"].get(col.lower(), []))
        # Only a column fed by exactly one upstream source column is a rename
        # (RISKGRADECD -> risk_grade_code); an aggregate over several source
        # columns (a count, a flag) is not a synonym of any of them.
        if legacy and len(legacy) == 1:
            syn.append(legacy[0]["column"])
        return _dedupe(syn)


# -- context builders ------------------------------------------------------
def _test_caveat(ix: SemanticIndex, t: dict) -> dict | None:
    status = (t.get("latest_status") or t.get("status") or "")
    status_l = str(status).lower()
    if status_l in ("pass", "success"):
        return None
    node = t.get("attached_node")
    rule = match_rule(t.get("name"), ix.ov)
    n = t.get("failures")
    if status_l in _FAILING:
        kind = "failing_test"
    elif status_l == "warn":
        kind = "warn_test"
    elif status_l in _UNVERIFIED:
        kind = "unverified_test"
    else:
        kind = "test_" + status_l
    target = (node or "?").split(".")[-1] + (f".{t['attached_column']}" if t.get("attached_column") else "")
    if rule:
        message = rule.get("short", rule.get("id", "")).format(n=n)
    elif kind == "unverified_test":
        message = f"dbt test {t.get('name')} on {target} has no verified result ({status or 'not run'})."
    else:
        message = f"dbt test {t.get('name')} on {target}: {status_l}" + (f" ({n} failing rows)" if n else "") + "."
    return {"kind": kind, "id": rule.get("id") if rule else t.get("name"), "test": t.get("name"),
            "status": status_l or None, "failures": n, "severity": t.get("severity"),
            "attached": target, "message": message,
            "metrics": rule.get("metrics", []) if rule else []}


def _caveats_for(ix: SemanticIndex, uids: set[str]) -> list[dict]:
    """Caveats for a set of nodes and their in-scope ancestors."""
    nodes = set(uids)
    for u in uids:
        nodes |= ix.ancestors(u)
    nodes = {n for n in nodes if ix.in_scope(n)}
    out, seen = [], set()
    for n in sorted(nodes):
        for t in ix.tests_by_node.get(n, []):
            c = _test_caveat(ix, t)
            if not c or c["test"] in seen:
                continue
            seen.add(c["test"])
            # Several tests matching one curated rule are one caveat.
            dup = next((o for o in out if o.get("rule") and o["id"] == c["id"]), None)
            if dup:
                dup["tests"] = dup.get("tests", 1) + 1
                continue
            if c["id"] == c["test"]:
                c.pop("id")
                c["id"] = c.pop("test")
            else:
                c["rule"] = True
            out.append(c)
    stale_by_conn: dict[tuple, list[str]] = defaultdict(list)
    for n in sorted(nodes):
        if not n.startswith("source."):
            continue
        obj = ix.obj_by_source.get(n)
        if obj and obj.get("object_id") in ix.stale_ids:
            o = obj.get("origin") or {}
            stale_by_conn[(o.get("connection_id"), o.get("connector_service"),
                           o.get("last_successful_sync"))].append(o.get("source_table") or obj.get("name"))
    for (conn, service, last), tables in sorted(stale_by_conn.items(), key=lambda kv: str(kv[0])):
        shown = ", ".join(sorted(tables)[:5]) + (f" (+{len(tables) - 5} more)" if len(tables) > 5 else "")
        out.append({"kind": "stale_sync", "id": f"STALE_SYNC:{conn}", "attached": conn,
                    "tables": len(tables),
                    "message": f"Fivetran connection {conn} ({service}) last synced {last}; "
                               f"{len(tables)} upstream table(s) flagged stale: {shown}."})
    for q in ix.quarantine_models:
        if q in uids or not ix.in_scope(q):
            continue
        if ix.ancestors(q) & nodes:
            m = ix.models[q]
            out.append({"kind": "quarantine", "id": f"QUARANTINE:{m.get('name')}",
                        "attached": m.get("name"),
                        "message": f"Records failing DQ rules upstream are registered in "
                                   f"{m.get('relation_name') or m.get('name')}; rows there may be "
                                   f"excluded or flagged in this result — query it to explain exclusions."})
    order = {"failing_test": 0, "stale_sync": 1, "warn_test": 2, "quarantine": 3, "unverified_test": 4}
    return sorted(out, key=lambda c: (order.get(c["kind"], 5), c.get("id") or ""))


def _trust_level(caveats: list[dict], has_tests: bool) -> str:
    kinds = {c["kind"] for c in caveats}
    if "failing_test" in kinds:
        return "at_risk"
    if kinds & {"warn_test", "stale_sync", "unverified_test"}:
        return "watch"
    return "trusted" if has_tests else "unknown"


def _provenance(ix: SemanticIndex, uids: set[str], max_tables: int = 8) -> list[dict]:
    """Fivetran provenance of the upstream sources, one entry per connection."""
    srcs = set()
    for u in uids:
        if u.startswith("source."):
            srcs.add(u)
        srcs |= {a for a in ix.ancestors(u) if a.startswith("source.")}
    by_conn: dict[str, dict] = {}
    unmatched = []
    for s in sorted(srcs):
        if not ix.in_scope(s):
            continue
        obj = ix.obj_by_source.get(s)
        if not obj:
            unmatched.append(s)
            continue
        o = obj.get("origin") or {}
        conn = o.get("connection_id") or "unknown"
        entry = by_conn.setdefault(conn, {
            "connection_id": o.get("connection_id"),
            "connector_service": o.get("connector_service"),
            "last_successful_sync": o.get("last_successful_sync"),
            "sync_state": o.get("sync_state"),
            "setup_state": o.get("setup_state"),
            "data_mode": ix.data_mode,
            "tables": [], "stale_tables": 0, "table_count": 0,
        })
        entry["table_count"] += 1
        if obj.get("object_id") in ix.stale_ids:
            entry["stale_tables"] += 1
        if len(entry["tables"]) < max_tables:
            entry["tables"].append({
                "destination": f"{obj.get('schema')}.{obj.get('name')}",
                "source": ".".join(p for p in (o.get("source_schema"), o.get("source_table")) if p),
            })
        last = o.get("last_successful_sync")
        if last and (entry["last_successful_sync"] or "") < last:
            entry["last_successful_sync"] = last
    out = list(by_conn.values())
    if unmatched:
        out.append({"connection_id": None, "note": "dbt sources not matched to a Fivetran-synced table",
                    "dbt_sources": unmatched[:max_tables], "table_count": len(unmatched)})
    return out


def _columns(ix: SemanticIndex, uid: str, qt: set[str], max_columns: int) -> tuple[list[dict], int]:
    model = ix.models.get(uid) or {}
    cols = []
    for c in model.get("columns") or []:
        name = c.get("name")
        legacy = ix.legacy_columns(uid, name) if name else []
        syn = ix.column_synonyms(uid, name, legacy=legacy)
        ct = _tokens((name or "").replace("_", " ")) | {t for s in syn for t in _tokens(s)}
        rel = len(qt & ct)
        entry = {"name": name, "data_type": c.get("data_type"),
                 "description": _short(c.get("description"), 200)}
        if syn:
            entry["synonyms"] = syn[:12]
        if legacy:
            entry["source_columns"] = legacy[:5]
        cols.append((rel, bool(c.get("description")), entry))
    cols.sort(key=lambda x: (-x[0], not x[1]))
    total = len(cols)
    return [e for _, _, e in cols[:max_columns]], max(total - max_columns, 0)


def _model_tests(ix: SemanticIndex, uid: str) -> dict:
    tests = ix.tests_by_node.get(uid, [])
    statuses = defaultdict(int)
    for t in tests:
        statuses[str(t.get("latest_status") or "not_run").lower()] += 1
    return {"total": len(tests), "by_status": dict(statuses)}


def _model_locations(ix: SemanticIndex, uid: str) -> list[dict]:
    m = ix.models.get(uid) or {}
    locs = []
    if m.get("relation_name"):
        locs.append({"catalog": "dbt_relation", "fqn": m["relation_name"].replace('"', "").replace("`", ""),
                     "engines": [ix.destination.get("warehouse_type")] if ix.destination.get("warehouse_type") else []})
    return locs


def model_context(ix: SemanticIndex, uid: str, qt: set[str], max_columns: int) -> dict:
    m = ix.models[uid]
    meta = m.get("meta") or {}
    columns, more = _columns(ix, uid, qt, max_columns)
    return {
        "kind": "model",
        "unique_id": uid,
        "name": m.get("name"),
        "description": _short(m.get("description"), 600),
        "materialized": m.get("materialized"),
        "grain": meta.get("grain"),
        "owner": (m.get("governance") or {}).get("owner") or meta.get("owner"),
        "synonyms": _dedupe(_meta_synonyms(meta) + list(ix.ov["table_synonyms"].get((m.get("name") or "").lower(), []))),
        "columns": columns,
        "columns_truncated": more,
        "tests": _model_tests(ix, uid),
        "locations": _model_locations(ix, uid),
        "status": m.get("latest_status"),
    }


def _metric_measures(ix: SemanticIndex, m: dict) -> list[dict]:
    tp = m.get("type_params") or {}
    names = []
    for key in ("measure", "numerator", "denominator"):
        if tp.get(key):
            names.append((key, tp[key]))
    for im in tp.get("input_measures") or []:
        if all(im["name"] != n["name"] for _, n in names):
            names.append(("input", im))
    details = {}
    for sm in ix.semantic_models.values():
        for md in sm.get("measure_details") or []:
            details.setdefault(md.get("name"), {**md, "semantic_model": sm.get("name"),
                                                "relation_name": sm.get("relation_name")})
    out = []
    for role, ref in names:
        child = ix.metrics.get(ref["name"].lower()) if role in ("numerator", "denominator") else None
        if child:  # ratio metrics take metrics, not measures, as inputs
            cm = _metric_measures(ix, child) if child is not m else []
            out.append({"role": role, "metric": child.get("name"), "type": child.get("type"),
                        "filter": ref.get("filter") or child.get("filter") or None,
                        "measures": [{k: v for k, v in x.items() if k != "role"} for x in cm]})
            continue
        if role == "input" and any(o.get("metric") for o in out):
            continue  # already listed under the numerator/denominator metric
        d = details.get(ref["name"], {})
        out.append({"role": role, "name": ref["name"], "agg": d.get("agg"), "expr": d.get("expr") or ref["name"],
                    "semantic_model": d.get("semantic_model"), "filter": ref.get("filter"),
                    "agg_time_dimension": d.get("agg_time_dimension")})
    return out


def _semantic_models_for(ix: SemanticIndex, m: dict, depth: int = 0) -> list[dict]:
    sms = [ix.semantic_models[u] for u in m.get("semantic_model_unique_ids") or [] if u in ix.semantic_models]
    if not sms:  # older snapshots: resolve through measures / models
        model_ids = set(m.get("model_unique_ids") or [])
        sms = [s for s in ix.semantic_models.values() if set(s.get("model_unique_ids") or []) & model_ids]
    if depth < 3:
        for im in m.get("input_metric_unique_ids") or []:
            child = ix.metrics.get(im.split(".")[-1].lower())
            if child:
                sms += _semantic_models_for(ix, child, depth + 1)
    seen, out = set(), []
    for s in sms:
        if s["unique_id"] not in seen:
            seen.add(s["unique_id"])
            out.append(s)
    return out


def metric_context(ix: SemanticIndex, key: str, qt: set[str], max_dimensions: int) -> dict:
    m = ix.metrics[key]
    tp = m.get("type_params") or {}
    sms = _semantic_models_for(ix, m)
    dims, entities, grain = [], [], m.get("time_granularity")
    for sm in sms:
        prim = sm.get("primary_entity") or next(
            (e["name"] for e in sm.get("entities") or [] if e.get("type") == "primary"), None)
        for e in sm.get("entities") or []:
            entities.append({"name": e.get("name"), "type": e.get("type"), "semantic_model": sm.get("name")})
        for d in sm.get("dimensions") or []:
            ref = f"{prim}__{d['name']}" if prim else d["name"]
            dims.append({"name": d.get("name"), "group_by": ref, "type": d.get("type"),
                         "time_granularity": d.get("time_granularity"), "semantic_model": sm.get("name"),
                         "description": _short(d.get("description"), 120)})
            if d.get("name") == sm.get("agg_time_dimension") and not grain:
                grain = d.get("time_granularity")
    for d in dims:
        dt = _tokens((d["name"] or "").replace("_", " ")) | {
            t for s in ix.ov["synonyms"].get((d["name"] or "").lower(), []) for t in _tokens(s)}
        d["_rel"] = len(qt & dt)
    dims.sort(key=lambda d: (-d["_rel"], d["type"] != "time", d["name"] or ""))
    for d in dims:
        d.pop("_rel")
    mq = ix.metric_quality.get(key) or {}
    note = ix.ov["metric_notes"].get(key)
    definition = {k: tp[k] for k in ("expr", "metrics", "window", "grain_to_date", "conversion") if tp.get(k)}
    return {
        "kind": "metric",
        "name": m.get("name"),
        "label": m.get("label"),
        "type": m.get("type"),
        "description": _short((m.get("description") or "") + (f" {note}" if note else ""), 800),
        "definition": {
            **definition,
            "measures": _metric_measures(ix, m),
            "filter": m.get("filter") or [],
            "time_grain": grain,
            "agg_time_dimension": next((s.get("agg_time_dimension") for s in sms if s.get("agg_time_dimension")), None),
        },
        "query_with": {"metrics": [m.get("name")], "group_by": ["metric_time"] + [d["group_by"] for d in dims[:3]
                                                                                   if d["type"] != "time"]},
        "dimensions": dims[:max_dimensions],
        "dimensions_truncated": max(len(dims) - max_dimensions, 0),
        "entities": entities,
        "semantic_models": [s.get("name") for s in sms],
        "models": [ix.models[u]["name"] for u in m.get("model_unique_ids") or [] if u in ix.models],
        "synonyms": _dedupe(_meta_synonyms(m.get("meta")) + list(ix.ov["metric_synonyms"].get(key, []))),
        "trust": {"level": mq.get("trust_level", "unknown"), "failing_tests": mq.get("failing_tests"),
                  "unverified_tests": mq.get("unverified_tests"),
                  "upstream_object_count": mq.get("upstream_object_count")},
    }


def table_context(ix: SemanticIndex, key: str, qt: set[str], max_columns: int) -> dict:
    schema, name = key.split(".", 1)
    o = ix.obj_by_name[(schema, name)]
    cols = []
    for c in o.get("columns") or []:
        ct = _tokens((c.get("name") or "").replace("_", " "))
        entry = {"name": c.get("name"), "data_type": c.get("data_type"), "nullable": c.get("nullable"),
                 "is_primary_key": c.get("is_primary_key"), "description": _short(c.get("dbt_description"), 200)}
        if c.get("source_name") and _squash(c["source_name"]) != _squash(c.get("name")):
            entry["source_name"] = c["source_name"]
        cols.append((len(qt & ct), bool(c.get("is_primary_key")), entry))
    cols.sort(key=lambda x: (-x[0], not x[1]))
    lake = o.get("lake") or {}
    out = {
        "kind": "table",
        "object_id": o.get("object_id"),
        "name": f"{o.get('schema')}.{o.get('name')}",
        "risk_level": (o.get("dq_summary") or {}).get("risk_level"),
        "dbt_source": (o.get("dbt") or {}).get("source_unique_id"),
        "downstream_models": [ix.models[u]["name"] for u in (o.get("dbt") or {}).get("model_unique_ids") or []
                              if u in ix.models][:20],
        "columns": [e for _, _, e in cols[:max_columns]],
        "columns_truncated": max(len(cols) - max_columns, 0),
        "locations": physical_locations(ix.destination, o.get("schema"), o.get("name")),
    }
    if lake:
        out["lake"] = {k: lake.get(k) for k in ("table_format", "format_version", "partition_spec",
                                               "last_updated", "snapshot_at", "total_records", "catalogs")
                       if lake.get(k) is not None}
    return out


def semantic_context(doc: dict, query: str, *, kind: str = "auto", top_k: int = 2,
                     max_columns: int = 15, max_dimensions: int = 12, max_caveats: int = 15,
                     overrides: dict | None = None, index: SemanticIndex | None = None) -> dict:
    """One-call grounding payload for ``query`` (see module docstring)."""
    ix = index or SemanticIndex(doc, overrides)
    top_k = max(1, min(int(top_k or 2), 10))
    matches = ix.resolve(query, kind)
    qt = _tokens(query.split(":", 1)[-1] if query else "")
    chosen = matches[:top_k]

    subjects, nodes, metric_names = [], set(), set()
    for mt in chosen:
        if mt["kind"] == "metric":
            ctx = metric_context(ix, mt["key"], qt, max_dimensions)
            met = ix.metrics[mt["key"]]
            nodes |= set(met.get("model_unique_ids") or [])
            metric_names.add(met.get("name"))
            ctx["locations"] = [loc for u in met.get("model_unique_ids") or [] for loc in _model_locations(ix, u)]
        elif mt["kind"] == "model":
            ctx = model_context(ix, mt["key"], qt, max_columns)
            nodes.add(mt["key"])
        else:
            ctx = table_context(ix, mt["key"], qt, max_columns)
            src = ctx.get("dbt_source")
            if src:
                nodes.add(src)
        ctx["match_score"] = mt["score"]
        subjects.append(ctx)

    caveats = _caveats_for(ix, nodes) if nodes else []
    has_tests = any(ix.tests_by_node.get(n) for n in nodes | {a for n in nodes for a in ix.ancestors(n)})
    levels = [s["trust"]["level"] for s in subjects if s["kind"] == "metric"]
    level = _trust_level(caveats, has_tests)
    rank = {"at_risk": 3, "watch": 2, "unknown": 1, "trusted": 0}
    for lv in levels:  # metric trust from the snapshot can only make it worse
        if rank.get(lv, 1) > rank.get(level, 1):
            level = lv

    out = {
        "query": query,
        "snapshot": {"generated_at": ix.doc.get("generated_at"), "version": ix.doc.get("version"),
                     "data_mode": ix.data_mode,
                     "destination": {k: ix.destination.get(k) for k in ("service", "warehouse_type", "database")
                                     if ix.destination.get(k)} or None},
        "resolved_as": chosen[0]["kind"] if chosen else None,
        "subjects": subjects,
        "other_matches": [{"id": f"{m['kind']}:{m['key']}", "name": m["name"], "kind": m["kind"],
                           "score": m["score"]} for m in matches[top_k: top_k + 10]],
        "provenance": _provenance(ix, nodes),
        "trust": {"level": level, "caveats": caveats[:max_caveats],
                  "caveats_truncated": max(len(caveats) - max_caveats, 0)},
    }
    if ix.ov["guidance"]:
        out["guidance"] = ix.ov["guidance"]
    if not chosen:
        out["message"] = ("No metric, model or table matched. Call list_semantic_metrics or "
                          "list_warehouse_objects to browse, or rephrase with a metric/table name.")
    compact = _compact(out)
    for key in ("subjects", "other_matches", "provenance"):
        compact.setdefault(key, [])
    compact["trust"].setdefault("caveats", [])
    return compact


def _compact(value):
    """Drop None / empty containers recursively (keeps False and 0)."""
    if isinstance(value, dict):
        out = {k: _compact(v) for k, v in value.items()}
        return {k: v for k, v in out.items() if v is not None and v != [] and v != {}}
    if isinstance(value, list):
        return [_compact(v) for v in value]
    return value


def list_semantic_metrics(doc: dict, *, trust_level: str | None = None, search: str | None = None,
                          limit: int = 50, offset: int = 0, overrides: dict | None = None,
                          index: SemanticIndex | None = None) -> dict:
    """Compact catalog of Semantic Layer metrics (definition summary + trust)."""
    ix = index or SemanticIndex(doc, overrides)
    keys = sorted(ix.metrics)
    if search:
        ranked = [m["key"] for m in ix.resolve(search, "metric")]
        keys = [k for k in ranked if k in ix.metrics]
    rows = []
    for key in keys:
        m = ix.metrics[key]
        mq = ix.metric_quality.get(key) or {}
        if trust_level and mq.get("trust_level") != trust_level:
            continue
        tp = m.get("type_params") or {}
        rows.append({
            "name": m.get("name"), "label": m.get("label"), "type": m.get("type"),
            "description": _short(m.get("description"), 200),
            "measures": [r["name"] for r in (tp.get(k) for k in ("measure", "numerator", "denominator")) if r],
            "input_metrics": [x["name"] for x in tp.get("metrics") or []],
            "has_filter": bool(m.get("filter")),
            "time_granularity": m.get("time_granularity"),
            "models": [ix.models[u]["name"] for u in m.get("model_unique_ids") or [] if u in ix.models],
            "trust_level": mq.get("trust_level", "unknown"),
            "synonyms": _dedupe(_meta_synonyms(m.get("meta")) + list(ix.ov["metric_synonyms"].get(key, [])))[:6],
        })
    total = len(rows)
    limit = max(1, min(int(limit or 50), 200))
    page = rows[offset: offset + limit]
    return {"count": total, "returned": len(page), "offset": offset, "metrics": page,
            "next_offset": offset + limit if offset + limit < total else None}


def search_documents(doc: dict, query: str, *, limit: int = 10, base_url: str | None = None,
                     overrides: dict | None = None) -> list[dict]:
    """ChatGPT-style ``search`` results: ``[{id, title, url}]``."""
    ix = SemanticIndex(doc, overrides)
    out = []
    for m in ix.resolve(query, "auto")[:limit]:
        rid = f"{m['kind']}:{m['key']}"
        out.append({"id": rid, "title": f"{m['kind']}: {m['name']}", "url": document_url(rid, base_url)})
    return out


def document_url(rid: str, base_url: str | None) -> str:
    from urllib.parse import quote

    base = (base_url or "metadata-service://").rstrip("/")
    return f"{base}/semantic/context?query={quote(rid, safe=':')}"
