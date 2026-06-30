# Example agent transcript

A simulated Data Quality agent answering questions over the live GitHub →
Fivetran → Snowflake → dbt reference build, using the metadata-service MCP tools.
Tool results are abbreviated; numbers are from the real snapshot (110 objects, 7
modeled in dbt, 0 failing tests). See [docs/use-cases/github-snowflake-dbt.md](../docs/use-cases/github-snowflake-dbt.md).

---

**User:** What are the biggest data-quality problems right now?

**Agent** → `get_dq_summary()`
```json
{ "object_count": 110, "matched": 7, "objects_missing_dbt_coverage": 103,
  "objects_with_failing_tests": 0,
  "recommendations": { "by_risk": { "missing_dbt_coverage": 100,
    "missing_model_contract": 4, "unowned_object": 4 } } }
```
**Agent:** Three things, in order of impact:
1. **Coverage** is the dominant gap — **103 of 110 replicated tables have no dbt
   tests**, so their quality is unverified.
2. **Governance** — 4 modeled objects have no enforced contract and 4 have no owner.
3. **Sensitivity** — 6 columns are flagged as potential PII.
No tests are currently failing, so there's no active breakage — the risk is
*unknown* quality on uncovered data.

---

**User:** How many users signed up with incomplete or invalid information?

**Agent** → `get_warehouse_object("github", "user")`
```json
{ "dbt": { "tests": [
  {"test_type":"not_null","attached_column":"user_login","status":"success"},
  {"test_type":"unique","attached_column":"user_id","status":"success"} ] } }
```
**Agent:** Completeness is asserted and **passing**: every user has a non-null
`login` and a unique id — so zero users with a missing login or duplicate identity.
There's no test on `email` yet, so I can't quantify email completeness — I'd
recommend a `not_null` test on `user.email` (note it's also flagged as PII). If a
rule *were* failing, dbt reports the exact count of violating rows in the test's
`failures` field.

---

**User:** If we drop or hash `issue.user_id`, what breaks?

**Agent** → `get_column_impact("github", "issue", "user_id")`
```json
{ "affected_columns": [ {"unique_id":"model.github_dq.stg_github__issue","column":"user_id"} ],
  "exposures": [ {"name":"github_repo_health_dashboard","type":"dashboard"},
                 {"name":"issue_triage_ml","type":"ml"} ] }
```
**Agent:** It flows into `stg_github__issue.user_id`, which feeds **two business
consumers**: the *Repo Health* dashboard and the *Issue Triage* ML model. Treat any
change to this column as a change-controlled event and notify both owners.

---

**User:** Can we trust the `open_issue_rate` metric?

**Agent** → `get_metric_quality("open_issue_rate")`
```json
{ "metric": "open_issue_rate", "trust_level": "watch", "upstream_object_count": 3,
  "upstream_objects": ["…/github/issue","…/github/pull_request","…/github/repository"],
  "failing_tests": 0 }
```
**Agent:** **Watch.** Its three upstream tables are tested with **no failures**, so
the number isn't wrong — but they still carry open test recommendations, so it's not
fully `trusted` yet. Close those recommendations and it moves to `trusted`.

---

**User:** Which sensitive columns are exposed un-hashed, and which models break governance?

**Agent** → `get_dq_recommendations(recommendation_type="signal")` and `get_dq_recommendations(risk="missing_model_contract")`
```json
{ "signals": ["commit.author_email","commit.committer_email","user.email","user.phone","user_email.email"],
  "missing_model_contract": 4, "unowned_object": 4 }
```
**Agent:** Five PII-named columns are un-hashed (emails + a phone) — review for
masking. On governance, the four staging-only paths (e.g. `label`,
`pull_request_review`) have **no enforced contract and no owner**; the mart path is
contracted, public, and owned by the Analytics Team.

---

> Pattern: the metadata-service **locates and explains** the risk. For raw row
> counts the agent reads a failing test's `failures` value or queries the warehouse
> directly; to act, it opens a dbt PR (add the recommended test) or files a ticket
> routed by the object's owner.
