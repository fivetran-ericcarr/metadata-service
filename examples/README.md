# Examples

Runnable starting points for consuming the metadata service.

| File | What it shows | Needs |
|---|---|---|
| [`agent_quickstart.py`](agent_quickstart.py) | A minimal **MCP agent** running the triage flow: orient → find gaps → drill in → act | `pip install -e ".[mcp]"` + a snapshot |
| [`rest_quickstart.sh`](rest_quickstart.sh) | The same metadata over the **REST API** (curl) | `metadata-service serve-api` |
| [`demo.sh`](demo.sh) | A self-contained **offline demo** (fixtures, no credentials) — ideal for screen recording | `pip install -e ".[dev,mcp]"` |
| [`agent_transcript.md`](agent_transcript.md) | A read-through **agent chat** answering real DQ questions with tool calls + answers (demo data) | — |
| [`aliases.example.json`](aliases.example.json) | Sample alias map for `build --aliases-file` | — |

## Agent quickstart (MCP)

```bash
pip install -e ".[mcp]"

# Local (spawns the server over stdio):
python examples/agent_quickstart.py

# Hosted (server started elsewhere with: metadata-service serve-mcp --transport http):
python examples/agent_quickstart.py --transport http --url http://localhost:8765/mcp
```

If `metadata-service` isn't on your PATH, set `MCP_SERVER_CMD=/full/path/to/metadata-service`.

Expected output (against the GitHub reference build):

```text
connected — tools: get_dq_summary, list_warehouse_objects, get_warehouse_object, ...
[1] get_dq_summary
    76 objects | matched 7 | missing coverage 69 | failing 0 | recs 223
[2] list_warehouse_objects(missing_coverage=true) -> 69 (showing 5)
    - github.asset  risk=medium  recommended_tests=4
    ...
[3] get_warehouse_object(github, issue)
    match=exact_schema_table | source=source.github_dq.github.issue | models=2 | tests=12 | freshness=pass
[4] get_dq_recommendations(github, issue) -> 2
    + add dbt test 'accepted_values' on state_reason (heuristic)
    + add dbt test 'relationships' on milestone_id (heuristic)
```

## Offline demo (no credentials)

```bash
./examples/demo.sh
```

Builds a snapshot from bundled fixtures and walks recommendations → drift → the
agent flow. Swap `--fixtures-dir tests/fixtures` for live credentials in
production.

See the [Capgemini Quickstart](../docs/capgemini-quickstart.md) for the full
picture: what the service does, how to use it, and where it fits in a DQaaS
architecture.
