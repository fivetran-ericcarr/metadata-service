"""Capgemini DQaaS agent quickstart — a minimal MCP client that runs the
recommended triage flow against the metadata-service MCP server.

This is the ~50-line starting point a Data Quality agent builds on: orient with
one cheap call, triage to the objects that matter, then drill in to act.

Usage
-----
    pip install -e ".[mcp]"

    # local (spawns `metadata-service serve-mcp` over stdio)
    python examples/agent_quickstart.py

    # hosted (server started with: metadata-service serve-mcp --transport http)
    python examples/agent_quickstart.py --transport http --url http://localhost:8765/mcp

A snapshot must exist first (`metadata-service build ...`).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os


def _parse(result):
    """CallToolResult -> dict (prefer structured content, fall back to text)."""
    sc = getattr(result, "structuredContent", None)
    if sc:
        return sc
    for block in getattr(result, "content", []) or []:
        text = getattr(block, "text", None)
        if text:
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return text
    return {}


async def run(session) -> None:
    await session.initialize()
    tools = (await session.list_tools()).tools
    print("connected — tools:", ", ".join(t.name for t in tools))

    # 1) Orient: one cheap call describes the whole estate.
    summary = _parse(await session.call_tool("get_dq_summary", {}))
    print("\n[1] get_dq_summary")
    print(f"    {summary.get('object_count')} objects | matched {summary.get('matched')} | "
          f"missing coverage {summary.get('objects_missing_dbt_coverage')} | "
          f"failing {summary.get('objects_with_failing_tests')} | "
          f"recs {summary.get('recommendations', {}).get('total')}")

    # 2) Triage: which Fivetran tables have no dbt coverage?
    missing = _parse(await session.call_tool(
        "list_warehouse_objects", {"missing_coverage": True, "limit": 5}))
    print(f"\n[2] list_warehouse_objects(missing_coverage=true) -> "
          f"{missing.get('count')} (showing {missing.get('returned')})")
    for o in missing.get("objects", []):
        print(f"    - {o['schema']}.{o['name']}  risk={o['risk_level']}  "
              f"recommended_tests={o['recommended_tests_count']}")

    # 3) Drill into one matched object for full detail.
    obj = _parse(await session.call_tool(
        "get_warehouse_object", {"schema": "github", "table": "issue"}))
    if obj.get("name"):
        d = obj.get("dbt", {})
        print("\n[3] get_warehouse_object(github, issue)")
        print(f"    match={obj.get('match_confidence')} | source={d.get('source_unique_id')} | "
              f"models={len(d.get('model_unique_ids', []))} | tests={len(d.get('tests', []))} | "
              f"freshness={(d.get('freshness') or {}).get('status')}")

    # 4) Recommendations to act on (e.g. open a dbt PR, raise a ticket).
    recs = _parse(await session.call_tool(
        "get_dq_recommendations", {"schema": "github", "table": "issue"}))
    print(f"\n[4] get_dq_recommendations(github, issue) -> {recs.get('count')}")
    for r in recs.get("recommendations", [])[:5]:
        kind = r.get("recommendation_type")
        if kind == "dbt_test":
            tgt = r["target"].get("column") or r["target"]["table"]
            print(f"    + add dbt test '{r['test_name']}' on {tgt} ({r['confidence']})")
        elif kind == "risk":
            print(f"    ! risk {r['risk']} ({r['severity']})")
        else:
            print(f"    ~ signal {r.get('signal')}: {r.get('recommended_action')}")


async def main() -> None:
    ap = argparse.ArgumentParser(description="metadata-service MCP agent quickstart")
    ap.add_argument("--transport", choices=["stdio", "http"], default="stdio")
    ap.add_argument("--url", default="http://localhost:8765/mcp")
    args = ap.parse_args()

    from mcp import ClientSession

    if args.transport == "http":
        from mcp.client.streamable_http import streamablehttp_client

        async with streamablehttp_client(args.url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await run(session)
    else:
        from mcp import StdioServerParameters
        from mcp.client.stdio import stdio_client

        # Override with MCP_SERVER_CMD if `metadata-service` isn't on PATH.
        params = StdioServerParameters(
            command=os.environ.get("MCP_SERVER_CMD", "metadata-service"),
            args=["serve-mcp"],
            env=dict(os.environ),
        )
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await run(session)


if __name__ == "__main__":
    asyncio.run(main())
