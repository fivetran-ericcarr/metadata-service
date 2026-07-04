"""MCP server entrypoint.

Binds the SDK-independent functions in ``tools.py`` to the official Python MCP
SDK. If the ``mcp`` package is not installed, ``run_server`` raises a clear
error with installation instructions instead of failing obscurely.

Supports both stdio (local subprocess) and HTTP (streamable-http, for hosted /
remote agents) transports.
"""

from __future__ import annotations

import logging

from . import tools

logger = logging.getLogger(__name__)

_INSTALL_HINT = (
    "The MCP SDK is not installed. Enable the MCP server with:\n"
    "    pip install 'metadata-service[mcp]'\n"
    "Then run: metadata-service serve-mcp"
)


def build_server(host: str = "0.0.0.0", port: int = 8765):
    """Construct and return a configured FastMCP server instance.

    Raises ImportError (with install instructions) if the SDK is unavailable.
    ``host``/``port`` only apply to HTTP transports.
    """
    try:
        from mcp.server.fastmcp import FastMCP  # type: ignore
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise ImportError(_INSTALL_HINT) from exc

    server = FastMCP("fivetran-dbt-metadata", host=host, port=port)

    # --- Orientation / discovery (compact, agent-friendly) ----------------
    @server.tool()
    def get_dq_summary() -> dict:
        """Account-level DQ rollup: object counts by risk, missing coverage,
        failing tests, stale syncs, recommendations by type/confidence, and drift.
        The first call an agent makes to orient itself."""
        return tools.get_dq_summary()

    @server.tool()
    def list_warehouse_objects(
        schema: str | None = None,
        risk_level: str | None = None,
        missing_coverage: bool | None = None,
        failing_tests: bool | None = None,
        warn_test_failures: bool | None = None,
        stale: bool | None = None,
        limit: int | None = None,
    ) -> dict:
        """Compact, filterable index of warehouse objects for triage (small rows,
        no columns/tests). Filter by schema, risk_level (low|medium|high),
        missing_coverage, failing_tests, warn_test_failures (warn-severity tests
        firing while the run stays green), stale. get_warehouse_object for detail."""
        return tools.list_warehouse_objects(
            schema, risk_level, missing_coverage, failing_tests,
            warn_test_failures, stale, limit
        )

    # --- Detail -----------------------------------------------------------
    @server.tool()
    def get_warehouse_object(schema: str, table: str) -> dict:
        """Return a single warehouse object (full detail) by schema + table."""
        return tools.get_warehouse_object(schema, table)

    @server.tool()
    def get_impact(schema: str, table: str) -> dict:
        """Blast radius for an object: downstream dbt models + exposures
        (dashboards/ML/apps) that depend on it. 'What breaks if this is wrong?'"""
        return tools.get_impact(schema, table)

    @server.tool()
    def get_column_impact(schema: str, table: str, column: str) -> dict:
        """Column-level blast radius: downstream model columns a Fivetran column
        feeds (via parsed SQL lineage), plus affected metrics and exposures."""
        return tools.get_column_impact(schema, table, column)

    @server.tool()
    def list_metrics() -> dict:
        """Semantic Layer metrics with a trust level (trusted|watch|at_risk) from
        the DQ posture of their upstream objects."""
        return tools.list_metrics()

    @server.tool()
    def get_metric_quality(metric: str) -> dict:
        """Trust detail for one governed metric: upstream objects + failing tests."""
        return tools.get_metric_quality(metric)

    # --- Activations (reverse ETL) ----------------------------------------
    @server.tool()
    def list_activations(verdict: str | None = None) -> dict:
        """Reverse-ETL activations with a readiness verdict (allow|warn|block|
        unknown): what data is being pushed back to operational systems, and is
        any of it unsafe? Filter by verdict."""
        return tools.list_activations(verdict)

    @server.tool()
    def get_activation_readiness(sync_id: str | None = None, label: str | None = None) -> dict:
        """Readiness detail for one activation: verdict + upstream reasons
        (failing/warn tests, staleness, missing contract) + destination field
        mappings. 'Is it safe to push this data back to prod?'"""
        return tools.get_activation_readiness(sync_id, label)

    @server.tool()
    def get_dq_recommendations(
        schema: str | None = None,
        table: str | None = None,
        recommendation_type: str | None = None,
        confidence: str | None = None,
        risk: str | None = None,
        limit: int | None = None,
    ) -> dict:
        """DQ recommendations, filterable per-object (schema/table) or across the
        whole snapshot by recommendation_type (dbt_test|risk|signal),
        confidence (high|medium|heuristic), or risk."""
        return tools.get_dq_recommendations(
            schema, table, recommendation_type, confidence, risk, limit
        )

    @server.tool()
    def get_schema_drift(
        schema: str | None = None,
        table: str | None = None,
        severity: str | None = None,
    ) -> dict:
        """Return schema drift records, optionally filtered by object and severity."""
        return tools.get_schema_drift(schema, table, severity)

    # --- Raw / bulk (use sparingly; large payloads) -----------------------
    @server.tool()
    def get_latest_metadata(scope: str = "all") -> dict:
        """Return the full normalized snapshot. scope: all|fivetran|dbt|
        warehouse_objects. Large — prefer get_dq_summary / list_warehouse_objects."""
        return tools.get_latest_metadata(scope)

    # --- Action -----------------------------------------------------------
    @server.tool()
    def refresh_metadata(
        fivetran_group_id: str | None = None,
        include_fivetran: bool = True,
        include_dbt: bool = True,
    ) -> dict:
        """Run extraction + normalization and write a new latest snapshot."""
        return tools.refresh_metadata(fivetran_group_id, include_fivetran, include_dbt)

    return server


def run_server(transport: str = "stdio", host: str = "0.0.0.0", port: int = 8765) -> None:
    """Build and run the MCP server.

    transport: ``stdio`` (local subprocess) | ``http``/``streamable-http`` |
    ``sse`` (HTTP transports use host/port).
    """
    try:
        server = build_server(host=host, port=port)
    except ImportError as exc:
        logger.error(str(exc))
        raise

    normalized = {"http": "streamable-http", "streamable-http": "streamable-http",
                  "sse": "sse", "stdio": "stdio"}.get(transport, "stdio")
    if normalized == "stdio":
        logger.info("Starting MCP server over stdio")
    else:
        logger.info("Starting MCP server over %s on %s:%s", normalized, host, port)
    server.run(transport=normalized)
