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


def build_server(host: str = "127.0.0.1", port: int = 8765):
    """Construct and return a configured FastMCP server instance.

    Raises ImportError (with install instructions) if the SDK is unavailable.
    ``host``/``port`` only apply to HTTP transports. The default bind is
    loopback: FastMCP's HTTP transports have no authentication, so remote
    exposure should go through an authenticating reverse proxy.
    """
    try:
        from mcp.server.fastmcp import FastMCP  # type: ignore
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise ImportError(_INSTALL_HINT) from exc

    import anyio

    from .tools import MetadataUnavailable

    server = FastMCP("fivetran-dbt-metadata", host=host, port=port)

    async def _threaded(fn, *args):
        """Run a blocking tool (storage read + full-document JSON parse) in a
        worker thread so a slow read doesn't freeze every other session on the
        HTTP transports — the read tools are otherwise invoked inline on the
        event loop.

        Error detail is sanitized before it leaves the server: a Metadata
        Unavailable message is curated and safe to show, but any other exception
        (e.g. a StorageError embedding a filesystem path, or an httpx error with
        an internal host) is logged server-side and replaced with a generic
        message — the HTTP transports may be reachable by untrusted clients."""
        try:
            return await anyio.to_thread.run_sync(lambda: fn(*args))
        except MetadataUnavailable:
            raise  # safe, curated message
        except Exception as exc:
            logger.exception("MCP tool %s failed", getattr(fn, "__name__", fn))
            raise RuntimeError(f"{getattr(fn, '__name__', 'tool')} failed; see server logs.") from exc

    # --- Orientation / discovery (compact, agent-friendly) ----------------
    @server.tool()
    async def get_dq_summary() -> dict:
        """Account-level DQ rollup: object counts by risk, missing coverage,
        failing tests, stale syncs, recommendations by type/confidence, and drift.
        The first call an agent makes to orient itself."""
        return await _threaded(tools.get_dq_summary)

    @server.tool()
    async def list_warehouse_objects(
        schema: str | None = None,
        risk_level: str | None = None,
        missing_coverage: bool | None = None,
        failing_tests: bool | None = None,
        warn_test_failures: bool | None = None,
        stale: bool | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> dict:
        """Compact, filterable index of warehouse objects for triage (small rows,
        no columns/tests). Filter by schema, risk_level (low|medium|high),
        missing_coverage, failing_tests, warn_test_failures (warn-severity tests
        firing while the run stays green), stale. limit+offset paginate.
        get_warehouse_object for detail."""
        return await _threaded(
            tools.list_warehouse_objects, schema, risk_level, missing_coverage,
            failing_tests, warn_test_failures, stale, limit, offset
        )

    # --- Detail -----------------------------------------------------------
    @server.tool()
    async def get_warehouse_object(schema: str, table: str) -> dict:
        """Return a single warehouse object (full detail) by schema + table."""
        return await _threaded(tools.get_warehouse_object, schema, table)

    @server.tool()
    async def get_impact(schema: str, table: str) -> dict:
        """Blast radius for an object: downstream dbt models + exposures
        (dashboards/ML/apps) that depend on it. 'What breaks if this is wrong?'"""
        return await _threaded(tools.get_impact, schema, table)

    @server.tool()
    async def get_column_impact(schema: str, table: str, column: str) -> dict:
        """Column-level blast radius: downstream model columns a Fivetran column
        feeds (via parsed SQL lineage), plus affected metrics and exposures."""
        return await _threaded(tools.get_column_impact, schema, table, column)

    @server.tool()
    async def list_metrics() -> dict:
        """Semantic Layer metrics with a trust level (trusted|watch|at_risk) from
        the DQ posture of their upstream objects."""
        return await _threaded(tools.list_metrics)

    @server.tool()
    async def get_metric_quality(metric: str) -> dict:
        """Trust detail for one governed metric: upstream objects + failing tests."""
        return await _threaded(tools.get_metric_quality, metric)

    # --- Activations (reverse ETL) ----------------------------------------
    @server.tool()
    async def list_activations(verdict: str | None = None) -> dict:
        """Reverse-ETL activations with a readiness verdict (allow|warn|block|
        unknown): what data is being pushed back to operational systems, and is
        any of it unsafe? Filter by verdict."""
        return await _threaded(tools.list_activations, verdict)

    @server.tool()
    async def get_activation_readiness(sync_id: str | None = None, label: str | None = None,
                                       schema: str | None = None, table: str | None = None) -> dict:
        """Readiness detail for one activation: verdict + upstream reasons
        (failing/warn tests, staleness, missing contract) + destination field
        mappings. Address by sync_id, label, or the warehouse schema+table the
        sync reads. 'Is it safe to push this data back to prod?'"""
        return await _threaded(tools.get_activation_readiness, sync_id, label, schema, table)

    @server.tool()
    async def get_dq_recommendations(
        schema: str | None = None,
        table: str | None = None,
        recommendation_type: str | None = None,
        confidence: str | None = None,
        risk: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> dict:
        """DQ recommendations, filterable per-object (schema/table) or across the
        whole snapshot by recommendation_type (dbt_test|risk|signal),
        confidence (high|medium|heuristic), or risk. limit+offset paginate."""
        return await _threaded(
            tools.get_dq_recommendations, schema, table, recommendation_type,
            confidence, risk, limit, offset
        )

    @server.tool()
    async def get_schema_drift(
        schema: str | None = None,
        table: str | None = None,
        severity: str | None = None,
    ) -> dict:
        """Return schema drift records, optionally filtered by object and severity."""
        return await _threaded(tools.get_schema_drift, schema, table, severity)

    # --- Raw / bulk (use sparingly; large payloads) -----------------------
    @server.tool()
    async def get_latest_metadata(scope: str = "all") -> dict:
        """Snapshot by scope. all = joined doc with raw source payloads replaced
        by counts (context-safe); fivetran|dbt = one raw section;
        warehouse_objects = the join; full = the verbatim ~1 MB document.
        Prefer get_dq_summary / list_warehouse_objects for triage."""
        return await _threaded(tools.get_latest_metadata, scope)

    # --- Action -----------------------------------------------------------
    @server.tool()
    async def refresh_metadata(
        fivetran_group_id: str | None = None,
        include_fivetran: bool = True,
        include_dbt: bool = True,
        include_activations: bool = True,
        dbt_project_id: int | None = None,
        connected_only: bool = False,
        skip_paused: bool = False,
    ) -> dict:
        """Run extraction + normalization and write a new latest snapshot. Accepts
        the same scoping as the CLI build (group, dbt project, filters) so an
        agent-triggered refresh matches the scheduled one. Long-running (minutes
        on live accounts); returns {status: in_progress_error} if already running."""
        from ..exceptions import RefreshInProgressError

        try:
            # Extraction takes minutes; run it in a worker thread so one refresh
            # doesn't freeze every other session on the HTTP transports.
            return await anyio.to_thread.run_sync(
                lambda: tools.refresh_metadata(
                    fivetran_group_id, include_fivetran, include_dbt,
                    include_activations, dbt_project_id, connected_only, skip_paused,
                )
            )
        except RefreshInProgressError:
            return {"status": "in_progress_error",
                    "message": "A refresh is already running; try again when it finishes."}
        except Exception:
            # Don't leak build internals (paths, hosts, tracebacks) to the client;
            # log server-side and return a generic status (mirrors the REST layer).
            logger.exception("MCP refresh_metadata failed")
            return {"status": "error", "message": "Refresh failed; see server logs."}

    return server


def run_server(transport: str = "stdio", host: str = "127.0.0.1", port: int = 8765) -> None:
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
