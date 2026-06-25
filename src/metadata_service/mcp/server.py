"""MCP server entrypoint.

Binds the SDK-independent functions in ``tools.py`` to the official Python MCP
SDK. If the ``mcp`` package is not installed, ``run_server`` raises a clear
error with installation instructions instead of failing obscurely.
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


def build_server():
    """Construct and return a configured FastMCP server instance.

    Raises ImportError (with install instructions) if the SDK is unavailable.
    """
    try:
        from mcp.server.fastmcp import FastMCP  # type: ignore
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise ImportError(_INSTALL_HINT) from exc

    server = FastMCP("fivetran-dbt-metadata")

    @server.tool()
    def refresh_metadata(
        fivetran_group_id: str | None = None,
        include_fivetran: bool = True,
        include_dbt: bool = True,
    ) -> dict:
        """Run extraction + normalization and write a new latest snapshot."""
        return tools.refresh_metadata(fivetran_group_id, include_fivetran, include_dbt)

    @server.tool()
    def get_latest_metadata(scope: str = "all") -> dict:
        """Return the latest normalized metadata. scope: all|fivetran|dbt|warehouse_objects."""
        return tools.get_latest_metadata(scope)

    @server.tool()
    def get_warehouse_object(schema: str, table: str) -> dict:
        """Return a single warehouse object by schema + table, or a not-found message."""
        return tools.get_warehouse_object(schema, table)

    @server.tool()
    def get_dq_recommendations(schema: str, table: str) -> dict:
        """Return DQ recommendations for a single warehouse object."""
        return tools.get_dq_recommendations(schema, table)

    @server.tool()
    def get_schema_drift(
        schema: str | None = None,
        table: str | None = None,
        severity: str | None = None,
    ) -> dict:
        """Return schema drift records, optionally filtered by object and severity."""
        return tools.get_schema_drift(schema, table, severity)

    return server


def run_server() -> None:
    """Build and run the MCP server over stdio."""
    try:
        server = build_server()
    except ImportError as exc:
        logger.error(str(exc))
        raise
    server.run()
