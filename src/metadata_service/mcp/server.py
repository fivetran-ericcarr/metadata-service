"""MCP server entrypoint.

Binds the SDK-independent functions in ``tools.py`` to the official Python MCP
SDK. If the ``mcp`` package is not installed, ``run_server`` raises a clear
error with installation instructions instead of failing obscurely.

Supports stdio (local subprocess) and HTTP (Streamable HTTP, for hosted /
remote agents such as Claude connectors, ChatGPT, Gemini/ADK, Snowflake Cortex
and Databricks) transports. HTTP deployments get a read-only tool allowlist by
default and bearer-key or OAuth resource-server auth (see ``auth.py`` and
docs/any-agent-mcp.md).
"""

from __future__ import annotations

import logging
from typing import Any

from . import tools

logger = logging.getLogger(__name__)

_INSTALL_HINT = (
    "The MCP SDK is not installed. Enable the MCP server with:\n"
    "    pip install 'metadata-service[mcp]'\n"
    "Then run: metadata-service serve-mcp"
)


#: Tools that only read the snapshot. Everything an external agent gets by default.
READ_ONLY_TOOLS = (
    "get_semantic_context", "list_semantic_metrics", "search", "fetch",
    "get_dq_summary", "list_warehouse_objects", "get_warehouse_object", "get_impact",
    "get_column_impact", "list_metrics", "get_metric_quality", "list_activations",
    "get_activation_readiness", "get_dq_recommendations", "get_schema_drift",
    "get_latest_metadata",
)
#: Tools with side effects (billed extraction, snapshot writes).
WRITE_TOOLS = ("refresh_metadata",)
ALL_TOOLS = READ_ONLY_TOOLS + WRITE_TOOLS


def resolve_tool_allowlist(spec: str | None, transport: str = "stdio") -> list[str]:
    """Tool names to register. ``auto``: all on stdio, read-only on HTTP."""
    raw = (spec or "auto").strip().lower()
    if raw == "auto":
        raw = "all" if transport == "stdio" else "read-only"
    if raw == "all":
        return list(ALL_TOOLS)
    if raw in ("read-only", "readonly", "read_only"):
        return list(READ_ONLY_TOOLS)
    names = [n.strip() for n in (spec or "").split(",") if n.strip()]
    unknown = [n for n in names if n not in ALL_TOOLS]
    if unknown:
        logger.warning("MCP_TOOLS names unknown tools (ignored): %s", ", ".join(unknown))
    return [n for n in ALL_TOOLS if n in names]


def _transport_security(settings, host: str):
    """DNS-rebinding protection (Host/Origin allowlist) for the HTTP transports.

    Configured hosts (``MCP_ALLOWED_HOSTS``, the ``MCP_PUBLIC_URL`` host) are
    always allowed, plus loopback — so a tunnel (ngrok, cloudflared) to a
    loopback bind works. With nothing configured: FastMCP's loopback-only
    default on a loopback bind; on a public bind behind a load balancer (Cloud
    Run), Host validation is off and per-request auth is the control.
    """
    from urllib.parse import urlparse

    from mcp.server.transport_security import TransportSecuritySettings

    hosts = [h.strip() for h in (settings.mcp_allowed_hosts or "").split(",") if h.strip()]
    if settings.mcp_public_url:
        netloc = urlparse(settings.mcp_public_url).netloc
        if netloc:
            hosts.append(netloc)
    loopback = host in ("127.0.0.1", "localhost", "::1")
    if not hosts:
        return None if loopback else TransportSecuritySettings(enable_dns_rebinding_protection=False)
    local = ["127.0.0.1:*", "localhost:*", "[::1]:*"]
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=hosts + local,
        allowed_origins=[f"https://{h}" for h in hosts] + [f"http://{h}" for h in local])


def _oauth(settings):
    """(AuthSettings, verifier) for OAuth resource-server mode."""
    from mcp.server.auth.settings import AuthSettings

    from .auth import CompositeVerifier, JWTVerifier, StaticKeyVerifier

    if not settings.mcp_oauth_issuer or not settings.mcp_public_url:
        raise ValueError("MCP_AUTH=oauth needs MCP_OAUTH_ISSUER and MCP_PUBLIC_URL "
                         "(the public https URL of the /mcp endpoint).")
    scopes = [x for x in (settings.mcp_oauth_required_scopes or "").split() if x] or None
    jwt_v = JWTVerifier(settings.mcp_oauth_issuer, audience=settings.mcp_oauth_audience,
                        jwks_url=settings.mcp_oauth_jwks_url, required_scopes=scopes)
    keys = settings.mcp_bearer_keys()
    verifier = CompositeVerifier(StaticKeyVerifier(keys) if keys else None, jwt_v)
    auth = AuthSettings(issuer_url=settings.mcp_oauth_issuer,
                        resource_server_url=settings.mcp_public_url, required_scopes=scopes)
    return auth, verifier


def build_server(host: str = "127.0.0.1", port: int = 8765, *, transport: str = "stdio",
                 settings=None, tools_spec: str | None = None):
    """Construct and return a configured FastMCP server instance.

    Raises ImportError (with install instructions) if the SDK is unavailable.
    ``host``/``port`` only apply to HTTP transports. ``transport`` selects the
    default tool allowlist (``MCP_TOOLS=auto``: everything on stdio, the
    read-only set on HTTP) and, for HTTP with ``MCP_AUTH=oauth``, wires the
    OAuth resource-server auth. Static API-key auth is applied around the ASGI
    app by :func:`build_http_app`.
    """
    try:
        from mcp.server.fastmcp import FastMCP  # type: ignore
        from mcp.types import ToolAnnotations
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise ImportError(_INSTALL_HINT) from exc

    import anyio

    from ..config import get_settings
    from .auth import resolve_auth_mode
    from .tools import MetadataUnavailable

    settings = settings or get_settings()
    kwargs: dict = {"host": host, "port": port}
    if transport != "stdio":
        kwargs["stateless_http"] = settings.mcp_stateless
        kwargs["json_response"] = settings.mcp_stateless
        security = _transport_security(settings, host)
        if security is not None:
            kwargs["transport_security"] = security
        if resolve_auth_mode(settings) == "oauth":
            kwargs["auth"], kwargs["token_verifier"] = _oauth(settings)
    server = FastMCP("fivetran-dbt-metadata", **kwargs)
    allowed = set(resolve_tool_allowlist(tools_spec if tools_spec is not None else settings.mcp_tools,
                                         transport))
    registered: list[str] = []

    def tool(fn):
        """Register ``fn`` if allowlisted, annotated read-only unless a write tool."""
        name = fn.__name__
        if name in allowed:
            read_only = name not in WRITE_TOOLS
            server.add_tool(fn, name=name, annotations=ToolAnnotations(
                readOnlyHint=read_only, destructiveHint=False, idempotentHint=read_only,
                openWorldHint=not read_only), structured_output=True)
            registered.append(name)
        return fn

    @server.custom_route("/health", methods=["GET"])
    async def health(_request):  # unauthenticated: Cloud Run / LB health checks
        from starlette.responses import JSONResponse

        return JSONResponse({"status": "ok", "tools": len(registered)})

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
            return await anyio.to_thread.run_sync(lambda: fn(*args, settings=settings))
        except MetadataUnavailable:
            raise  # safe, curated message
        except Exception as exc:
            logger.exception("MCP tool %s failed", getattr(fn, "__name__", fn))
            raise RuntimeError(f"{getattr(fn, '__name__', 'tool')} failed; see server logs.") from exc

    # --- Semantic context (grounding for any agent) ----------------------
    @tool
    async def get_semantic_context(query: str, kind: str = "auto", top_k: int = 2,
                                   max_columns: int = 15) -> dict[str, Any]:
        """Ground a question in ONE call. query = a dbt Semantic Layer metric,
        dbt model, schema.table, or a natural-language topic ("written premium
        by state"). Returns for the top matches: metric definition (measures,
        agg/expr, filters, time grain, group_by dimensions), model/column
        descriptions with synonyms and legacy source column names, Fivetran
        provenance (connection, last sync, live vs fixtures), DQ trust
        (level + caveats from failing/warn tests, stale syncs, quarantine) and
        the physical table per engine/catalog. kind = auto|topic|metric|model|
        table; top_k (1-10) expanded matches; other matches listed by id."""
        return await _threaded(tools.get_semantic_context, query, kind, top_k, max_columns)

    @tool
    async def list_semantic_metrics(trust_level: str | None = None, search: str | None = None,
                                    limit: int = 50, offset: int = 0) -> dict[str, Any]:
        """Catalog of governed dbt Semantic Layer metrics: type, measures,
        input metrics, models, synonyms and trust_level (trusted|watch|at_risk|
        unknown). Filter by trust_level or a search phrase; paginate with
        limit/offset (next_offset). get_semantic_context for full detail."""
        return await _threaded(tools.list_semantic_metrics, trust_level, search, limit, offset)

    @tool
    async def search(query: str) -> dict[str, Any]:
        """Search governed metrics, dbt models and synced tables by name,
        synonym or topic. Returns {results: [{id, title, url}]}; pass an id to
        fetch for the full semantic context."""
        return await _threaded(tools.search, query)

    @tool
    async def fetch(id: str) -> dict[str, Any]:  # noqa: A002 - name fixed by the search/fetch convention
        """Fetch one search result by id (e.g. "metric:bind_rate",
        "table:salesforce.account"): {id, title, text, url, metadata}, where
        text is the semantic context JSON (definition, provenance, trust)."""
        return await _threaded(tools.fetch, id)

    # --- Orientation / discovery (compact, agent-friendly) ----------------
    @tool
    async def get_dq_summary() -> dict[str, Any]:
        """Account-level DQ rollup: object counts by risk, missing coverage,
        failing tests, stale syncs, recommendations by type/confidence, and drift.
        The first call an agent makes to orient itself."""
        return await _threaded(tools.get_dq_summary)

    @tool
    async def list_warehouse_objects(
        schema: str | None = None,
        risk_level: str | None = None,
        missing_coverage: bool | None = None,
        failing_tests: bool | None = None,
        warn_test_failures: bool | None = None,
        stale: bool | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> dict[str, Any]:
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
    @tool
    async def get_warehouse_object(schema: str, table: str) -> dict[str, Any]:
        """Return a single warehouse object (full detail) by schema + table."""
        return await _threaded(tools.get_warehouse_object, schema, table)

    @tool
    async def get_impact(schema: str, table: str) -> dict[str, Any]:
        """Blast radius for an object: downstream dbt models + exposures
        (dashboards/ML/apps) that depend on it. 'What breaks if this is wrong?'"""
        return await _threaded(tools.get_impact, schema, table)

    @tool
    async def get_column_impact(schema: str, table: str, column: str) -> dict[str, Any]:
        """Column-level blast radius: downstream model columns a Fivetran column
        feeds (via parsed SQL lineage), plus affected metrics and exposures."""
        return await _threaded(tools.get_column_impact, schema, table, column)

    @tool
    async def list_metrics() -> dict[str, Any]:
        """Semantic Layer metrics with a trust level (trusted|watch|at_risk) from
        the DQ posture of their upstream objects."""
        return await _threaded(tools.list_metrics)

    @tool
    async def get_metric_quality(metric: str) -> dict[str, Any]:
        """Trust detail for one governed metric: upstream objects + failing tests."""
        return await _threaded(tools.get_metric_quality, metric)

    # --- Activations (reverse ETL) ----------------------------------------
    @tool
    async def list_activations(verdict: str | None = None) -> dict[str, Any]:
        """Reverse-ETL activations with a readiness verdict (allow|warn|block|
        unknown): what data is being pushed back to operational systems, and is
        any of it unsafe? Filter by verdict."""
        return await _threaded(tools.list_activations, verdict)

    @tool
    async def get_activation_readiness(sync_id: str | None = None, label: str | None = None,
                                       schema: str | None = None, table: str | None = None) -> dict[str, Any]:
        """Readiness detail for one activation: verdict + upstream reasons
        (failing/warn tests, staleness, missing contract) + destination field
        mappings. Address by sync_id, label, or the warehouse schema+table the
        sync reads. 'Is it safe to push this data back to prod?'"""
        return await _threaded(tools.get_activation_readiness, sync_id, label, schema, table)

    @tool
    async def get_dq_recommendations(
        schema: str | None = None,
        table: str | None = None,
        recommendation_type: str | None = None,
        confidence: str | None = None,
        risk: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> dict[str, Any]:
        """DQ recommendations, filterable per-object (schema/table) or across the
        whole snapshot by recommendation_type (dbt_test|risk|signal),
        confidence (high|medium|heuristic), or risk. limit+offset paginate."""
        return await _threaded(
            tools.get_dq_recommendations, schema, table, recommendation_type,
            confidence, risk, limit, offset
        )

    @tool
    async def get_schema_drift(
        schema: str | None = None,
        table: str | None = None,
        severity: str | None = None,
    ) -> dict[str, Any]:
        """Return schema drift records, optionally filtered by object and severity."""
        return await _threaded(tools.get_schema_drift, schema, table, severity)

    # --- Raw / bulk (use sparingly; large payloads) -----------------------
    @tool
    async def get_latest_metadata(scope: str = "all") -> dict[str, Any]:
        """Snapshot by scope. all = joined doc with raw source payloads replaced
        by counts (context-safe); fivetran|dbt = one raw section;
        warehouse_objects = the join; full = the verbatim ~1 MB document.
        Prefer get_dq_summary / list_warehouse_objects for triage."""
        return await _threaded(tools.get_latest_metadata, scope)

    # --- Action -----------------------------------------------------------
    @tool
    async def refresh_metadata(
        fivetran_group_id: str | None = None,
        include_fivetran: bool = True,
        include_dbt: bool = True,
        include_activations: bool = True,
        dbt_project_id: int | None = None,
        connected_only: bool = False,
        skip_paused: bool = False,
    ) -> dict[str, Any]:
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

    server.registered_tools = registered  # type: ignore[attr-defined]
    logger.info("MCP tools registered (%s): %s", transport, ", ".join(registered))
    return server


def build_http_app(settings=None, *, host: str = "127.0.0.1", port: int = 8765, tools_spec=None):
    """Streamable HTTP ASGI app with the configured auth (for uvicorn / tests).

    ``api_key`` mode wraps the app in :class:`ApiKeyMiddleware`; ``oauth`` mode
    is enforced inside FastMCP (``RequireAuthMiddleware`` + protected-resource
    metadata at ``/.well-known/oauth-protected-resource``); ``none`` serves
    unauthenticated (only behind a proxy that authenticates).
    """
    from ..config import get_settings
    from .auth import ApiKeyMiddleware, resolve_auth_mode

    settings = settings or get_settings()
    server = build_server(host=host, port=port, transport="streamable-http", settings=settings,
                          tools_spec=tools_spec)
    app = server.streamable_http_app()
    mode = resolve_auth_mode(settings)
    if mode == "api_key":
        keys = settings.mcp_bearer_keys()
        if not keys:
            raise ValueError("MCP_AUTH=api_key needs METADATA_API_KEY or MCP_API_KEYS.")
        app = ApiKeyMiddleware(app, keys)
    elif mode not in ("oauth", "none"):
        raise ValueError(f"Unknown MCP_AUTH {mode!r}: use auto, api_key, oauth or none.")
    return app


def run_server(transport: str = "stdio", host: str = "127.0.0.1", port: int = 8765) -> None:
    """Build and run the MCP server.

    transport: ``stdio`` (local subprocess) | ``http``/``streamable-http`` |
    ``sse`` (HTTP transports use host/port). Streamable HTTP is served by
    uvicorn around :func:`build_http_app` so the configured auth applies.
    """
    normalized = {"http": "streamable-http", "streamable-http": "streamable-http",
                  "sse": "sse", "stdio": "stdio"}.get(transport, "stdio")
    if normalized == "streamable-http":
        import uvicorn

        app = build_http_app(host=host, port=port)
        logger.info("Starting MCP server over streamable-http on %s:%s (path /mcp)", host, port)
        uvicorn.run(app, host=host, port=port, proxy_headers=True, forwarded_allow_ips="*")
        return
    try:
        server = build_server(host=host, port=port, transport=normalized)
    except ImportError as exc:
        logger.error(str(exc))
        raise
    if normalized == "stdio":
        logger.info("Starting MCP server over stdio")
    else:
        logger.info("Starting MCP server over %s on %s:%s (no per-request auth; "
                    "front it with an authenticating proxy)", normalized, host, port)
    server.run(transport=normalized)
