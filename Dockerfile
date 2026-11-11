# metadata-service remote MCP server — Cloud Run image (any agent over
# Streamable HTTP at /mcp). See docs/any-agent-mcp.md and deploy/cloudrun/.
FROM python:3.12-slim

# uv for a reproducible install from uv.lock.
COPY --from=ghcr.io/astral-sh/uv:0.8.15 /uv /uvx /bin/

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

COPY pyproject.toml uv.lock README.md ./
COPY src ./src
# remote = MCP SDK + sqlglot (column lineage); catalog-glue/s3 = boto3;
# warehouse-snowflake for live INFORMATION_SCHEMA enrichment on refresh.
RUN uv sync --frozen --no-dev --extra remote --extra catalog-glue --extra s3 \
      --extra warehouse-snowflake \
  && useradd --create-home --uid 10001 app \
  && mkdir -p /snapshots && chown app /snapshots

USER app
ENV PATH="/app/.venv/bin:$PATH" \
    MCP_TRANSPORT=http \
    MCP_HOST=0.0.0.0 \
    MCP_TOOLS=read-only \
    METADATA_LOCAL_PATH=/snapshots \
    LOG_LEVEL=INFO

# Cloud Run injects PORT (default 8080).
EXPOSE 8080
CMD ["sh", "-c", "exec metadata-service serve-mcp --transport http --host 0.0.0.0 --port ${PORT:-8080}"]
