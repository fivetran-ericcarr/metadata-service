"""Typed configuration loaded from environment variables / .env file."""

from __future__ import annotations

import os
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Service configuration.

    Values are read from environment variables (and a local ``.env`` file when
    present). Secrets are never hardcoded and never logged.

    The ``.env`` path resolves relative to the process CWD; when the service is
    launched from elsewhere (e.g. an MCP client spawning it), either pin the
    working directory (``uv run --directory ...``) or point
    ``METADATA_SERVICE_ENV_FILE`` at the file explicitly.
    """

    model_config = SettingsConfigDict(
        env_file=os.environ.get("METADATA_SERVICE_ENV_FILE", ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        # Allow constructing Settings(field_name=...) in addition to env aliases.
        # Without this, alias'd fields silently ignore field-name kwargs (extra=ignore),
        # which made programmatic overrides (e.g. in tests) no-ops.
        populate_by_name=True,
    )

    # --- Fivetran ---------------------------------------------------------
    fivetran_api_key: str | None = Field(default=None, alias="FIVETRAN_API_KEY")
    fivetran_api_secret: str | None = Field(default=None, alias="FIVETRAN_API_SECRET")
    fivetran_group_id: str | None = Field(default=None, alias="FIVETRAN_GROUP_ID")
    fivetran_base_url: str = Field(
        default="https://api.fivetran.com/v1", alias="FIVETRAN_BASE_URL"
    )

    # --- dbt Platform / dbt Cloud ----------------------------------------
    dbt_account_id: str | None = Field(default=None, alias="DBT_ACCOUNT_ID")
    dbt_service_token: str | None = Field(default=None, alias="DBT_SERVICE_TOKEN")
    dbt_base_url: str = Field(default="https://cloud.getdbt.com/api", alias="DBT_BASE_URL")
    dbt_metadata_api_url: str | None = Field(default=None, alias="DBT_METADATA_API_URL")
    # dbt State (state-aware orchestration, dbt Fusion): nodes a run did not
    # re-execute report status ``reused``. Their last executed result is looked up
    # in up to this many earlier runs of the same scope; 0 disables the lookup
    # (reused nodes then stay unverified and the activation gate warns).
    dbt_reused_lookback_runs: int = Field(default=10, alias="DBT_REUSED_LOOKBACK_RUNS")

    # --- Storage ----------------------------------------------------------
    metadata_storage_backend: str = Field(default="local", alias="METADATA_STORAGE_BACKEND")
    metadata_local_path: str = Field(
        default="./metadata_snapshots", alias="METADATA_LOCAL_PATH"
    )
    metadata_s3_bucket: str | None = Field(default=None, alias="METADATA_S3_BUCKET")
    metadata_s3_prefix: str = Field(default="metadata", alias="METADATA_S3_PREFIX")
    # Keep at most N timestamped snapshots (latest.json is never pruned).
    # None/0 keeps everything (an hourly refresh otherwise grows unbounded).
    metadata_retention_snapshots: int | None = Field(
        default=None, alias="METADATA_RETENTION_SNAPSHOTS"
    )

    # --- Service ----------------------------------------------------------
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    # Bind loopback by default: the snapshot is a full warehouse inventory.
    # Set API_HOST=0.0.0.0 explicitly (with METADATA_API_KEY) to serve remotely.
    api_host: str = Field(default="127.0.0.1", alias="API_HOST")
    api_port: int = Field(default=8080, alias="API_PORT")
    # When set, every REST route except /health requires this key
    # (X-API-Key header or Authorization: Bearer).
    metadata_api_key: str | None = Field(default=None, alias="METADATA_API_KEY")
    # Escape hatch: allow a non-loopback bind without a key (e.g. when auth is
    # enforced by a trusted reverse proxy in front of the service).
    allow_unauthenticated: bool = Field(default=False, alias="METADATA_ALLOW_UNAUTHENTICATED")

    # --- MCP server -------------------------------------------------------
    mcp_transport: str = Field(default="stdio", alias="MCP_TRANSPORT")
    mcp_host: str = Field(default="127.0.0.1", alias="MCP_HOST")
    mcp_port: int = Field(default=8765, alias="MCP_PORT")
    # Tool surface: "auto" = every tool on stdio, the read-only set on HTTP
    # transports (external agents); "read-only"; "all"; or a comma list.
    mcp_tools: str = Field(default="auto", alias="MCP_TOOLS")
    # Remote auth for the HTTP transports: "auto" (oauth when MCP_OAUTH_ISSUER is
    # set, else api_key when a key is set), "api_key", "oauth", or "none"
    # (only behind an authenticating proxy; requires METADATA_ALLOW_UNAUTHENTICATED).
    mcp_auth: str = Field(default="auto", alias="MCP_AUTH")
    # Extra bearer keys accepted by the MCP server besides METADATA_API_KEY
    # (comma-separated, e.g. one per agent so each can be revoked).
    mcp_api_keys: str | None = Field(default=None, alias="MCP_API_KEYS")
    # OAuth 2.1 resource-server mode (ChatGPT, Snowflake Cortex, Claude sign-in):
    # tokens are JWTs issued by an external authorization server (Auth0, Okta,
    # Entra, Cognito, WorkOS, Descope, ...) that supports DCR or CIMD.
    mcp_oauth_issuer: str | None = Field(default=None, alias="MCP_OAUTH_ISSUER")
    mcp_oauth_audience: str | None = Field(default=None, alias="MCP_OAUTH_AUDIENCE")
    mcp_oauth_jwks_url: str | None = Field(default=None, alias="MCP_OAUTH_JWKS_URL")
    mcp_oauth_required_scopes: str | None = Field(default=None, alias="MCP_OAUTH_REQUIRED_SCOPES")
    # Public https URL of the MCP endpoint (e.g. https://x.run.app/mcp): the
    # OAuth resource identifier, and the base of citation URLs from `search`.
    mcp_public_url: str | None = Field(default=None, alias="MCP_PUBLIC_URL")
    # Host header allowlist for DNS-rebinding protection on a public bind
    # (comma list; defaults to the MCP_PUBLIC_URL host).
    mcp_allowed_hosts: str | None = Field(default=None, alias="MCP_ALLOWED_HOSTS")
    # Stateless Streamable HTTP: no server-side session, so any instance can
    # serve any request (Cloud Run autoscaling). JSON responses, no SSE stream.
    mcp_stateless: bool = Field(default=True, alias="MCP_STATELESS")

    # --- Data Quality -----------------------------------------------------
    warehouse_type: str = Field(default="warehouse", alias="WAREHOUSE_TYPE")
    stale_sync_threshold_hours: int = Field(
        default=24, alias="STALE_SYNC_THRESHOLD_HOURS"
    )

    # --- Warehouse metadata reader ----------------------------------------
    # Reads authoritative PKs/lineage from the Fivetran Platform Connector's
    # `fivetran_metadata` schema, and column data_type/max_length/nullable from
    # the destination's INFORMATION_SCHEMA. The dialect and database are
    # auto-detected from the Fivetran destination (WAREHOUSE_TYPE and
    # WAREHOUSE_DATABASE override); the credential never is, so it must be set
    # here — Fivetran masks every password-format destination config field.
    warehouse_account: str | None = Field(default=None, alias="WAREHOUSE_ACCOUNT")
    warehouse_user: str | None = Field(default=None, alias="WAREHOUSE_USER")
    warehouse_role: str | None = Field(default=None, alias="WAREHOUSE_ROLE")
    warehouse_name: str | None = Field(default=None, alias="WAREHOUSE_NAME")
    warehouse_database: str | None = Field(default=None, alias="WAREHOUSE_DATABASE")
    # Host/port overrides for dialects that connect directly (Postgres, and
    # Redshift/SQL Server to follow) rather than resolving a hostname from an
    # account identifier the way Snowflake does. Auto-detected from the Fivetran
    # destination's config when unset.
    warehouse_host: str | None = Field(default=None, alias="WAREHOUSE_HOST")
    warehouse_port: int | None = Field(default=None, alias="WAREHOUSE_PORT")
    warehouse_metadata_schema: str = Field(
        default="fivetran_metadata", alias="WAREHOUSE_METADATA_SCHEMA"
    )
    warehouse_private_key_path: str | None = Field(
        default=None, alias="WAREHOUSE_PRIVATE_KEY_PATH"
    )
    warehouse_private_key_passphrase: str | None = Field(
        default=None, alias="WAREHOUSE_PRIVATE_KEY_PASSPHRASE"
    )
    warehouse_password: str | None = Field(default=None, alias="WAREHOUSE_PASSWORD")

    # --- Lake catalogs (Managed Data Lake destinations) --------------------
    # "auto" reads every catalog the Fivetran destination maintains (Polaris
    # always; Glue/Unity/BigLake/OneLake when enabled on the destination);
    # "none" disables; or a comma list ("polaris,glue") to force readers, e.g.
    # for warehouse-managed Iceberg tables registered in Polaris/Glue.
    lake_catalogs: str = Field(default="auto", alias="LAKE_CATALOGS")
    # Polaris coordinates are derived from GET /v1/destinations/{id}
    # (polaris_catalog_configuration); these override. The secret never is
    # derivable (Fivetran masks it), so it is the one value an operator sets.
    polaris_endpoint: str | None = Field(default=None, alias="POLARIS_ENDPOINT")
    polaris_catalog: str | None = Field(default=None, alias="POLARIS_CATALOG")
    polaris_client_id: str | None = Field(default=None, alias="POLARIS_CLIENT_ID")
    polaris_client_secret: str | None = Field(default=None, alias="POLARIS_CLIENT_SECRET")
    polaris_scope: str = Field(default="PRINCIPAL_ROLE:ALL", alias="POLARIS_SCOPE")
    # Glue: region/catalog id default to the destination's; credentials come
    # from the standard AWS chain (AWS_PROFILE, SSO, env keys, workload role).
    glue_region: str | None = Field(default=None, alias="GLUE_REGION")
    glue_catalog_id: str | None = Field(default=None, alias="GLUE_CATALOG_ID")
    catalog_timeout_seconds: float = Field(default=30.0, alias="CATALOG_TIMEOUT_SECONDS")

    # --- Semantic context ---------------------------------------------------
    # Optional JSON with curated vocabulary the manifest does not carry
    # (synonyms, metric notes, DQ-rule impact text). See docs/any-agent-mcp.md.
    semantic_overrides_file: str | None = Field(default=None, alias="SEMANTIC_OVERRIDES_FILE")

    # --- Fivetran Activations (reverse ETL / Census) ----------------------
    activations_base_url: str = Field(
        default="https://app.getcensus.com/api/v1", alias="ACTIVATIONS_BASE_URL"
    )
    activations_api_token: str | None = Field(default=None, alias="ACTIVATIONS_API_TOKEN")

    def mcp_bearer_keys(self) -> list[str]:
        keys = [self.metadata_api_key] + (self.mcp_api_keys or "").split(",")
        return [k.strip() for k in keys if k and k.strip()]

    def activations_enabled(self) -> bool:
        return bool(self.activations_api_token)

    def dbt_enabled(self) -> bool:
        return bool(self.dbt_account_id and self.dbt_service_token)

    def warehouse_credentials_present(self) -> bool:
        """True when an operator has supplied a warehouse login.

        Deliberately does NOT check ``warehouse_type``/``warehouse_database``:
        both can be auto-detected from the Fivetran destination, while the
        credential never can — Fivetran masks every password-format config field.
        """
        return bool(
            self.warehouse_user
            and (self.warehouse_private_key_path or self.warehouse_password)
        )

    def warehouse_reader_enabled(self) -> bool:
        """True when a Snowflake reader is fully configured *without* relying on
        destination auto-detection. Kept for callers that need the strict,
        explicit-config answer; the reader factory itself uses
        :meth:`warehouse_credentials_present` plus the resolved dialect."""
        return bool(
            (self.warehouse_type or "").lower() == "snowflake"
            and self.warehouse_account
            and self.warehouse_database
            and self.warehouse_credentials_present()
        )

    def require_fivetran(self) -> None:
        if not self.fivetran_api_key or not self.fivetran_api_secret:
            raise ValueError(
                "Fivetran credentials missing: set FIVETRAN_API_KEY and "
                "FIVETRAN_API_SECRET (or run with --fixtures-dir for offline builds)."
            )

    def require_dbt(self) -> None:
        if not self.dbt_account_id or not self.dbt_service_token:
            raise ValueError(
                "dbt credentials missing: set DBT_ACCOUNT_ID and DBT_SERVICE_TOKEN "
                "(or run with --fixtures-dir for offline builds)."
            )


_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost", ""}


def guard_remote_bind(host: str | None, api_key: str | None, *, surface: str) -> None:
    """Refuse to start an unauthenticated service on a non-loopback interface.

    The snapshot is a full warehouse inventory (incl. PII-flagged columns) and
    ``refresh`` triggers billed extraction, so binding a public interface with no
    key is fail-open. An empty-string key counts as no key. Set
    METADATA_ALLOW_UNAUTHENTICATED=1 to opt in (e.g. auth enforced by a proxy).
    """
    if (host or "").strip().lower() in _LOOPBACK_HOSTS:
        return
    if (api_key or "").strip():
        return
    raise ValueError(
        f"Refusing to start {surface} on non-loopback host {host!r} without an API key. "
        "Set METADATA_API_KEY, bind a loopback host, or set "
        "METADATA_ALLOW_UNAUTHENTICATED=1 if a trusted proxy enforces auth."
    )


@lru_cache
def get_settings() -> Settings:
    """Return a cached Settings instance."""
    return Settings()
