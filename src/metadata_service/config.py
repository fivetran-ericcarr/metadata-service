"""Typed configuration loaded from environment variables / .env file."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Service configuration.

    Values are read from environment variables (and a local ``.env`` file when
    present). Secrets are never hardcoded and never logged.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
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

    # --- Storage ----------------------------------------------------------
    metadata_storage_backend: str = Field(default="local", alias="METADATA_STORAGE_BACKEND")
    metadata_local_path: str = Field(
        default="./metadata_snapshots", alias="METADATA_LOCAL_PATH"
    )
    metadata_s3_bucket: str | None = Field(default=None, alias="METADATA_S3_BUCKET")
    metadata_s3_prefix: str = Field(default="metadata", alias="METADATA_S3_PREFIX")

    # --- Service ----------------------------------------------------------
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    api_host: str = Field(default="0.0.0.0", alias="API_HOST")
    api_port: int = Field(default=8080, alias="API_PORT")

    # --- MCP server -------------------------------------------------------
    mcp_transport: str = Field(default="stdio", alias="MCP_TRANSPORT")
    mcp_host: str = Field(default="0.0.0.0", alias="MCP_HOST")
    mcp_port: int = Field(default=8765, alias="MCP_PORT")

    # --- Data Quality -----------------------------------------------------
    warehouse_type: str = Field(default="warehouse", alias="WAREHOUSE_TYPE")
    stale_sync_threshold_hours: int = Field(
        default=24, alias="STALE_SYNC_THRESHOLD_HOURS"
    )

    # --- Warehouse metadata reader (Fivetran Platform Connector) ----------
    # Reads authoritative PKs/lineage from the `fivetran_metadata` schema in the
    # destination. Enabled when WAREHOUSE_TYPE=snowflake and account/database set.
    warehouse_account: str | None = Field(default=None, alias="WAREHOUSE_ACCOUNT")
    warehouse_user: str | None = Field(default=None, alias="WAREHOUSE_USER")
    warehouse_role: str | None = Field(default=None, alias="WAREHOUSE_ROLE")
    warehouse_name: str | None = Field(default=None, alias="WAREHOUSE_NAME")
    warehouse_database: str | None = Field(default=None, alias="WAREHOUSE_DATABASE")
    warehouse_metadata_schema: str = Field(
        default="fivetran_metadata", alias="WAREHOUSE_METADATA_SCHEMA"
    )
    warehouse_private_key_path: str | None = Field(
        default=None, alias="WAREHOUSE_PRIVATE_KEY_PATH"
    )
    warehouse_password: str | None = Field(default=None, alias="WAREHOUSE_PASSWORD")

    def warehouse_reader_enabled(self) -> bool:
        return bool(
            (self.warehouse_type or "").lower() == "snowflake"
            and self.warehouse_account
            and self.warehouse_database
            and (self.warehouse_private_key_path or self.warehouse_password)
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


@lru_cache
def get_settings() -> Settings:
    """Return a cached Settings instance."""
    return Settings()
