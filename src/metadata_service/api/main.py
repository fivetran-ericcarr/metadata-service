"""FastAPI application factory."""

from __future__ import annotations

from fastapi import FastAPI

from ..config import get_settings
from ..logging_config import configure_logging
from .routes import router


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.log_level)
    app = FastAPI(
        title="Fivetran + dbt Metadata Service",
        version="1.0.0",
        description="Serves normalized Fivetran + dbt metadata for agentic Data Quality.",
    )
    app.include_router(router)
    return app


# Module-level app for `uvicorn metadata_service.api.main:app`.
app = create_app()
