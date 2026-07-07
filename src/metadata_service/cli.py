"""Typer CLI for the metadata service."""

from __future__ import annotations

import json
from pathlib import Path

import typer

from .clients import ActivationsClient, DbtClient, FivetranClient
from .config import get_settings
from .dq.drift import detect_drift
from .extractors import DbtExtractor, FivetranExtractor
from .extractors.activations_extractor import ActivationsExtractor
from .logging_config import configure_logging
from .pipeline import build_and_store
from .storage.base import get_storage

app = typer.Typer(help="Fivetran + dbt Platform metadata service.", no_args_is_help=True)
fivetran_app = typer.Typer(help="Fivetran extraction commands.")
dbt_app = typer.Typer(help="dbt extraction commands.")
activations_app = typer.Typer(help="Fivetran Activations (reverse ETL) extraction commands.")
app.add_typer(fivetran_app, name="fivetran")
app.add_typer(dbt_app, name="dbt")
app.add_typer(activations_app, name="activations")


@app.callback()
def _main() -> None:
    configure_logging(get_settings().log_level)


def _write_json(path: str, payload: dict) -> None:
    Path(path).write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    typer.echo(f"Wrote {path}")


@fivetran_app.command("extract")
def fivetran_extract(
    group_id: str | None = typer.Option(None, "--group-id", help="Filter to a Fivetran group."),
    connected_only: bool = typer.Option(
        False, "--connected-only", help="Skip connections whose setup is not 'connected' (broken/incomplete)."
    ),
    skip_paused: bool = typer.Option(
        False, "--skip-paused", help="Skip paused connections (paused flag or sync_state=paused)."
    ),
    out: str = typer.Option("fivetran_raw_latest.json", "--out"),
) -> None:
    """Pull raw Fivetran metadata and write fivetran_raw_latest.json."""
    settings = get_settings()
    with FivetranClient(settings) as client:
        raw = FivetranExtractor(client).extract(
            group_id=group_id or settings.fivetran_group_id,
            connected_only=connected_only,
            skip_paused=skip_paused,
        )
    _write_json(out, raw)


@dbt_app.command("extract")
def dbt_extract(
    project_id: int | None = typer.Option(None, "--project-id", help="Scope to a dbt project id."),
    job_id: int | None = typer.Option(None, "--job-id", help="Pull artifacts from a specific job's recent runs."),
    run_limit: int = typer.Option(50, "--run-limit", help="How many recent runs to consider for artifacts."),
    out: str = typer.Option("dbt_raw_latest.json", "--out"),
) -> None:
    """Pull raw dbt metadata and write dbt_raw_latest.json."""
    settings = get_settings()
    settings.require_dbt()
    with DbtClient(settings) as client:
        raw = DbtExtractor(client, settings.dbt_account_id or "").extract(
            run_limit=run_limit, project_id=project_id, job_id=job_id
        )
    _write_json(out, raw)


@activations_app.command("extract")
def activations_extract(
    source_database: str | None = typer.Option(
        None, "--source-database",
        help="Scope to syncs whose source reads this warehouse database "
             "(defaults to WAREHOUSE_DATABASE).",
    ),
    out: str = typer.Option("activations_raw_latest.json", "--out"),
) -> None:
    """Pull raw Activations (reverse ETL) syncs and write activations_raw_latest.json."""
    settings = get_settings()
    if not settings.activations_enabled():
        typer.echo("ACTIVATIONS_API_TOKEN is not set; nothing to extract.")
        raise typer.Exit(code=1)
    with ActivationsClient(settings) as client:
        raw = ActivationsExtractor(client).extract(
            source_database=source_database or settings.warehouse_database
        )
    _write_json(out, raw)


@app.command()
def build(
    group_id: str | None = typer.Option(None, "--group-id", help="Fivetran group id."),
    fixtures_dir: str | None = typer.Option(
        None, "--fixtures-dir", help="Build from local JSON fixtures instead of live APIs (offline)."
    ),
    include_fivetran: bool = typer.Option(True, "--include-fivetran/--no-fivetran"),
    include_dbt: bool = typer.Option(True, "--include-dbt/--no-dbt"),
    include_activations: bool = typer.Option(
        True, "--include-activations/--no-activations",
        help="Include Fivetran Activations (reverse ETL) readiness when ACTIVATIONS_API_TOKEN is set.",
    ),
    connected_only: bool = typer.Option(
        False, "--connected-only", help="Skip connections whose setup is not 'connected' (broken/incomplete)."
    ),
    skip_paused: bool = typer.Option(
        False, "--skip-paused", help="Skip paused connections (paused flag or sync_state=paused)."
    ),
    dbt_project_id: int | None = typer.Option(None, "--dbt-project-id", help="Scope dbt extraction to a project id."),
    dbt_job_id: int | None = typer.Option(None, "--dbt-job-id", help="Pull dbt artifacts from a specific job id."),
    aliases_file: str | None = typer.Option(
        None, "--aliases-file",
        help="JSON map of '<dest_schema>.<dest_table>': '<dbt_schema>.<dbt_table>' "
             "for the configured_alias match tier.",
    ),
    warehouse_metadata: bool = typer.Option(
        True, "--warehouse-metadata/--no-warehouse-metadata",
        help="Enrich PKs from the Fivetran Platform Connector's fivetran_metadata "
             "schema when WAREHOUSE_* is configured.",
    ),
    write_latest: bool | None = typer.Option(
        None, "--write-latest/--no-write-latest",
        help="Persist the snapshot as latest.json. Defaults to yes for live builds "
             "and NO for --fixtures-dir builds (so an offline test never clobbers "
             "the snapshot a running MCP/REST server is serving).",
    ),
) -> None:
    """Run full extraction + normalization and write a snapshot (latest.json)."""
    settings = get_settings()
    aliases = json.loads(Path(aliases_file).read_text(encoding="utf-8")) if aliases_file else None
    if write_latest is None:
        write_latest = not fixtures_dir
    result = build_and_store(
        settings,
        group_id=group_id,
        include_fivetran=include_fivetran,
        include_dbt=include_dbt,
        include_activations=include_activations,
        fixtures_dir=fixtures_dir,
        connected_only=connected_only,
        skip_paused=skip_paused,
        dbt_project_id=dbt_project_id,
        dbt_job_id=dbt_job_id,
        aliases=aliases,
        enrich_warehouse=warehouse_metadata,
        write_latest=write_latest,
    )
    summary = {k: v for k, v in result.items() if k != "doc"}
    summary["drift_count"] = len(result["doc"].get("schema_drift", []))
    typer.echo(json.dumps(summary, indent=2, default=str))
    if not write_latest:
        typer.echo("(dry run: snapshot NOT persisted; latest.json unchanged)")


@app.command()
def drift() -> None:
    """Compare the latest snapshot against the previous one."""
    storage = get_storage(get_settings())
    latest = storage.read_latest()
    previous = storage.read_previous()
    if latest is None:
        typer.echo("No snapshots found. Run `metadata-service build` first.")
        raise typer.Exit(code=1)
    records = detect_drift(previous, latest)
    typer.echo(json.dumps({"count": len(records), "drift": records}, indent=2, default=str))


@app.command()
def recommendations(
    schema: str | None = typer.Option(None, "--schema"),
    table: str | None = typer.Option(None, "--table"),
) -> None:
    """Print DQ recommendations from the latest snapshot."""
    storage = get_storage(get_settings())
    latest = storage.read_latest()
    if latest is None:
        typer.echo("No snapshots found. Run `metadata-service build` first.")
        raise typer.Exit(code=1)
    recs = latest.get("dq_recommendations", [])
    if schema:
        recs = [r for r in recs if (r.get("target", {}).get("schema") or "").lower() == schema.lower()]
    if table:
        recs = [r for r in recs if (r.get("target", {}).get("table") or "").lower() == table.lower()]
    typer.echo(json.dumps({"count": len(recs), "recommendations": recs}, indent=2, default=str))


@app.command("serve-api")
def serve_api() -> None:
    """Start the FastAPI service with uvicorn."""
    import uvicorn

    settings = get_settings()
    uvicorn.run("metadata_service.api.main:app", host=settings.api_host, port=settings.api_port)


@app.command("serve-mcp")
def serve_mcp(
    transport: str | None = typer.Option(None, "--transport", help="stdio | http | sse (default: MCP_TRANSPORT or stdio)"),
    host: str | None = typer.Option(None, "--host", help="Bind host for HTTP transports."),
    port: int | None = typer.Option(None, "--port", help="Bind port for HTTP transports."),
) -> None:
    """Start the MCP server (requires the optional mcp extra).

    stdio for local subprocess agents; http (streamable-http) or sse for hosted
    / remote agents.
    """
    from .mcp.server import run_server

    settings = get_settings()
    run_server(
        transport=transport or settings.mcp_transport,
        host=host or settings.mcp_host,
        port=port or settings.mcp_port,
    )


if __name__ == "__main__":
    app()
