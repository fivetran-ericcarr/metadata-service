"""Typer CLI for the metadata service."""

from __future__ import annotations

import json
from pathlib import Path

import typer

from .clients import DbtClient, FivetranClient
from .config import get_settings
from .dq.drift import detect_drift
from .extractors import DbtExtractor, FivetranExtractor
from .logging_config import configure_logging
from .pipeline import build_and_store
from .storage.base import get_storage

app = typer.Typer(help="Fivetran + dbt Platform metadata service.", no_args_is_help=True)
fivetran_app = typer.Typer(help="Fivetran extraction commands.")
dbt_app = typer.Typer(help="dbt extraction commands.")
app.add_typer(fivetran_app, name="fivetran")
app.add_typer(dbt_app, name="dbt")


@app.callback()
def _main() -> None:
    configure_logging(get_settings().log_level)


def _write_json(path: str, payload: dict) -> None:
    Path(path).write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    typer.echo(f"Wrote {path}")


@fivetran_app.command("extract")
def fivetran_extract(
    group_id: str | None = typer.Option(None, "--group-id", help="Filter to a Fivetran group."),
    out: str = typer.Option("fivetran_raw_latest.json", "--out"),
) -> None:
    """Pull raw Fivetran metadata and write fivetran_raw_latest.json."""
    settings = get_settings()
    with FivetranClient(settings) as client:
        raw = FivetranExtractor(client).extract(group_id=group_id or settings.fivetran_group_id)
    _write_json(out, raw)


@dbt_app.command("extract")
def dbt_extract(out: str = typer.Option("dbt_raw_latest.json", "--out")) -> None:
    """Pull raw dbt metadata and write dbt_raw_latest.json."""
    settings = get_settings()
    settings.require_dbt()
    with DbtClient(settings) as client:
        raw = DbtExtractor(client, settings.dbt_account_id or "").extract()
    _write_json(out, raw)


@app.command()
def build(
    group_id: str | None = typer.Option(None, "--group-id", help="Fivetran group id."),
    fixtures_dir: str | None = typer.Option(
        None, "--fixtures-dir", help="Build from local JSON fixtures instead of live APIs (offline)."
    ),
    include_fivetran: bool = typer.Option(True, "--include-fivetran/--no-fivetran"),
    include_dbt: bool = typer.Option(True, "--include-dbt/--no-dbt"),
    write_latest: bool = typer.Option(True, "--write-latest/--no-write-latest"),
) -> None:
    """Run full extraction + normalization and write a snapshot (latest.json)."""
    settings = get_settings()
    result = build_and_store(
        settings,
        group_id=group_id,
        include_fivetran=include_fivetran,
        include_dbt=include_dbt,
        fixtures_dir=fixtures_dir,
    )
    summary = {k: v for k, v in result.items() if k != "doc"}
    summary["drift_count"] = len(result["doc"].get("schema_drift", []))
    typer.echo(json.dumps(summary, indent=2, default=str))
    if not write_latest:
        typer.echo("(snapshot was still written by the storage backend)")


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
def serve_mcp() -> None:
    """Start the MCP server (requires the optional mcp extra)."""
    from .mcp.server import run_server

    run_server()


if __name__ == "__main__":
    app()
