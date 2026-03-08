"""Typer CLI entrypoint for the altdata framework."""

from __future__ import annotations

import asyncio
import signal
import sys
from typing import Optional

import typer
from typing_extensions import Annotated

app = typer.Typer(
    name="altdata",
    help="Production-quality alt-data scraping framework CLI.",
    add_completion=False,
)


def _get_source(source_id: str):  # type: ignore[return]
    """Resolve a source_id to a BaseSource instance."""
    from altdata.sources import REGISTRY

    cls = REGISTRY.get(source_id)
    if cls is None:
        typer.echo(
            f"[error] Unknown source_id: {source_id!r}. "
            f"Available: {', '.join(sorted(REGISTRY))}"
        )
        raise typer.Exit(1)
    return cls()


@app.command("run")
def run_source(
    source_id: Annotated[str, typer.Argument(help="Source ID to run (e.g. example_yahoo_finance_rss)")],
) -> None:
    """Run a registered source once immediately."""
    from altdata.core.job_runner import JobRunner
    from altdata.logging import configure_logging

    configure_logging()
    source = _get_source(source_id)
    runner = JobRunner()
    summary = asyncio.run(runner.run(source))

    status_color = typer.colors.GREEN if summary.status == "success" else typer.colors.RED
    typer.secho(f"Status:           {summary.status}", fg=status_color, bold=True)
    typer.echo(f"Run ID:           {summary.run_id}")
    typer.echo(f"Records fetched:  {summary.records_fetched}")
    typer.echo(f"Records upserted: {summary.records_upserted}")
    if summary.error_message:
        typer.secho(f"Error: {summary.error_message}", fg=typer.colors.RED)
    raise typer.Exit(0 if summary.status == "success" else 1)


@app.command("list")
def list_sources() -> None:
    """List all registered sources and their schedules."""
    from altdata.sources import REGISTRY

    if not REGISTRY:
        typer.echo("No sources registered.")
        return

    typer.echo(f"{'Source ID':<40} {'Schedule':<20} {'Proxy':<8} {'Playwright'}")
    typer.echo("-" * 80)
    for source_id in sorted(REGISTRY):
        cls = REGISTRY[source_id]
        instance = cls()
        proxy = "yes" if instance.use_proxy else "no"
        pw = "yes" if instance.use_playwright else "no"
        typer.echo(f"{source_id:<40} {instance.schedule:<20} {proxy:<8} {pw}")


@app.command("runs")
def show_runs(
    source: Annotated[Optional[str], typer.Option("--source", "-s", help="Filter by source ID")] = None,
    limit: Annotated[int, typer.Option("--limit", "-n", help="Maximum rows to show")] = 20,
) -> None:
    """Show recent ScraperRuns from the database."""
    from altdata.db.repos.run_repo import RunRepo
    from altdata.db.session import get_session_factory
    from altdata.logging import configure_logging
    from altdata.settings import get_settings

    configure_logging()
    settings = get_settings()

    async def _fetch() -> None:
        session_factory = get_session_factory(settings)
        async with session_factory() as session:
            repo = RunRepo(session)
            runs = await repo.list_runs(source_id=source, limit=limit)

        if not runs:
            typer.echo("No runs found.")
            return

        typer.echo(
            f"{'Run ID':<38} {'Source':<32} {'Status':<10} "
            f"{'Fetched':>8} {'Upserted':>9} {'Started At'}"
        )
        typer.echo("-" * 110)
        for r in runs:
            typer.echo(
                f"{str(r.id):<38} {r.source_id:<32} {r.status:<10} "
                f"{r.records_fetched:>8} {r.records_upserted:>9} {r.started_at.isoformat()}"
            )

    asyncio.run(_fetch())


@app.command("db")
def db_command(
    action: Annotated[str, typer.Argument(help="DB action: 'init' runs alembic upgrade head")],
) -> None:
    """Database management commands."""
    if action == "init":
        import subprocess

        typer.echo("Running: alembic upgrade head")
        result = subprocess.run(["alembic", "upgrade", "head"], check=False)
        raise typer.Exit(result.returncode)
    else:
        typer.echo(f"[error] Unknown db action: {action!r}. Available: init")
        raise typer.Exit(1)


@app.command("scheduler")
def scheduler_command(
    action: Annotated[str, typer.Argument(help="Scheduler action: 'start'")] = "start",
) -> None:
    """Scheduler management commands."""
    if action != "start":
        typer.echo(f"[error] Unknown scheduler action: {action!r}. Available: start")
        raise typer.Exit(1)

    from altdata.logging import configure_logging
    from altdata.scheduler import AltDataScheduler
    from altdata.settings import get_settings
    from altdata.sources import REGISTRY

    configure_logging()
    settings = get_settings()
    scheduler = AltDataScheduler(settings)

    for source_id, cls in REGISTRY.items():
        source = cls()
        scheduler.register_source(source)
        typer.echo(f"  Registered: {source_id} ({source.schedule})")

    scheduler.start()
    typer.echo("Scheduler started. Press Ctrl+C to stop.")

    loop = asyncio.get_event_loop()

    def _shutdown(signum: int, frame: object) -> None:
        typer.echo("\nShutting down scheduler...")
        scheduler.stop()
        loop.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        loop.run_forever()
    finally:
        typer.echo("Scheduler stopped.")


if __name__ == "__main__":
    app()
