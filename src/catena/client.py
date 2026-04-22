"""Typer CLI for Catena client operations."""

from __future__ import annotations

import typer

app = typer.Typer(
    no_args_is_help=True,
    help="Catena client CLI. Placeholder commands for user-facing client actions.",
)


@app.command("submit")
def submit_job() -> None:
    """Submit a job through the Catena client stub."""

    typer.echo("Catena client stub: job submission is not implemented yet.")


@app.command("status")
def client_status(job_id: str = typer.Argument("demo-job", help="Job identifier to inspect.")) -> None:
    """Show the Catena client stub status for a job."""

    typer.echo(f"Catena client stub: no status backend is implemented for '{job_id}'.")
