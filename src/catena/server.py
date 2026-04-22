"""Typer CLI for Catena server operations."""

from __future__ import annotations

import typer

app = typer.Typer(
    no_args_is_help=True,
    help="Catena server CLI. Placeholder commands for the server-side workflow.",
)


@app.command("start")
def start_server() -> None:
    """Start the Catena server stub."""

    typer.echo("Catena server stub: no runtime logic is implemented yet.")


@app.command("status")
def server_status() -> None:
    """Show the Catena server stub status."""

    typer.echo("Catena server stub is available, but no backend is implemented yet.")
