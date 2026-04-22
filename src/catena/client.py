"""Typer CLI for Catena client operations."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import typer
from pydantic import ValidationError

from catena import config
from catena.models import JobRequest

app = typer.Typer(
    no_args_is_help=True,
    help="Catena client CLI.",
)


def load_request_payload(path: str) -> str:
    """Load and validate a local job request JSON document."""

    payload = Path(path).read_text(encoding="utf-8")
    JobRequest.from_json(payload)
    return payload


def run_ssh_command(
    host: str,
    user: str,
    remote_args: list[str],
    stdin_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a Catena command on the remote host over SSH."""

    return subprocess.run(
        ["ssh", f"{user}@{host}", *remote_args],
        input=stdin_text,
        capture_output=True,
        text=True,
        check=False,
    )


def parse_json_response(text: str) -> dict[str, Any]:
    """Parse a JSON response returned by the remote server."""

    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        msg = "remote command returned non-object JSON"
        raise ValueError(msg)
    return parsed


def emit_json(payload: dict[str, Any]) -> None:
    """Print a machine-readable JSON document."""

    typer.echo(json.dumps(payload, indent=2))


def emit_error(message: str) -> None:
    """Print a machine-readable error payload."""

    emit_json({"message": message})


def handle_remote_result(result: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    """Validate the remote command output and surface useful SSH errors."""

    stdout = result.stdout.strip()
    stderr = result.stderr.strip()

    if stdout:
        try:
            payload = parse_json_response(stdout)
        except (json.JSONDecodeError, ValueError) as exc:
            msg = f"remote command returned invalid JSON: {exc}"
            if stderr:
                msg = f"{msg}; stderr: {stderr}"
            raise ValueError(msg) from exc
        return payload

    if result.returncode != 0:
        msg = f"ssh failed with exit code {result.returncode}"
        if stderr:
            msg = f"{msg}: {stderr}"
        raise RuntimeError(msg)

    msg = "remote command returned empty output"
    if stderr:
        msg = f"{msg}: {stderr}"
    raise ValueError(msg)


@app.command()
def submit(
    request_path: str,
    host: str = typer.Option(config.REMOTE_HOST, "--host", help="Remote SSH host."),
    user: str = typer.Option(config.REMOTE_USER, "--user", help="Remote SSH user."),
) -> None:
    """
    Submit a Catena job from a local JSON request file.
    """

    try:
        request_payload = load_request_payload(request_path)
        result = run_ssh_command(
            host=host,
            user=user,
            remote_args=[config.REMOTE_SERVER_CMD, "submit", "-"],
            stdin_text=request_payload,
        )
        payload = handle_remote_result(result)
    except (FileNotFoundError, OSError, ValidationError, ValueError, RuntimeError) as exc:
        emit_error(str(exc))
        raise typer.Exit(code=1) from exc

    emit_json(payload)
    if result.returncode != 0:
        raise typer.Exit(code=result.returncode)


@app.command()
def status(
    job_id: str,
    host: str = typer.Option(config.REMOTE_HOST, "--host", help="Remote SSH host."),
    user: str = typer.Option(config.REMOTE_USER, "--user", help="Remote SSH user."),
) -> None:
    """
    Show Catena job status for a given job_id.
    """

    try:
        result = run_ssh_command(
            host=host,
            user=user,
            remote_args=[config.REMOTE_SERVER_CMD, "status", job_id],
        )
        payload = handle_remote_result(result)
    except (OSError, ValueError, RuntimeError) as exc:
        emit_error(str(exc))
        raise typer.Exit(code=1) from exc

    emit_json(payload)
    if result.returncode != 0:
        raise typer.Exit(code=result.returncode)
