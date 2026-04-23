"""Typer CLI for Catena client operations."""

from __future__ import annotations

import time
from pathlib import Path

import typer
from pydantic import ValidationError

from catena_common import config
from catena_common.jsonio import dumps_json
from catena_common.models import JobRequest, validate_job_id

from catena_client.ssh import RemoteResponseError, SSHError, handle_remote_result, run_ssh_command
from catena_client.transfer import (
    TransferError,
    ensure_remote_stage_dir,
    fetch_file,
    remote_stage_dir,
    resolve_progress_option,
    upload_files,
)

app = typer.Typer(
    no_args_is_help=True,
    help="Catena client CLI.",
)


def load_request_payload(path: str) -> str:
    """Load and validate a local job request JSON document."""

    payload = Path(path).read_text(encoding="utf-8")
    JobRequest.from_json(payload)
    return payload


def emit_json(payload: dict[str, object]) -> None:
    """Print a machine-readable JSON document."""

    typer.echo(dumps_json(payload, indent=2))


def emit_error(message: str, error_type: str = "error") -> None:
    """Print a machine-readable error payload."""

    emit_json({"error_type": error_type, "message": message})


def error_type_for_exception(exc: BaseException) -> str:
    """Classify client-side failures for JSON error responses."""

    if isinstance(exc, RemoteResponseError):
        return "remote_response_error"
    if isinstance(exc, (FileNotFoundError, ValidationError, ValueError)):
        return "invalid_input"
    if isinstance(exc, SSHError):
        return "ssh_failure"
    if isinstance(exc, TransferError):
        return "transfer_failure"
    return "error"


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
        emit_error(str(exc), error_type_for_exception(exc))
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
        validate_job_id(job_id)
        result = run_ssh_command(
            host=host,
            user=user,
            remote_args=[config.REMOTE_SERVER_CMD, "status", job_id],
        )
        payload = handle_remote_result(result)
    except (OSError, ValueError, RuntimeError) as exc:
        emit_error(str(exc), error_type_for_exception(exc))
        raise typer.Exit(code=1) from exc

    emit_json(payload)
    if result.returncode != 0:
        raise typer.Exit(code=result.returncode)


@app.command()
def upload(
    job_id: str,
    files: list[str],
    progress: bool | None = typer.Option(None, "--progress/--no-progress", help="Show transfer progress on stderr."),
    host: str = typer.Option(config.REMOTE_HOST, "--host", help="Remote SSH host."),
    user: str = typer.Option(config.REMOTE_USER, "--user", help="Remote SSH user."),
) -> None:
    """
    Upload large input files into the remote Catena staging area.
    """

    if not files:
        emit_error("at least one file must be provided", "invalid_input")
        raise typer.Exit(code=1)

    try:
        validate_job_id(job_id)
        progress_enabled = resolve_progress_option(progress)
        local_files = [str(Path(file).resolve()) for file in files]
        for file in local_files:
            path = Path(file)
            if not path.exists():
                msg = f"local file not found: {file}"
                raise FileNotFoundError(msg)
            if not path.is_file():
                msg = f"local path is not a file: {file}"
                raise ValueError(msg)

        ensure_remote_stage_dir(host, user, job_id)
        upload_files(host, user, job_id, local_files, progress=progress_enabled)
    except (FileNotFoundError, OSError, RuntimeError, ValueError) as exc:
        emit_json(
            {
                "error_type": error_type_for_exception(exc),
                "job_id": job_id,
                "uploaded_files": [],
                "remote_stage_dir": remote_stage_dir(job_id),
                "message": str(exc),
            }
        )
        raise typer.Exit(code=1) from exc

    emit_json(
        {
            "job_id": job_id,
            "uploaded_files": [Path(file).name for file in files],
            "remote_stage_dir": remote_stage_dir(job_id),
            "message": "upload completed",
        }
    )


@app.command()
def fetch(
    job_id: str,
    dest: str | None = typer.Option(None, "--dest", help="Local destination path for the fetched zip."),
    progress: bool | None = typer.Option(None, "--progress/--no-progress", help="Show transfer progress on stderr."),
    host: str = typer.Option(config.REMOTE_HOST, "--host", help="Remote SSH host."),
    user: str = typer.Option(config.REMOTE_USER, "--user", help="Remote SSH user."),
) -> None:
    """
    Fetch a bundled Catena job archive from the remote server.
    """

    try:
        validate_job_id(job_id)
    except ValueError as exc:
        emit_error(str(exc), "invalid_input")
        raise typer.Exit(code=2) from exc

    local_path = Path(dest) if dest is not None else Path.cwd() / f"{job_id}.zip"

    try:
        progress_enabled = resolve_progress_option(progress)
        result = run_ssh_command(
            host=host,
            user=user,
            remote_args=[config.REMOTE_SERVER_CMD, "bundle", job_id],
        )
        payload = handle_remote_result(result)
        remote_zip_path = payload["zip_path"]
        remote_job_id = payload["job_id"]
        if not isinstance(remote_zip_path, str) or not isinstance(remote_job_id, str):
            msg = "remote bundle response is missing job_id or zip_path"
            raise ValueError(msg)
        fetch_file(host, user, remote_zip_path, local_path, progress=progress_enabled)
    except (KeyError, OSError, ValueError, RuntimeError) as exc:
        emit_json(
            {
                "error_type": error_type_for_exception(exc),
                "job_id": job_id,
                "remote_zip_path": "",
                "local_path": str(local_path),
                "message": str(exc),
            }
        )
        raise typer.Exit(code=1) from exc

    emit_json(
        {
            "job_id": remote_job_id,
            "remote_zip_path": remote_zip_path,
            "local_path": str(local_path),
            "message": "fetch completed",
        }
    )


@app.command()
def watch(
    job_id: str,
    interval: float = typer.Option(20.0, "--interval", help="Polling interval in seconds."),
    host: str = typer.Option(config.REMOTE_HOST, "--host", help="Remote SSH host."),
    user: str = typer.Option(config.REMOTE_USER, "--user", help="Remote SSH user."),
) -> None:
    """
    Watch Catena job status until it reaches a terminal state.
    """

    if interval <= 0:
        emit_error("interval must be greater than zero", "invalid_input")
        raise typer.Exit(code=2)

    try:
        validate_job_id(job_id)
    except ValueError as exc:
        emit_error(str(exc), "invalid_input")
        raise typer.Exit(code=2) from exc

    final_payload: dict[str, object] | None = None
    final_returncode = 0

    while True:
        try:
            result = run_ssh_command(
                host=host,
                user=user,
                remote_args=[config.REMOTE_SERVER_CMD, "status", job_id],
            )
            payload = handle_remote_result(result)
        except (OSError, ValueError, RuntimeError) as exc:
            emit_error(str(exc), error_type_for_exception(exc))
            raise typer.Exit(code=1) from exc

        final_payload = payload
        final_returncode = result.returncode

        state = str(payload.get("state", "UNKNOWN"))
        active = bool(payload.get("active", False))
        message = payload.get("message")
        detail = f" ({message})" if message else ""
        typer.echo(f"{job_id}: {state}{detail}", err=True)

        if result.returncode != 0 or not active:
            break

        time.sleep(interval)

    emit_json(final_payload)
    if final_returncode != 0:
        raise typer.Exit(code=final_returncode)
