"""Typer CLI for Catena client operations."""

from __future__ import annotations

import json
import shlex
import shutil
import subprocess
import sys
import threading
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
        ["ssh", f"{user}@{host}", " ".join(shlex.quote(arg) for arg in remote_args)],
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


def resolve_progress_option(progress: bool | None) -> bool:
    """Resolve the transfer progress setting from CLI input and TTY state."""

    if progress is not None:
        return progress
    return sys.stderr.isatty()


def remote_stage_dir(job_id: str) -> str:
    """Return the remote staging directory for a job."""

    return f"{config.BASE_STAGE_DIR}/{job_id}"


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


def ensure_remote_stage_dir(host: str, user: str, job_id: str) -> None:
    """Ensure the remote staging directory exists."""

    result = run_ssh_command(
        host=host,
        user=user,
        remote_args=["mkdir", "-p", remote_stage_dir(job_id)],
    )
    if result.returncode != 0:
        stderr = result.stderr.strip() or "ssh mkdir failed"
        raise RuntimeError(f"ssh failed with exit code {result.returncode}: {stderr}")


def run_transfer_command(command: list[str], progress: bool) -> subprocess.CompletedProcess[str]:
    """Run a transfer command while keeping final JSON output isolated on stdout."""

    if not progress:
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []

    def forward(stream: Any, chunks: list[str]) -> None:
        while True:
            chunk = stream.read(1)
            if chunk == "":
                break
            chunks.append(chunk)
            sys.stderr.write(chunk)
            sys.stderr.flush()
        stream.close()

    threads = [
        threading.Thread(target=forward, args=(process.stdout, stdout_chunks)),
        threading.Thread(target=forward, args=(process.stderr, stderr_chunks)),
    ]
    for thread in threads:
        thread.start()

    returncode = process.wait()
    for thread in threads:
        thread.join()

    return subprocess.CompletedProcess(
        command,
        returncode,
        "".join(stdout_chunks),
        "".join(stderr_chunks),
    )


def upload_with_rsync(
    host: str,
    user: str,
    job_id: str,
    files: list[str],
    progress: bool,
) -> subprocess.CompletedProcess[str]:
    """Upload files with rsync over SSH."""

    destination = f"{user}@{host}:{remote_stage_dir(job_id)}/"
    command = ["rsync", "-av", "-e", "ssh"]
    if progress:
        command.extend(["--info=progress2", "--human-readable"])
    command.extend([*files, destination])
    return run_transfer_command(command, progress=progress)


def upload_with_scp(
    host: str,
    user: str,
    job_id: str,
    files: list[str],
    progress: bool,
) -> subprocess.CompletedProcess[str]:
    """Upload files with scp as a fallback."""

    destination = f"{user}@{host}:{remote_stage_dir(job_id)}/"
    command = ["scp"]
    if not progress:
        command.append("-q")
    command.extend([*files, destination])
    return run_transfer_command(command, progress=progress)


def upload_files(host: str, user: str, job_id: str, files: list[str], progress: bool) -> None:
    """Upload files to the remote staging area with rsync or scp fallback."""

    if shutil.which("rsync"):
        result = upload_with_rsync(host, user, job_id, files, progress=progress)
        if result.returncode == 0:
            return
        if shutil.which("scp"):
            fallback = upload_with_scp(host, user, job_id, files, progress=progress)
            if fallback.returncode == 0:
                return
            stderr = fallback.stderr.strip() or result.stderr.strip() or "scp upload failed"
            raise RuntimeError(stderr)
        stderr = result.stderr.strip() or "rsync upload failed"
        raise RuntimeError(stderr)

    if shutil.which("scp"):
        result = upload_with_scp(host, user, job_id, files, progress=progress)
        if result.returncode == 0:
            return
        stderr = result.stderr.strip() or "scp upload failed"
        raise RuntimeError(stderr)

    raise RuntimeError("neither rsync nor scp is available locally")


def fetch_with_rsync(
    host: str,
    user: str,
    remote_path: str,
    local_path: Path,
    progress: bool,
) -> subprocess.CompletedProcess[str]:
    """Fetch a remote file with rsync over SSH."""

    local_path.parent.mkdir(parents=True, exist_ok=True)
    command = ["rsync", "-av", "-e", "ssh"]
    if progress:
        command.extend(["--info=progress2", "--human-readable"])
    command.extend([f"{user}@{host}:{remote_path}", str(local_path)])
    return run_transfer_command(command, progress=progress)


def fetch_with_scp(
    host: str,
    user: str,
    remote_path: str,
    local_path: Path,
    progress: bool,
) -> subprocess.CompletedProcess[str]:
    """Fetch a remote file with scp as a fallback."""

    local_path.parent.mkdir(parents=True, exist_ok=True)
    command = ["scp"]
    if not progress:
        command.append("-q")
    command.extend([f"{user}@{host}:{remote_path}", str(local_path)])
    return run_transfer_command(command, progress=progress)


def fetch_file(host: str, user: str, remote_path: str, local_path: Path, progress: bool) -> None:
    """Fetch a remote file locally with rsync or scp fallback."""

    if shutil.which("rsync"):
        result = fetch_with_rsync(host, user, remote_path, local_path, progress=progress)
        if result.returncode == 0:
            return
        if shutil.which("scp"):
            fallback = fetch_with_scp(host, user, remote_path, local_path, progress=progress)
            if fallback.returncode == 0:
                return
            stderr = fallback.stderr.strip() or result.stderr.strip() or "scp fetch failed"
            raise RuntimeError(stderr)
        stderr = result.stderr.strip() or "rsync fetch failed"
        raise RuntimeError(stderr)

    if shutil.which("scp"):
        result = fetch_with_scp(host, user, remote_path, local_path, progress=progress)
        if result.returncode == 0:
            return
        stderr = result.stderr.strip() or "scp fetch failed"
        raise RuntimeError(stderr)

    raise RuntimeError("neither rsync nor scp is available locally")


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
        emit_error("at least one file must be provided")
        raise typer.Exit(code=1)

    try:
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
