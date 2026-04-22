"""Typer CLI for Catena server operations."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import typer
from pydantic import ValidationError

from catena.models import JobRequest, JobState, JobStatus
from catena.paths import get_job_paths
from catena.registry import create_job_bundle, create_job_layout, job_exists, read_state_json, write_state_json
from catena.runners.python_run import build_python_slurm_body
from catena.slurm import write_slurm_script

app = typer.Typer(
    no_args_is_help=True,
    help="Catena server CLI.",
)

ACTIVE_STATES = {
    JobState.SUBMITTED,
    JobState.PENDING,
    JobState.RUNNING,
}
STATE_PRIORITY = {
    JobState.UNKNOWN: -1,
    JobState.RECEIVED: 0,
    JobState.PREPARING: 1,
    JobState.SUBMITTED: 2,
    JobState.PENDING: 3,
    JobState.RUNNING: 4,
    JobState.COMPLETED: 5,
    JobState.FAILED: 5,
    JobState.CANCELLED: 5,
}
TERMINAL_STATES = {
    JobState.COMPLETED,
    JobState.FAILED,
    JobState.CANCELLED,
}


def load_request(path: str) -> JobRequest:
    """Load and validate a job request from a JSON file."""

    return JobRequest.from_json(Path(path).read_text(encoding="utf-8"))


def submit_slurm_script(script_path: Path) -> tuple[str | None, str | None]:
    """Submit a rendered SLURM script and return the job id or an error."""

    result = subprocess.run(
        ["sbatch", "--parsable", str(script_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None, result.stderr.strip() or "sbatch failed"

    stdout = result.stdout.strip()
    if not stdout:
        return None, result.stderr.strip() or "sbatch returned no job id"

    return stdout.split(";", maxsplit=1)[0].strip(), None


def query_squeue(slurm_job_id: str) -> tuple[str | None, str | None]:
    """Query the current SLURM state from squeue."""

    result = subprocess.run(
        ["squeue", "--noheader", "--format=%T", "--jobs", slurm_job_id],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None, result.stderr.strip() or "squeue failed"

    states = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if not states:
        return None, None
    return states[0], None


def query_sacct(slurm_job_id: str) -> tuple[str | None, str | None]:
    """Query historical SLURM state from sacct."""

    result = subprocess.run(
        ["sacct", "--noheader", "--parsable2", "--format=State", "--jobs", slurm_job_id],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None, result.stderr.strip() or "sacct failed"

    states = [line.strip().split("|", maxsplit=1)[0] for line in result.stdout.splitlines() if line.strip()]
    if not states:
        return None, None
    return states[0], None


def map_slurm_state(slurm_state: str) -> JobState:
    """Map a SLURM state value into the Catena job state enum."""

    normalized = slurm_state.strip().upper().split(maxsplit=1)[0].rstrip("+")
    state_map = {
        "PENDING": JobState.PENDING,
        "RUNNING": JobState.RUNNING,
        "COMPLETED": JobState.COMPLETED,
        "FAILED": JobState.FAILED,
        "CANCELLED": JobState.CANCELLED,
        "TIMEOUT": JobState.FAILED,
    }
    return state_map.get(normalized, JobState.UNKNOWN)


def _is_active(state: JobState) -> bool:
    """Return True when a Catena state should be considered active."""

    return state in ACTIVE_STATES


def _emit_status(status: JobStatus) -> None:
    """Print a machine-readable status document."""

    typer.echo(status.to_json(indent=2))


def _emit_bundle_result(job_id: str, job_dir: str, zip_path: str, message: str | None) -> None:
    """Print a machine-readable bundle result document."""

    typer.echo(
        json.dumps(
            {
                "job_id": job_id,
                "job_dir": job_dir,
                "zip_path": zip_path,
                "message": message,
            },
            indent=2,
        )
    )


def _build_status(
    job_id: str,
    state: JobState,
    job_dir: str,
    slurm_job_id: str | None = None,
    message: str | None = None,
) -> JobStatus:
    """Construct a JSON-serializable status payload."""

    return JobStatus(
        job_id=job_id,
        state=state,
        active=_is_active(state),
        slurm_job_id=slurm_job_id,
        job_dir=job_dir,
        message=message,
    )


def _should_update_state(current_state: JobState, new_state: JobState) -> bool:
    """Return True when a newly observed state should replace the local state."""

    if new_state == JobState.UNKNOWN or new_state == current_state:
        return False
    if current_state in TERMINAL_STATES:
        return False
    return STATE_PRIORITY[new_state] >= STATE_PRIORITY[current_state]


def _build_runner_body(job_request: JobRequest) -> str:
    """Build a SLURM script body for the given job request."""

    if job_request.task_type.value == "python":
        return build_python_slurm_body(job_request)

    msg = f"task_type '{job_request.task_type.value}' is not implemented"
    raise NotImplementedError(msg)


@app.command()
def submit(request_path: str) -> None:
    """
    Submit a Catena job from a JSON request file.
    """

    try:
        job_request = load_request(request_path)
    except (FileNotFoundError, OSError, ValidationError, ValueError) as exc:
        raise typer.BadParameter(str(exc), param_hint="request_path") from exc

    job_paths = get_job_paths(job_request.job_id)
    if job_exists(job_request.job_id):
        duplicate_status = _build_status(
            job_id=job_request.job_id,
            state=JobState.UNKNOWN,
            slurm_job_id=None,
            job_dir=str(job_paths.job_dir),
            message=f"job '{job_request.job_id}' already exists",
        )
        _emit_status(duplicate_status)
        raise typer.Exit(code=1)

    try:
        body = _build_runner_body(job_request)
    except (NotImplementedError, ValueError) as exc:
        unsupported_status = _build_status(
            job_id=job_request.job_id,
            state=JobState.UNKNOWN,
            slurm_job_id=None,
            job_dir=str(job_paths.job_dir),
            message=str(exc),
        )
        _emit_status(unsupported_status)
        raise typer.Exit(code=1)

    create_job_layout(job_request)
    script_path = write_slurm_script(job_request.job_id, body)
    slurm_job_id, submit_error = submit_slurm_script(script_path)

    if submit_error or not slurm_job_id:
        failed_record = write_state_json(
            job_request.job_id,
            JobState.FAILED,
            message=submit_error,
        )
        failed_status = _build_status(
            job_id=failed_record.job_id,
            state=failed_record.state,
            slurm_job_id=failed_record.slurm_job_id,
            job_dir=failed_record.job_dir,
            message=failed_record.message,
        )
        _emit_status(failed_status)
        raise typer.Exit(code=1)

    submitted_record = write_state_json(
        job_request.job_id,
        JobState.SUBMITTED,
        slurm_job_id=slurm_job_id,
        message=None,
    )
    submitted_status = _build_status(
        job_id=submitted_record.job_id,
        state=submitted_record.state,
        slurm_job_id=submitted_record.slurm_job_id,
        job_dir=submitted_record.job_dir,
        message=submitted_record.message,
    )
    _emit_status(submitted_status)


@app.command()
def status(job_id: str) -> None:
    """
    Show Catena job status for a given job_id.
    """

    job_paths = get_job_paths(job_id)
    try:
        state_record = read_state_json(job_id)
    except FileNotFoundError:
        missing_status = _build_status(
            job_id=job_id,
            state=JobState.UNKNOWN,
            slurm_job_id=None,
            job_dir=str(job_paths.job_dir),
            message=f"state file not found for '{job_id}'",
        )
        _emit_status(missing_status)
        raise typer.Exit(code=1)

    current_state = state_record.state
    slurm_job_id = state_record.slurm_job_id
    message = state_record.message

    if slurm_job_id:
        squeue_state, squeue_error = query_squeue(slurm_job_id)
        if squeue_state:
            mapped_state = map_slurm_state(squeue_state)
            if _should_update_state(current_state, mapped_state):
                state_record = write_state_json(job_id, mapped_state, slurm_job_id=slurm_job_id, message=None)
                current_state = state_record.state
                message = state_record.message
            else:
                message = None
        else:
            sacct_state, sacct_error = query_sacct(slurm_job_id)
            if sacct_state:
                mapped_state = map_slurm_state(sacct_state)
                if _should_update_state(current_state, mapped_state):
                    state_record = write_state_json(job_id, mapped_state, slurm_job_id=slurm_job_id, message=None)
                    current_state = state_record.state
                    message = state_record.message
                else:
                    message = None
            else:
                errors = []
                if squeue_error:
                    errors.append(f"squeue: {squeue_error}")
                if sacct_error:
                    errors.append(f"sacct: {sacct_error}")
                message = "; ".join(errors) if errors else state_record.message

    current_status = _build_status(
        job_id=state_record.job_id,
        state=current_state,
        slurm_job_id=slurm_job_id,
        job_dir=state_record.job_dir,
        message=message,
    )
    _emit_status(current_status)


@app.command()
def bundle(job_id: str) -> None:
    """
    Create or refresh a Catena job bundle for a given job_id.
    """

    job_paths = get_job_paths(job_id)
    try:
        zip_path = create_job_bundle(job_id)
    except FileNotFoundError as exc:
        _emit_bundle_result(
            job_id=job_id,
            job_dir=str(job_paths.job_dir),
            zip_path=str(job_paths.zip_path),
            message=str(exc),
        )
        raise typer.Exit(code=1) from exc

    _emit_bundle_result(
        job_id=job_id,
        job_dir=str(job_paths.job_dir),
        zip_path=str(zip_path),
        message="bundle created",
    )
