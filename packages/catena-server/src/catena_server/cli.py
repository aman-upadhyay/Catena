"""Typer CLI for Catena server operations."""

from __future__ import annotations

from datetime import datetime, timezone
import re
import shutil
import sys
import subprocess
from dataclasses import dataclass
from pathlib import Path

import typer
from pydantic import ValidationError

from catena_common.diagnostics import log_missing_dependency
from catena_common.jsonio import dumps_json, load_json_file
from catena_common.models import JobRequest, JobState, JobStatus
from catena_common.paths import base_job_path, base_stage_path, get_job_paths

from catena_server.bundle import bundle_metadata, create_job_bundle
from catena_server.registry import create_job_layout, job_exists, read_state_json, write_state_json
from catena_server.runners.cpp import build_cpp_slurm_body
from catena_server.runners.delphes import build_delphes_slurm_body, delphes_settings
from catena_server.runners.mg5_pythia import build_mg5_pythia_slurm_body
from catena_server.runners.pythia8 import build_pythia8_slurm_body, pythia8_settings
from catena_server.runners.python_run import build_python_slurm_body
from catena_server.slurm import write_slurm_script

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
DELETE_ACTIVE_STATES = {
    JobState.SUBMITTED,
    JobState.PENDING,
    JobState.RUNNING,
}
MAX_TREE_ENTRIES_PER_DIR = 20


@dataclass(frozen=True, slots=True)
class SlurmStateInfo:
    """Small parsed view of SLURM state information."""

    state: str
    exit_code: int | None = None


def load_request(path: str) -> JobRequest:
    """Load and validate a job request from a JSON file."""

    if path == "-":
        return JobRequest.from_json(sys.stdin.read())
    return JobRequest.from_json(Path(path).read_text(encoding="utf-8"))


def submit_slurm_script(script_path: Path) -> tuple[str | None, str | None, int | None]:
    """Submit a rendered SLURM script and return the job id or an error."""

    command = ["sbatch", "--parsable", str(script_path)]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        log_missing_dependency(
            scope="server",
            operation_type="server_submit",
            missing="sbatch",
            category="bash_utility",
            command=command,
            stderr=str(exc),
        )
        raise
    if result.returncode != 0:
        return None, result.stderr.strip() or "sbatch failed", result.returncode

    stdout = result.stdout.strip()
    if not stdout:
        return None, result.stderr.strip() or "sbatch returned no job id", result.returncode

    return stdout.split(";", maxsplit=1)[0].strip(), None, None


def query_squeue(slurm_job_id: str) -> tuple[str | None, str | None]:
    """Query the current SLURM state from squeue."""

    command = ["squeue", "--noheader", "--format=%T", "--jobs", slurm_job_id]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        log_missing_dependency(
            scope="server",
            operation_type="server_status",
            missing="squeue",
            category="bash_utility",
            command=command,
            stderr=str(exc),
        )
        raise
    if result.returncode != 0:
        return None, result.stderr.strip() or "squeue failed"

    states = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if not states:
        return None, None
    return states[0], None


def parse_slurm_exit_code(value: str) -> int | None:
    """Parse a SLURM ExitCode field like 1:0 into the process exit status."""

    try:
        return int(value.split(":", maxsplit=1)[0])
    except (ValueError, IndexError):
        return None


def query_sacct(slurm_job_id: str) -> tuple[SlurmStateInfo | None, str | None]:
    """Query historical SLURM state from sacct."""

    command = ["sacct", "--noheader", "--parsable2", "--format=State,ExitCode", "--jobs", slurm_job_id]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        log_missing_dependency(
            scope="server",
            operation_type="server_status",
            missing="sacct",
            category="bash_utility",
            command=command,
            stderr=str(exc),
        )
        raise
    if result.returncode != 0:
        return None, result.stderr.strip() or "sacct failed"

    records = [line.strip().split("|") for line in result.stdout.splitlines() if line.strip()]
    if not records:
        return None, None
    fields = records[0]
    state = fields[0]
    exit_code = parse_slurm_exit_code(fields[1]) if len(fields) > 1 else None
    return SlurmStateInfo(state=state, exit_code=exit_code), None


def map_slurm_state(slurm_state: str) -> JobState:
    """Map a SLURM state value into the Catena job state enum."""

    normalized = normalize_slurm_state(slurm_state)
    state_map = {
        "PENDING": JobState.PENDING,
        "RUNNING": JobState.RUNNING,
        "COMPLETED": JobState.COMPLETED,
        "FAILED": JobState.FAILED,
        "CANCELLED": JobState.CANCELLED,
        "TIMEOUT": JobState.FAILED,
    }
    return state_map.get(normalized, JobState.UNKNOWN)


def normalize_slurm_state(slurm_state: str) -> str:
    """Normalize a raw SLURM state string for persistence and mapping."""

    return slurm_state.strip().upper().split(maxsplit=1)[0].rstrip("+")


def _is_active(state: JobState) -> bool:
    """Return True when a Catena state should be considered active."""

    return state in ACTIVE_STATES


def _emit_status(status: JobStatus) -> None:
    """Print a machine-readable status document."""

    typer.echo(status.to_json(indent=2))


def _emit_error(error_type: str, message: str) -> None:
    """Print a machine-readable error document."""

    typer.echo(dumps_json({"error_type": error_type, "message": message}, indent=2))


def _emit_submit_error(error_type: str, job_id: str, message: str) -> None:
    """Print a machine-readable submit error document."""

    typer.echo(dumps_json({"error_type": error_type, "job_id": job_id, "message": message}, indent=2))


def _emit_bundle_result(
    job_id: str,
    job_dir: str,
    zip_path: str,
    message: str | None,
    zip_size_bytes: int | None = None,
    zip_sha256: str | None = None,
) -> None:
    """Print a machine-readable bundle result document."""

    typer.echo(
        dumps_json(
            {
                "job_id": job_id,
                "job_dir": job_dir,
                "zip_path": zip_path,
                "zip_size_bytes": zip_size_bytes,
                "zip_sha256": zip_sha256,
                "message": message,
            },
            indent=2,
        )
    )


def _emit_jobs_result(jobs: list[dict[str, object]]) -> None:
    """Print a machine-readable jobs listing document."""

    typer.echo(dumps_json({"jobs": jobs}, indent=2))


def _emit_delete_result(job_id: str, deleted: bool, message: str) -> None:
    """Print a machine-readable job deletion result document."""

    typer.echo(dumps_json({"job_id": job_id, "deleted": deleted, "message": message}, indent=2))


def _emit_stages_result(stages: list[dict[str, object]]) -> None:
    """Print a machine-readable stages listing document."""

    typer.echo(dumps_json({"stages": stages}, indent=2))


def _emit_stage_tree_result(job_id: str, stage_dir: str, depth: int, tree: str) -> None:
    """Print a machine-readable stage tree document."""

    typer.echo(
        dumps_json(
            {
                "job_id": job_id,
                "stage_dir": stage_dir,
                "depth": depth,
                "tree": tree,
            },
            indent=2,
        )
    )


def _emit_clear_stage_result(job_id: str, cleared: bool, message: str) -> None:
    """Print a machine-readable stage clear result document."""

    typer.echo(dumps_json({"job_id": job_id, "cleared": cleared, "message": message}, indent=2))


def _build_status(
    job_id: str,
    state: JobState,
    job_dir: str,
    slurm_job_id: str | None = None,
    message: str | None = None,
    failure_reason: str | None = None,
    exit_code: int | None = None,
) -> JobStatus:
    """Construct a JSON-serializable status payload."""

    return JobStatus(
        job_id=job_id,
        state=state,
        active=_is_active(state),
        slurm_job_id=slurm_job_id,
        job_dir=job_dir,
        message=message,
        failure_reason=failure_reason,
        exit_code=exit_code,
    )


def _should_update_state(current_state: JobState, new_state: JobState) -> bool:
    """Return True when a newly observed state should replace the local state."""

    if new_state == JobState.UNKNOWN or new_state == current_state:
        return False
    if current_state in TERMINAL_STATES:
        return False
    return STATE_PRIORITY[new_state] >= STATE_PRIORITY[current_state]


def _terminal_slurm_state(slurm_state: str, mapped_state: JobState) -> str | None:
    """Return the final raw SLURM state when the mapped state is terminal."""

    if mapped_state in TERMINAL_STATES:
        return normalize_slurm_state(slurm_state)
    return None


def _read_text_if_exists(path: Path) -> str:
    """Read a text file if it exists, returning an empty string otherwise."""

    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return ""


def _iso_utc_from_timestamp(timestamp: float) -> str:
    """Convert a filesystem timestamp to a stable UTC ISO string."""

    return datetime.fromtimestamp(timestamp, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _job_task_type_from_payload(payload: object) -> str:
    """Extract the job task type from a persisted job.json payload."""

    if not isinstance(payload, dict):
        return "unknown"
    request = payload.get("request")
    if not isinstance(request, dict):
        return "unknown"
    task_type = request.get("task_type")
    return str(task_type) if isinstance(task_type, str) and task_type else "unknown"


def _job_listing_entry(job_dir: Path) -> dict[str, object]:
    """Build one compact jobs-listing entry from a job directory."""

    entry: dict[str, object] = {
        "job_id": job_dir.name,
        "task_type": "unknown",
        "state": JobState.UNKNOWN.value,
        "submit_time": None,
        "finish_time": None,
        "last_update_time": None,
        "message": None,
        "failure_reason": None,
    }
    notes: list[str] = []

    try:
        job_payload = load_json_file(job_dir / "job.json")
        entry["task_type"] = _job_task_type_from_payload(job_payload)
    except FileNotFoundError:
        notes.append("job.json missing")
    except (OSError, ValidationError, ValueError) as exc:
        notes.append(f"job.json unreadable: {exc}")

    try:
        state_record = read_state_json(job_dir.name)
        entry["state"] = state_record.state.value
        entry["submit_time"] = state_record.submit_time
        entry["finish_time"] = state_record.finish_time
        entry["last_update_time"] = state_record.last_update_time
        entry["message"] = state_record.message
        entry["failure_reason"] = state_record.failure_reason
    except FileNotFoundError:
        notes.append("state.json missing")
    except (OSError, ValidationError, ValueError) as exc:
        notes.append(f"state.json unreadable: {exc}")

    # Listing should stay best-effort: malformed job directories still show up
    # with a note instead of aborting the whole jobs command.
    if notes:
        note_text = "; ".join(notes)
        current_message = entry.get("message")
        entry["message"] = f"{current_message}; {note_text}" if isinstance(current_message, str) and current_message else note_text

    try:
        fallback_sort_time = _iso_utc_from_timestamp(job_dir.stat().st_mtime)
    except OSError as exc:
        fallback_sort_time = ""
        note_text = f"job directory unreadable: {exc}"
        current_message = entry.get("message")
        entry["message"] = f"{current_message}; {note_text}" if isinstance(current_message, str) and current_message else note_text

    entry["_sort_time"] = entry.get("submit_time") or entry.get("last_update_time") or entry.get("finish_time") or fallback_sort_time
    return entry


def list_jobs_payload() -> list[dict[str, object]]:
    """Return compact listing payloads for all job directories."""

    job_root = base_job_path()
    if not job_root.exists():
        return []

    jobs: list[dict[str, object]] = []
    for path in job_root.iterdir():
        if not path.is_dir():
            continue
        try:
            jobs.append(_job_listing_entry(path))
        except OSError as exc:
            jobs.append(
                {
                    "job_id": path.name,
                    "task_type": "unknown",
                    "state": JobState.UNKNOWN.value,
                    "submit_time": None,
                    "finish_time": None,
                    "last_update_time": None,
                    "message": f"job directory unreadable: {exc}",
                    "failure_reason": None,
                    "_sort_time": "",
                }
            )
    jobs.sort(key=lambda item: str(item["_sort_time"]), reverse=True)
    for job in jobs:
        job.pop("_sort_time", None)
    return jobs


def _stage_listing_entry(stage_dir: Path) -> dict[str, object]:
    """Build one compact staging-area listing entry."""

    file_count = 0
    total_size_bytes = 0
    try:
        for path in stage_dir.rglob("*"):
            if not path.is_file():
                continue
            file_count += 1
            total_size_bytes += path.stat().st_size
        modified_time = _iso_utc_from_timestamp(stage_dir.stat().st_mtime)
    except OSError:
        modified_time = ""

    return {
        "stage_id": stage_dir.name,
        "modified_time": modified_time,
        "file_count": file_count,
        "total_size_bytes": total_size_bytes,
        "_sort_time": modified_time,
    }


def list_stages_payload() -> list[dict[str, object]]:
    """Return compact listing payloads for all staging directories."""

    stage_root = base_stage_path()
    if not stage_root.exists():
        return []

    stages: list[dict[str, object]] = []
    for path in stage_root.iterdir():
        if not path.is_dir():
            continue
        try:
            stages.append(_stage_listing_entry(path))
        except OSError:
            stages.append(
                {
                    "stage_id": path.name,
                    "modified_time": "",
                    "file_count": 0,
                    "total_size_bytes": 0,
                    "_sort_time": "",
                }
            )
    stages.sort(key=lambda item: str(item["_sort_time"]), reverse=True)
    for stage in stages:
        stage.pop("_sort_time", None)
    return stages


def _tree_label(path: Path) -> str:
    """Return a human-readable tree label for a path."""

    return f"{path.name}/" if path.is_dir() else path.name


def _build_tree_lines(path: Path, depth: int, prefix: str = "", level: int = 0) -> list[str]:
    """Return pruned tree lines for a directory up to the requested depth."""

    lines = [f"{prefix}{_tree_label(path)}"]
    if not path.is_dir() or level >= depth:
        if path.is_dir() and level >= depth:
            try:
                has_entries = any(path.iterdir())
            except OSError:
                has_entries = False
            if has_entries:
                lines.append(f"{prefix}└── ...")
        return lines

    try:
        entries = sorted(path.iterdir(), key=lambda item: (not item.is_dir(), item.name.lower()))
    except OSError as exc:
        lines.append(f"{prefix}└── [unreadable: {exc}]")
        return lines

    # Prune very large staging directories so stage-tree stays readable in a
    # terminal and inexpensive to render over SSH.
    visible_entries = entries[:MAX_TREE_ENTRIES_PER_DIR]
    for index, child in enumerate(visible_entries):
        connector = "└── " if index == len(visible_entries) - 1 and len(entries) <= MAX_TREE_ENTRIES_PER_DIR else "├── "
        child_prefix = prefix + ("    " if connector == "└── " else "│   ")
        child_lines = _build_tree_lines(child, depth, prefix="", level=level + 1)
        lines.append(f"{prefix}{connector}{child_lines[0]}")
        for extra_line in child_lines[1:]:
            lines.append(f"{child_prefix}{extra_line}")

    hidden_count = len(entries) - len(visible_entries)
    if hidden_count > 0:
        lines.append(f"{prefix}└── ... ({hidden_count} more entries)")
    return lines


def build_stage_tree_text(stage_dir: Path, depth: int) -> str:
    """Return a formatted stage-directory tree string."""

    return "\n".join(_build_tree_lines(stage_dir, depth=depth))


def extract_pythia8_lhapdf_failure_reason(out_log: str, err_log: str) -> str | None:
    """Extract a concise LHAPDF-related failure reason from Pythia8 logs."""

    marker_match = re.search(r"^=== PYTHIA8 LHAPDF FAILURE === (.+)$", out_log, flags=re.MULTILINE)
    if marker_match:
        return marker_match.group(1).strip()

    combined = f"{out_log}\n{err_log}"
    set_match = re.search(r"Info file not found for PDF set ['\"]([^'\"]+)['\"]", combined)
    if set_match:
        return f"missing LHAPDF set {set_match.group(1)}"
    if "LHAPDF::ReadError" in combined:
        return "LHAPDF runtime error; set may be missing"
    return None


def infer_failure_reason(job_id: str, slurm_state: str | None = None) -> str:
    """Infer a concise failed-job reason from runner markers and SLURM state."""

    job_paths = get_job_paths(job_id)
    out_log = _read_text_if_exists(job_paths.out_log)
    err_log = _read_text_if_exists(job_paths.err_log)
    if "=== CPP COMPILE FAILED ===" in out_log:
        return "compile failed; see err.log"
    if "=== CPP RUN FAILED ===" in out_log:
        return "process exited nonzero"
    if "=== DELPHES RUN FAILED ===" in out_log:
        return "delphes failed; see err.log"
    if "=== MG5 RUN FAILED ===" in out_log:
        return "mg5 failed; see err.log"
    pythia8_lhapdf_reason = extract_pythia8_lhapdf_failure_reason(out_log, err_log)
    if pythia8_lhapdf_reason:
        return pythia8_lhapdf_reason
    if "=== PYTHIA8 BUILD FAILED ===" in out_log:
        return "pythia8 build failed; see err.log"
    if "=== PYTHIA8 RUN FAILED ===" in out_log:
        return "pythia8 process exited nonzero"
    if "=== PYTHON TASK FAILURE ===" in out_log:
        return "process exited nonzero"

    normalized = normalize_slurm_state(slurm_state or "")
    if normalized == "TIMEOUT":
        return "slurm job timed out"
    if normalized == "CANCELLED":
        return "slurm job was cancelled"
    if normalized:
        return f"slurm job ended with {normalized}; see err.log"
    return "slurm job failed; see err.log"


def _update_state_from_slurm(
    job_id: str,
    state_record,
    slurm_job_id: str,
    slurm_state: str,
    slurm_exit_code: int | None = None,
):
    """Persist state metadata from an observed SLURM state."""

    mapped_state = map_slurm_state(slurm_state)
    if mapped_state == JobState.UNKNOWN:
        return state_record, state_record.state, f"unmapped SLURM state: {slurm_state}"

    target_state = mapped_state if _should_update_state(state_record.state, mapped_state) else state_record.state
    final_slurm_state = _terminal_slurm_state(slurm_state, mapped_state)
    failure_reason = infer_failure_reason(job_id, slurm_state) if target_state == JobState.FAILED else None
    message = failure_reason if target_state == JobState.FAILED else None
    updated_record = write_state_json(
        job_id,
        target_state,
        slurm_job_id=slurm_job_id,
        message=message,
        final_slurm_state=final_slurm_state,
        failure_reason=failure_reason,
        exit_code=slurm_exit_code,
    )
    return updated_record, updated_record.state, updated_record.message


def _build_runner_body(job_request: JobRequest) -> str:
    """Build a SLURM script body for the given job request."""

    if job_request.task_type.value == "python":
        return build_python_slurm_body(job_request)
    if job_request.task_type.value == "cpp":
        return build_cpp_slurm_body(job_request)
    if job_request.task_type.value == "delphes":
        return build_delphes_slurm_body(job_request)
    if job_request.task_type.value == "mg5_pythia":
        return build_mg5_pythia_slurm_body(job_request)
    if job_request.task_type.value == "pythia8":
        return build_pythia8_slurm_body(job_request)

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
        _emit_error("invalid_input", str(exc))
        raise typer.Exit(code=2) from exc

    job_paths = get_job_paths(job_request.job_id)
    if job_exists(job_request.job_id):
        _emit_submit_error(
            error_type="job_id_exists",
            job_id=job_request.job_id,
            message=f"job_id '{job_request.job_id}' already exists at {job_paths.job_dir}",
        )
        raise typer.Exit(code=1)

    body: str | None = None
    try:
        if job_request.task_type.value == "delphes":
            delphes_settings(job_request)
        elif job_request.task_type.value == "pythia8":
            pythia8_settings(job_request)
        else:
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

    try:
        create_job_layout(job_request)
    except FileExistsError as exc:
        _emit_submit_error(
            error_type="job_id_exists",
            job_id=job_request.job_id,
            message=f"job_id '{job_request.job_id}' already exists at {job_paths.job_dir}",
        )
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, OSError, ValueError) as exc:
        failure_reason = str(exc)
        failed_record = write_state_json(
            job_request.job_id,
            JobState.FAILED,
            message=failure_reason,
            failure_reason=failure_reason,
        )
        failed_status = _build_status(
            job_id=failed_record.job_id,
            state=failed_record.state,
            slurm_job_id=failed_record.slurm_job_id,
            job_dir=failed_record.job_dir,
            message=failed_record.message,
            failure_reason=failed_record.failure_reason,
            exit_code=failed_record.exit_code,
        )
        _emit_status(failed_status)
        raise typer.Exit(code=1) from exc

    try:
        if body is None:
            body = _build_runner_body(job_request)
        script_path = write_slurm_script(job_request.job_id, body)
        slurm_job_id, submit_error, submit_exit_code = submit_slurm_script(script_path)
    except (OSError, ValueError) as exc:
        failure_reason = str(exc)
        failed_record = write_state_json(
            job_request.job_id,
            JobState.FAILED,
            message=failure_reason,
            failure_reason=failure_reason,
        )
        failed_status = _build_status(
            job_id=failed_record.job_id,
            state=failed_record.state,
            slurm_job_id=failed_record.slurm_job_id,
            job_dir=failed_record.job_dir,
            message=failed_record.message,
            failure_reason=failed_record.failure_reason,
            exit_code=failed_record.exit_code,
        )
        _emit_status(failed_status)
        raise typer.Exit(code=1) from exc

    if submit_error or not slurm_job_id:
        failure_reason = submit_error or "sbatch failed"
        failed_record = write_state_json(
            job_request.job_id,
            JobState.FAILED,
            message=failure_reason,
            failure_reason=failure_reason,
            exit_code=submit_exit_code,
        )
        failed_status = _build_status(
            job_id=failed_record.job_id,
            state=failed_record.state,
            slurm_job_id=failed_record.slurm_job_id,
            job_dir=failed_record.job_dir,
            message=failed_record.message,
            failure_reason=failed_record.failure_reason,
            exit_code=failed_record.exit_code,
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
        failure_reason=submitted_record.failure_reason,
        exit_code=submitted_record.exit_code,
    )
    _emit_status(submitted_status)


@app.command()
def status(job_id: str) -> None:
    """
    Show Catena job status for a given job_id.
    """

    try:
        job_paths = get_job_paths(job_id)
    except ValueError as exc:
        _emit_error("invalid_input", str(exc))
        raise typer.Exit(code=2) from exc

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
    except (OSError, ValidationError, ValueError) as exc:
        _emit_error("state_read_failure", str(exc))
        raise typer.Exit(code=1) from exc

    current_state = state_record.state
    slurm_job_id = state_record.slurm_job_id
    message = state_record.message
    failure_reason = state_record.failure_reason
    slurm_exit_code = state_record.exit_code
    exit_code = 0

    if slurm_job_id:
        # Active jobs usually appear in squeue. Completed jobs often disappear
        # from squeue, so sacct is the fallback for historical terminal state.
        squeue_state, squeue_error = query_squeue(slurm_job_id)
        if squeue_state:
            state_record, current_state, message = _update_state_from_slurm(
                job_id,
                state_record,
                slurm_job_id,
                squeue_state,
            )
            failure_reason = state_record.failure_reason
            slurm_exit_code = state_record.exit_code
        else:
            sacct_state, sacct_error = query_sacct(slurm_job_id)
            if sacct_state:
                state_record, current_state, message = _update_state_from_slurm(
                    job_id,
                    state_record,
                    slurm_job_id,
                    sacct_state.state,
                    slurm_exit_code=sacct_state.exit_code,
                )
                failure_reason = state_record.failure_reason
                slurm_exit_code = state_record.exit_code
            else:
                errors = []
                if squeue_error:
                    errors.append(f"squeue: {squeue_error}")
                if sacct_error:
                    errors.append(f"sacct: {sacct_error}")
                message = "; ".join(errors) if errors else state_record.message
                if errors:
                    exit_code = 1

    if current_state == JobState.FAILED and not failure_reason:
        failure_reason = infer_failure_reason(job_id, state_record.final_slurm_state)
        message = message or failure_reason
    if current_state == JobState.FAILED and failure_reason and not message:
        message = failure_reason

    # Refresh last_update_time even when SLURM did not report a newer state.
    state_record = write_state_json(
        job_id,
        current_state,
        slurm_job_id=slurm_job_id,
        message=message,
        failure_reason=failure_reason,
        exit_code=slurm_exit_code,
    )

    current_status = _build_status(
        job_id=state_record.job_id,
        state=state_record.state,
        slurm_job_id=slurm_job_id,
        job_dir=state_record.job_dir,
        message=state_record.message,
        failure_reason=state_record.failure_reason,
        exit_code=state_record.exit_code,
    )
    _emit_status(current_status)
    if exit_code != 0:
        raise typer.Exit(code=exit_code)


@app.command()
def jobs() -> None:
    """
    List Catena jobs from the server job root.
    """

    try:
        _emit_jobs_result(list_jobs_payload())
    except OSError as exc:
        _emit_error("jobs_list_failure", str(exc))
        raise typer.Exit(code=1) from exc


@app.command()
def delete(
    job_id: str,
    force_cancel: bool = typer.Option(
        False,
        "--force-cancel",
        help="Cancel an active SLURM job before deleting the job directory.",
    ),
) -> None:
    """
    Delete a Catena job directory by job_id.
    """

    try:
        job_paths = get_job_paths(job_id)
    except ValueError as exc:
        _emit_error("invalid_input", str(exc))
        raise typer.Exit(code=2) from exc

    if not job_paths.job_dir.exists():
        _emit_delete_result(job_id, deleted=False, message=f"job '{job_id}' does not exist")
        raise typer.Exit(code=1)

    state_record = None
    state_read_error: str | None = None
    try:
        state_record = read_state_json(job_id)
    except FileNotFoundError:
        state_read_error = "state.json missing"
    except (OSError, ValidationError, ValueError) as exc:
        state_read_error = f"state.json unreadable: {exc}"

    if state_record and state_record.state in DELETE_ACTIVE_STATES and not force_cancel:
        _emit_delete_result(
            job_id,
            deleted=False,
            message=f"job '{job_id}' is active; use --force-cancel to cancel and delete it",
        )
        raise typer.Exit(code=1)

    if state_record and state_record.state in DELETE_ACTIVE_STATES and force_cancel and state_record.slurm_job_id:
        command = ["scancel", state_record.slurm_job_id]
        try:
            cancel_result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError as exc:
            log_missing_dependency(
                scope="server",
                operation_type="server_delete",
                job_id=job_id,
                missing="scancel",
                category="bash_utility",
                command=command,
                stderr=str(exc),
            )
            raise
        if cancel_result.returncode != 0:
            cancel_error = cancel_result.stderr.strip() or "scancel failed"
            _emit_delete_result(
                job_id,
                deleted=False,
                message=f"failed to cancel active job '{job_id}': {cancel_error}",
            )
            raise typer.Exit(code=1)

    try:
        shutil.rmtree(job_paths.job_dir)
    except OSError as exc:
        _emit_delete_result(job_id, deleted=False, message=str(exc))
        raise typer.Exit(code=1) from exc

    message = f"deleted job '{job_id}'"
    if state_record and state_record.state in DELETE_ACTIVE_STATES and force_cancel:
        message = f"cancelled and deleted job '{job_id}'"
    if state_read_error:
        message = f"{message}; {state_read_error}"
    _emit_delete_result(job_id, deleted=True, message=message)


@app.command()
def stages() -> None:
    """
    List Catena staging areas from the server stage root.
    """

    try:
        _emit_stages_result(list_stages_payload())
    except OSError as exc:
        _emit_error("stages_list_failure", str(exc))
        raise typer.Exit(code=1) from exc


@app.command("stage-tree")
def stage_tree(
    job_id: str,
    depth: int = typer.Option(2, "--depth", help="Maximum tree depth to display."),
) -> None:
    """
    Show a compact staging tree for a given job_id.
    """

    if depth < 0:
        _emit_error("invalid_input", "depth must be greater than or equal to zero")
        raise typer.Exit(code=2)

    try:
        stage_dir = get_job_paths(job_id).stage_dir
    except ValueError as exc:
        _emit_error("invalid_input", str(exc))
        raise typer.Exit(code=2) from exc

    if not stage_dir.exists():
        _emit_stage_tree_result(job_id, str(stage_dir), depth, f"{stage_dir.name}/\n└── [missing]")
        raise typer.Exit(code=1)

    try:
        tree = build_stage_tree_text(stage_dir, depth=depth)
    except OSError as exc:
        _emit_stage_tree_result(job_id, str(stage_dir), depth, f"{stage_dir.name}/\n└── [unreadable: {exc}]")
        raise typer.Exit(code=1) from exc

    _emit_stage_tree_result(job_id, str(stage_dir), depth, tree)


@app.command("clear-stage")
def clear_stage(job_id: str) -> None:
    """
    Delete a staging area by job_id.
    """

    try:
        stage_dir = get_job_paths(job_id).stage_dir
    except ValueError as exc:
        _emit_error("invalid_input", str(exc))
        raise typer.Exit(code=2) from exc

    if not stage_dir.exists():
        _emit_clear_stage_result(job_id, cleared=False, message=f"stage '{job_id}' does not exist")
        raise typer.Exit(code=1)

    try:
        shutil.rmtree(stage_dir)
    except OSError as exc:
        _emit_clear_stage_result(job_id, cleared=False, message=str(exc))
        raise typer.Exit(code=1) from exc

    _emit_clear_stage_result(job_id, cleared=True, message=f"cleared stage '{job_id}'")


@app.command()
def bundle(
    job_id: str,
    include_inputs: bool = typer.Option(
        True,
        "--include-inputs/--no-inputs",
        help="Include staged input files in the bundle.",
    ),
) -> None:
    """
    Create or refresh a Catena job bundle for a given job_id.
    """

    try:
        job_paths = get_job_paths(job_id)
    except ValueError as exc:
        _emit_error("invalid_input", str(exc))
        raise typer.Exit(code=2) from exc

    try:
        zip_path = create_job_bundle(job_id, include_inputs=include_inputs)
    except FileNotFoundError as exc:
        _emit_bundle_result(
            job_id=job_id,
            job_dir=str(job_paths.job_dir),
            zip_path=str(job_paths.zip_path),
            message=str(exc),
        )
        raise typer.Exit(code=1) from exc
    except OSError as exc:
        _emit_bundle_result(
            job_id=job_id,
            job_dir=str(job_paths.job_dir),
            zip_path=str(job_paths.zip_path),
            message=str(exc),
        )
        raise typer.Exit(code=1) from exc

    try:
        metadata = bundle_metadata(zip_path)
    except OSError as exc:
        _emit_bundle_result(
            job_id=job_id,
            job_dir=str(job_paths.job_dir),
            zip_path=str(zip_path),
            message=str(exc),
        )
        raise typer.Exit(code=1) from exc
    try:
        state_record = read_state_json(job_id)
        # Bundles are created on demand, so state.json is updated here instead
        # of during submit/status.
        write_state_json(
            job_id,
            state_record.state,
            slurm_job_id=state_record.slurm_job_id,
            message=state_record.message,
            bundle_path=str(zip_path),
        )
    except FileNotFoundError:
        pass

    _emit_bundle_result(
        job_id=job_id,
        job_dir=str(job_paths.job_dir),
        zip_path=str(zip_path),
        zip_size_bytes=int(metadata["zip_size_bytes"]),
        zip_sha256=str(metadata["zip_sha256"]),
        message="bundle created",
    )
