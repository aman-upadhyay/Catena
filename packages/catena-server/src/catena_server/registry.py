"""File-based job registry for Catena."""

from __future__ import annotations

import base64
import shutil
from datetime import datetime, timezone
from pathlib import Path

from pydantic import field_validator

from catena_common.jsonio import dump_json_file, load_json_file
from catena_common.models import CatenaModel, InputFile, JobRequest, JobState, validate_job_id
from catena_common.paths import JobPaths, get_job_paths


def utc_now_iso() -> str:
    """Return a stable UTC timestamp string for persisted records."""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def is_active_state(state: JobState) -> bool:
    """Return True while a job is still considered active."""

    return state in {
        JobState.RECEIVED,
        JobState.PREPARING,
        JobState.SUBMITTED,
        JobState.PENDING,
        JobState.RUNNING,
    }


def decode_input_file_content(content_b64: str) -> bytes:
    """Decode a base64 payload for writing job inputs."""

    return base64.b64decode(content_b64, validate=True)


def _copy_file(source: Path, destination: Path) -> None:
    """Copy a file into the destination path, creating parent directories."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def _touch_file(path: Path) -> None:
    """Create an empty file if it does not already exist."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)


def resolve_input_file(input_file: InputFile, job_paths: JobPaths) -> None:
    """Materialize one request input into the final job input directory."""

    destination = job_paths.inputs_dir / input_file.name

    if input_file.mode == "inline":
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(decode_input_file_content(input_file.content_b64 or ""))
        return

    if input_file.mode == "uploaded":
        source = job_paths.stage_dir / input_file.name
        if not source.exists():
            msg = f"uploaded staged file missing: {input_file.name}"
            raise FileNotFoundError(msg)
        if not source.is_file():
            msg = f"uploaded staged path is not a file: {input_file.name}"
            raise FileNotFoundError(msg)
        _copy_file(source, destination)
        return

    if input_file.mode == "server_path":
        source = Path(input_file.path or "")
        if not source.exists():
            msg = f"server_path input missing: {source}"
            raise FileNotFoundError(msg)
        if not source.is_file():
            msg = f"server_path input is not a file: {source}"
            raise FileNotFoundError(msg)
        _copy_file(source, destination)
        return

    msg = f"unsupported input mode: {input_file.mode}"
    raise ValueError(msg)


def _write_input_files(job_request: JobRequest, job_paths: JobPaths) -> None:
    """Materialize request input files under the job input directory."""

    for input_file in job_request.input_files:
        resolve_input_file(input_file, job_paths)


class PersistedStateRecord(CatenaModel):
    """State record stored on disk for each job."""

    job_id: str
    state: JobState
    active: bool
    slurm_job_id: str | None = None
    job_dir: str
    message: str | None = None
    created_at: str
    updated_at: str
    submit_time: str | None = None
    finish_time: str | None = None
    last_update_time: str | None = None
    final_slurm_state: str | None = None
    bundle_path: str | None = None
    failure_reason: str | None = None
    exit_code: int | None = None

    @field_validator("job_id")
    @classmethod
    def validate_job_id_field(cls, value: str) -> str:
        """Ensure the persisted job id is safe."""

        return validate_job_id(value)


def job_exists(job_id: str, base_dir: str | Path | None = None) -> bool:
    """Return True when the job directory already exists."""

    return get_job_paths(job_id, base_dir=base_dir).job_dir.exists()


def write_job_json(job_request: JobRequest, base_dir: str | Path | None = None) -> Path:
    """Persist a submitted job request as readable JSON."""

    job_paths = get_job_paths(job_request.job_id, base_dir=base_dir)
    timestamp = utc_now_iso()
    created_at = timestamp

    if job_paths.job_json.exists():
        existing_payload = load_json_file(job_paths.job_json)
        created_at = existing_payload.get("created_at", timestamp)

    payload = {
        "job_id": job_request.job_id,
        "created_at": created_at,
        "updated_at": timestamp,
        "request": job_request.model_dump(mode="json"),
    }
    dump_json_file(job_paths.job_json, payload, indent=2, sort_keys=True)
    return job_paths.job_json


def write_state_json(
    job_id: str,
    state: JobState,
    slurm_job_id: str | None = None,
    message: str | None = None,
    submit_time: str | None = None,
    finish_time: str | None = None,
    final_slurm_state: str | None = None,
    bundle_path: str | None = None,
    failure_reason: str | None = None,
    exit_code: int | None = None,
    base_dir: str | Path | None = None,
) -> PersistedStateRecord:
    """Persist the current job state to disk."""

    job_paths = get_job_paths(job_id, base_dir=base_dir)
    timestamp = utc_now_iso()
    created_at = timestamp

    if job_paths.state_json.exists():
        existing_state = PersistedStateRecord.from_json(job_paths.state_json.read_text(encoding="utf-8"))
        created_at = existing_state.created_at
        if slurm_job_id is None:
            slurm_job_id = existing_state.slurm_job_id
        # State writes are incremental. Preserve previously discovered metadata
        # unless the caller explicitly provides newer information.
        if submit_time is None:
            submit_time = existing_state.submit_time
        if finish_time is None:
            finish_time = existing_state.finish_time
        if final_slurm_state is None:
            final_slurm_state = existing_state.final_slurm_state
        if bundle_path is None:
            bundle_path = existing_state.bundle_path
        if failure_reason is None:
            failure_reason = existing_state.failure_reason
        if exit_code is None:
            exit_code = existing_state.exit_code

    if state == JobState.SUBMITTED and submit_time is None:
        submit_time = timestamp
    if state in {JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED} and finish_time is None:
        finish_time = timestamp
    if state != JobState.FAILED:
        failure_reason = None
        exit_code = None

    # `updated_at` keeps compatibility with the original state file. The newer
    # `last_update_time` makes the status-polling meaning explicit.
    record = PersistedStateRecord(
        job_id=job_id,
        state=state,
        active=is_active_state(state),
        slurm_job_id=slurm_job_id,
        job_dir=str(job_paths.job_dir),
        message=message,
        created_at=created_at,
        updated_at=timestamp,
        submit_time=submit_time,
        finish_time=finish_time,
        last_update_time=timestamp,
        final_slurm_state=final_slurm_state,
        bundle_path=bundle_path,
        failure_reason=failure_reason,
        exit_code=exit_code,
    )
    dump_json_file(job_paths.state_json, record.model_dump(mode="json"), indent=2)
    return record


def read_state_json(job_id: str, base_dir: str | Path | None = None) -> PersistedStateRecord:
    """Read the persisted state record for a job."""

    job_paths = get_job_paths(job_id, base_dir=base_dir)
    return PersistedStateRecord.from_json(job_paths.state_json.read_text(encoding="utf-8"))


def create_job_layout(job_request: JobRequest, base_dir: str | Path | None = None) -> JobPaths:
    """Create the directory layout and persisted metadata for a new job."""

    job_paths = get_job_paths(job_request.job_id, base_dir=base_dir)
    if job_paths.job_dir.exists():
        msg = f"job '{job_request.job_id}' already exists"
        raise FileExistsError(msg)

    job_paths.base_dir.mkdir(parents=True, exist_ok=True)
    job_paths.job_dir.mkdir(parents=False, exist_ok=False)
    job_paths.inputs_dir.mkdir(parents=True, exist_ok=True)
    job_paths.outputs_dir.mkdir(parents=True, exist_ok=True)
    job_paths.bundle_dir.mkdir(parents=True, exist_ok=True)
    _touch_file(job_paths.slurm_script)
    _touch_file(job_paths.out_log)
    _touch_file(job_paths.err_log)

    write_state_json(job_request.job_id, JobState.RECEIVED, base_dir=base_dir)
    write_job_json(job_request, base_dir=base_dir)
    _write_input_files(job_request, job_paths)
    write_state_json(job_request.job_id, JobState.PREPARING, base_dir=base_dir)

    return job_paths
