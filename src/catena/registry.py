"""File-based job registry for Catena."""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import field_validator

from catena.models import CatenaModel, JobRequest, JobState, validate_job_id
from catena.paths import JobPaths, get_job_paths


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


def _json_dumps(payload: dict[str, Any]) -> str:
    """Serialize a payload as readable JSON."""

    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _write_text_file(path: Path, content: str) -> None:
    """Write text content to a file, ensuring parent directories exist."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _touch_file(path: Path) -> None:
    """Create an empty file if it does not already exist."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)


def _write_input_files(job_request: JobRequest, job_paths: JobPaths) -> None:
    """Materialize request input files under the job input directory."""

    for input_file in job_request.input_files:
        destination = job_paths.inputs_dir / input_file.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(decode_input_file_content(input_file.content_b64))


def _read_json_file(path: Path) -> dict[str, Any]:
    """Read a JSON file from disk."""

    return json.loads(path.read_text(encoding="utf-8"))


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
        existing_payload = _read_json_file(job_paths.job_json)
        created_at = existing_payload.get("created_at", timestamp)

    payload = {
        "job_id": job_request.job_id,
        "created_at": created_at,
        "updated_at": timestamp,
        "request": job_request.model_dump(mode="json"),
    }
    _write_text_file(job_paths.job_json, _json_dumps(payload))
    return job_paths.job_json


def write_state_json(
    job_id: str,
    state: JobState,
    slurm_job_id: str | None = None,
    message: str | None = None,
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

    record = PersistedStateRecord(
        job_id=job_id,
        state=state,
        active=is_active_state(state),
        slurm_job_id=slurm_job_id,
        job_dir=str(job_paths.job_dir),
        message=message,
        created_at=created_at,
        updated_at=timestamp,
    )
    _write_text_file(job_paths.state_json, record.to_json(indent=2))
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
