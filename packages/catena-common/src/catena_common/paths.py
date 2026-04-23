"""Path helpers for Catena job artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from catena_common.config import BASE_JOB_DIR, BASE_STAGE_DIR
from catena_common.models import validate_job_id


@dataclass(frozen=True, slots=True)
class JobPaths:
    """All filesystem paths associated with a Catena job."""

    job_id: str
    base_dir: Path
    stage_root: Path
    job_dir: Path
    stage_dir: Path
    inputs_dir: Path
    outputs_dir: Path
    bundle_dir: Path
    job_json: Path
    state_json: Path
    slurm_script: Path
    out_log: Path
    err_log: Path
    zip_path: Path


def base_job_path(base_dir: str | Path | None = None) -> Path:
    """Return the root directory used for Catena job artifacts."""

    if base_dir is None:
        return Path(BASE_JOB_DIR)
    return Path(base_dir)


def base_stage_path(stage_dir: str | Path | None = None) -> Path:
    """Return the root directory used for staged job uploads."""

    if stage_dir is None:
        return Path(BASE_STAGE_DIR)
    return Path(stage_dir)


def get_job_paths(
    job_id: str,
    base_dir: str | Path | None = None,
    stage_dir: str | Path | None = None,
) -> JobPaths:
    """Return the full set of paths for a given job id."""

    validate_job_id(job_id)

    root = base_job_path(base_dir)
    stage_root = base_stage_path(stage_dir)
    job_dir = root / job_id
    staged_dir = stage_root / job_id
    return JobPaths(
        job_id=job_id,
        base_dir=root,
        stage_root=stage_root,
        job_dir=job_dir,
        stage_dir=staged_dir,
        inputs_dir=job_dir / "inputs",
        outputs_dir=job_dir / "outputs",
        bundle_dir=job_dir / "bundle",
        job_json=job_dir / "job.json",
        state_json=job_dir / "state.json",
        slurm_script=job_dir / "slurm.sh",
        out_log=job_dir / "out.log",
        err_log=job_dir / "err.log",
        zip_path=job_dir / "bundle" / f"{job_id}.zip",
    )


def staged_file_path(job_id: str, relative_name: str, stage_dir: str | Path | None = None) -> Path:
    """Return the staged-file path for a job and safe relative file name."""

    job_paths = get_job_paths(job_id, stage_dir=stage_dir)
    return job_paths.stage_dir / relative_name
