"""Bundle helpers for Catena jobs."""

from __future__ import annotations

from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from catena_common.paths import JobPaths, get_job_paths


def _iter_bundle_members(job_paths: JobPaths) -> list[Path]:
    """Return job files that should be included in a bundle."""

    members: list[Path] = []
    for path in sorted(job_paths.job_dir.rglob("*")):
        if not path.is_file():
            continue
        if path.resolve() == job_paths.zip_path.resolve():
            continue
        members.append(path)
    return members


def create_job_bundle(job_id: str, base_dir: str | Path | None = None) -> Path:
    """Create or refresh the bundle zip for a job."""

    job_paths = get_job_paths(job_id, base_dir=base_dir)
    if not job_paths.job_dir.exists():
        msg = f"job '{job_id}' does not exist"
        raise FileNotFoundError(msg)

    job_paths.bundle_dir.mkdir(parents=True, exist_ok=True)
    with ZipFile(job_paths.zip_path, mode="w", compression=ZIP_DEFLATED) as bundle_zip:
        for path in _iter_bundle_members(job_paths):
            bundle_zip.write(path, arcname=path.relative_to(job_paths.job_dir))
    return job_paths.zip_path
