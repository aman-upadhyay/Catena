"""Bundle helpers for Catena jobs."""

from __future__ import annotations

import hashlib
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


def sha256_file(path: str | Path) -> str:
    """Return the SHA-256 hex digest for a file."""

    digest = hashlib.sha256()
    with Path(path).open("rb") as file_handle:
        # Read in chunks so large job bundles do not need to be loaded into RAM.
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def bundle_metadata(path: str | Path) -> dict[str, int | str]:
    """Return simple metadata for a bundle zip."""

    bundle_path = Path(path)
    return {
        "zip_size_bytes": bundle_path.stat().st_size,
        "zip_sha256": sha256_file(bundle_path),
    }
