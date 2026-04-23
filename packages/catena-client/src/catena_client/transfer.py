"""Transfer helpers for the Catena client."""

from __future__ import annotations

import shutil
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

from catena_common import config

from catena_client.ssh import run_ssh_command


def resolve_progress_option(progress: bool | None) -> bool:
    """Resolve the transfer progress setting from CLI input and TTY state."""

    if progress is not None:
        return progress
    return sys.stderr.isatty()


def remote_stage_dir(job_id: str) -> str:
    """Return the remote staging directory for a job."""

    return f"{config.BASE_STAGE_DIR}/{job_id}"


def run_transfer_command(command: list[str], progress: bool) -> subprocess.CompletedProcess[str]:
    """Run a transfer command while keeping final JSON output isolated on stdout."""

    if not progress:
        return subprocess.run(command, capture_output=True, text=True, check=False)

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
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

    return subprocess.CompletedProcess(command, returncode, "".join(stdout_chunks), "".join(stderr_chunks))


def ensure_remote_stage_dir(host: str, user: str, job_id: str) -> None:
    """Ensure the remote staging directory exists."""

    result = run_ssh_command(host=host, user=user, remote_args=["mkdir", "-p", remote_stage_dir(job_id)])
    if result.returncode != 0:
        stderr = result.stderr.strip() or "ssh mkdir failed"
        raise RuntimeError(f"ssh failed with exit code {result.returncode}: {stderr}")


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
