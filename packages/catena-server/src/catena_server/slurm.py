"""SLURM-facing helpers for Catena."""

from __future__ import annotations

from pathlib import Path

from catena_common import config
from catena_common.models import SlurmSettings
from catena_common.paths import get_job_paths


def default_slurm_settings() -> SlurmSettings:
    """Return the configured default SLURM settings."""

    return SlurmSettings(
        partition=config.SLURM_PARTITION,
        requeue=config.SLURM_REQUEUE,
        nodes=config.SLURM_NODES,
        ntasks=config.SLURM_NTASKS,
        cpus_per_task=config.SLURM_CPUS_PER_TASK,
        mem_mb=config.SLURM_MEM_MB,
        time=config.SLURM_TIME,
        array=config.SLURM_ARRAY,
    )


def slurm_script_lines(job_id: str, body: str) -> list[str]:
    """Return the generated SLURM script as a list of lines."""

    job_paths = get_job_paths(job_id)
    settings = default_slurm_settings()
    script_lines = [
        "#!/bin/bash",
        f"#SBATCH --partition={settings.partition}",
        "#SBATCH --requeue" if settings.requeue else "#SBATCH --no-requeue",
        f"#SBATCH --job-name={job_id}",
        f"#SBATCH --nodes={settings.nodes}",
        f"#SBATCH --ntasks={settings.ntasks}",
        f"#SBATCH --cpus-per-task={settings.cpus_per_task}",
        f"#SBATCH --mem={settings.mem_mb}",
        f"#SBATCH --time={settings.time}",
        f"#SBATCH --array={settings.array}",
        f"#SBATCH --output={job_paths.out_log}",
        f"#SBATCH --error={job_paths.err_log}",
        "",
        "set -euo pipefail",
        "",
        'echo "=== PIPELINE START @ $(date) ==="',
        'echo "hostname: $(hostname)"',
        'echo "pwd: $(pwd)"',
        f'echo "job id: {job_id}"',
    ]

    stripped_body = body.strip()
    if stripped_body:
        script_lines.extend(["", stripped_body])

    script_lines.extend(["", 'echo "=== PIPELINE END @ $(date) ==="', ""])
    return script_lines


def render_slurm_script(job_id: str, body: str) -> str:
    """Render the SLURM bash script for a given job."""

    return "\n".join(slurm_script_lines(job_id, body))


def write_slurm_script(job_id: str, body: str) -> Path:
    """Write the rendered SLURM script into the job directory."""

    job_paths = get_job_paths(job_id)
    script = render_slurm_script(job_id, body)
    job_paths.slurm_script.write_text(script, encoding="utf-8")
    return job_paths.slurm_script
