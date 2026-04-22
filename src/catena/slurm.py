"""SLURM-facing helpers for Catena."""

from __future__ import annotations

from catena import config
from catena.models import SlurmSettings


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
