"""Delphes runner implementation."""

from __future__ import annotations

import shlex

from catena_common import config
from catena_common.models import JobRequest, validate_safe_relative_name
from catena_common.paths import get_job_paths

DEFAULT_OUT_ROOT = "output.root"


def _extra_string(job_request: JobRequest, key: str, default: str | None = None) -> str:
    """Return a string value from request.extra."""

    value = job_request.extra.get(key, default)
    if not isinstance(value, str) or not value:
        msg = f"delphes tasks require extra['{key}']"
        raise ValueError(msg)
    return value


def delphes_settings(job_request: JobRequest) -> tuple[str, str, str]:
    """Return validated Delphes card, HepMC input, and output ROOT names."""

    delphes_card = validate_safe_relative_name(_extra_string(job_request, "delphes_card"))
    hepmc_file = validate_safe_relative_name(_extra_string(job_request, "hepmc_file"))
    out_root = validate_safe_relative_name(_extra_string(job_request, "out_root", DEFAULT_OUT_ROOT))
    return delphes_card, hepmc_file, out_root


def render_delphes_command(delphes_exe: str, delphes_card: str, out_root_path: str, hepmc_file: str) -> str:
    """Render the DelphesHepMC2 command with shell-safe arguments."""

    return " ".join(f'"{part}"' for part in [delphes_exe, delphes_card, out_root_path, hepmc_file])


def build_delphes_slurm_body(job_request: JobRequest) -> str:
    """Build the SLURM body for a Delphes task."""

    delphes_card, hepmc_file, out_root = delphes_settings(job_request)
    job_paths = get_job_paths(job_request.job_id)
    delphes_command = render_delphes_command(
        "$DELPHES_EXE",
        "$DELPHES_CARD",
        "$OUT_ROOT",
        "$HEPMC",
    )
    lines = [
        f"WORKDIR={shlex.quote(str(job_paths.job_dir))}",
        'INPUTS_DIR="$WORKDIR/inputs"',
        'OUTPUTS_DIR="$WORKDIR/outputs"',
        f'CONDA_SH="{config.CONDA_SH}"',
        f'DELPHES_EXE="{config.DELPHES_EXE}"',
        f"DELPHES_CARD={shlex.quote(delphes_card)}",
        f"HEPMC={shlex.quote(hepmc_file)}",
        f'OUT_ROOT="$OUTPUTS_DIR/{out_root}"',
        'echo "=== DELPHES TASK START @ $(date) ==="',
        'echo "WORKDIR: $WORKDIR"',
        'echo "INPUTS_DIR: $INPUTS_DIR"',
        'echo "OUTPUTS_DIR: $OUTPUTS_DIR"',
        'echo "Runner hostname: $(hostname)"',
        'source "$CONDA_SH"',
        "set +u",
        f"conda activate {shlex.quote(config.DLPS_ENV)}",
        "set -u",
        'echo "→ Using conda env: $CONDA_PREFIX"',
        'mkdir -p "$(dirname "$OUT_ROOT")"',
        'cd "$INPUTS_DIR"',
        'echo "Runner pwd: $(pwd)"',
        'echo "Delphes executable: $DELPHES_EXE"',
        'echo "Delphes card: $DELPHES_CARD"',
        'echo "HepMC input: $HEPMC"',
        'echo "ROOT output: $OUT_ROOT"',
        "",
        'if [ ! -f "$DELPHES_CARD" ]; then',
        '  echo "=== DELPHES RUN FAILED ==="',
        '  echo "Missing Delphes card: $DELPHES_CARD" >&2',
        "  exit 1",
        "fi",
        'if [ ! -f "$HEPMC" ]; then',
        '  echo "=== DELPHES RUN FAILED ==="',
        '  echo "Missing HepMC input: $HEPMC" >&2',
        "  exit 1",
        "fi",
        'if [ ! -x "$DELPHES_EXE" ]; then',
        '  echo "=== DELPHES RUN FAILED ==="',
        '  echo "Delphes executable is not executable: $DELPHES_EXE" >&2',
        "  exit 1",
        "fi",
        "",
        'echo "=== DELPHES RUN START ==="',
        'echo "Run command:"',
        f"echo {shlex.quote(delphes_command)}",
        f"if {delphes_command}; then",
        '  echo "=== DELPHES RUN SUCCESS ==="',
        "else",
        '  echo "=== DELPHES RUN FAILED ==="',
        "  exit 1",
        "fi",
        'echo "=== DELPHES TASK END @ $(date) ==="',
    ]
    return "\n".join(lines)
