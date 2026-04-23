"""Delphes runner implementation."""

from __future__ import annotations

import shlex

from catena_common import config
from catena_common.models import JobRequest, validate_safe_relative_name
from catena_common.paths import get_job_paths

DEFAULT_OUT_ROOT = "output.root"
HEPMC_HEADER_LINES = 5


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
    """Render the Delphes command with shell-safe arguments."""

    return " ".join(f'"{part}"' for part in [delphes_exe, delphes_card, out_root_path, hepmc_file])


def detect_hepmc_version(header: str) -> int:
    """Detect HepMC major version from the first few lines of a HepMC file."""

    normalized = header.lower()
    if "hepmc::version 3" in normalized or "asciiv3" in normalized or "hepmc3" in normalized:
        return 3
    if "hepmc::version 2" in normalized or "io_genevent" in normalized or "hepmc2" in normalized:
        return 2
    msg = "could not determine HepMC version from first 5 lines"
    raise ValueError(msg)


def read_hepmc_header(path: str) -> str:
    """Read the same header region a `head -n 5` check would inspect."""

    header_lines: list[str] = []
    with open(path, encoding="utf-8", errors="replace") as hepmc_file:
        for _ in range(HEPMC_HEADER_LINES):
            line = hepmc_file.readline()
            if line == "":
                break
            header_lines.append(line)
    return "".join(header_lines)


def detect_hepmc_version_file(path: str) -> int:
    """Detect the HepMC major version from a file path."""

    return detect_hepmc_version(read_hepmc_header(path))


def delphes_executable_for_hepmc_version(version: int) -> str:
    """Return the Delphes executable path for a HepMC major version."""

    if version == 2:
        return config.DELPHES_HEPMC2_EXE
    if version == 3:
        return config.DELPHES_HEPMC3_EXE
    msg = f"unsupported HepMC version: {version}"
    raise ValueError(msg)


def delphes_executable_for_job(job_request: JobRequest) -> str:
    """Inspect the staged HepMC input and select the matching Delphes executable."""

    _, hepmc_file, _ = delphes_settings(job_request)
    job_paths = get_job_paths(job_request.job_id)
    return delphes_executable_for_hepmc_version(detect_hepmc_version_file(str(job_paths.inputs_dir / hepmc_file)))


def build_delphes_slurm_body(job_request: JobRequest, delphes_exe: str | None = None) -> str:
    """Build the SLURM body for a Delphes task."""

    delphes_card, hepmc_file, out_root = delphes_settings(job_request)
    job_paths = get_job_paths(job_request.job_id)
    selected_delphes_exe = delphes_exe or delphes_executable_for_job(job_request)
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
        f'DELPHES_EXE="{selected_delphes_exe}"',
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
