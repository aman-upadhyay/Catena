"""MG5 + Pythia runner implementation."""

from __future__ import annotations

import shlex

from catena_common import config
from catena_common.models import JobRequest, validate_safe_relative_name
from catena_common.paths import get_job_paths


def _extra_string(job_request: JobRequest, key: str, default: str) -> str:
    """Return an optional string from request.extra."""

    value = job_request.extra.get(key, default)
    if not isinstance(value, str) or not value:
        msg = f"extra['{key}'] must be a non-empty string when provided"
        raise ValueError(msg)
    return value


def _extra_bool(job_request: JobRequest, key: str, default: bool) -> bool:
    """Return an optional bool from request.extra."""

    value = job_request.extra.get(key, default)
    if not isinstance(value, bool):
        msg = f"extra['{key}'] must be a boolean when provided"
        raise ValueError(msg)
    return value


def mg5_settings(job_request: JobRequest) -> tuple[str, str, bool]:
    """Return validated MG5 entry file, executable, and preservation setting."""

    if not job_request.entry_file:
        msg = "mg5_pythia tasks require entry_file"
        raise ValueError(msg)

    entry_file = validate_safe_relative_name(job_request.entry_file)
    mg5_exec = _extra_string(job_request, "mg5_exec", config.MG5_EXEC)
    preserve_run_dir = _extra_bool(job_request, "preserve_run_dir", True)
    return entry_file, mg5_exec, preserve_run_dir


def render_mg5_command(mg5_exec: str, entry_file: str) -> str:
    """Render the MG5 command with shell-safe arguments."""

    return " ".join(f'"{part}"' for part in [mg5_exec, entry_file])


def build_mg5_pythia_slurm_body(job_request: JobRequest) -> str:
    """Build the SLURM body for an MG5 + Pythia task."""

    entry_file, mg5_exec, preserve_run_dir = mg5_settings(job_request)
    job_paths = get_job_paths(job_request.job_id)
    mg5_command = render_mg5_command("$MG5_EXEC", "$ENTRY_FILE")
    lines = [
        f"WORKDIR={shlex.quote(str(job_paths.job_dir))}",
        'INPUTS_DIR="$WORKDIR/inputs"',
        'OUTPUTS_DIR="$WORKDIR/outputs"',
        'MG5_ARTIFACTS_DIR="$OUTPUTS_DIR/mg5_artifacts"',
        f'CONDA_SH="{config.CONDA_SH}"',
        f"MG5_EXEC={shlex.quote(mg5_exec)}",
        f"ENTRY_FILE={shlex.quote(entry_file)}",
        f"PRESERVE_RUN_DIR={str(preserve_run_dir).lower()}",
        'echo "=== MG5 TASK START @ $(date) ==="',
        'echo "WORKDIR: $WORKDIR"',
        'echo "INPUTS_DIR: $INPUTS_DIR"',
        'echo "OUTPUTS_DIR: $OUTPUTS_DIR"',
        'echo "Runner hostname: $(hostname)"',
        'source "$CONDA_SH"',
        "set +u",
        f"conda activate {shlex.quote(config.MG_ENV)}",
        "set -u",
        'echo "→ Using conda env: $CONDA_PREFIX"',
        'mkdir -p "$OUTPUTS_DIR" "$MG5_ARTIFACTS_DIR"',
        'cd "$INPUTS_DIR"',
        'echo "Runner pwd: $(pwd)"',
        'echo "MG5 executable: $MG5_EXEC"',
        'echo "MG5 entry file: $ENTRY_FILE"',
        'echo "Preserve run directory: $PRESERVE_RUN_DIR"',
        "",
        'if [ ! -f "$ENTRY_FILE" ]; then',
        '  echo "=== MG5 RUN FAILED ==="',
        '  echo "Missing MG5 entry file: $ENTRY_FILE" >&2',
        "  exit 1",
        "fi",
        'if [ ! -x "$MG5_EXEC" ]; then',
        '  echo "=== MG5 RUN FAILED ==="',
        '  echo "MG5 executable is not executable: $MG5_EXEC" >&2',
        "  exit 1",
        "fi",
        "",
        'echo "=== MG5 RUN START ==="',
        'echo "Run command:"',
        f"echo {shlex.quote(mg5_command)}",
        f"if {mg5_command}; then",
        '  echo "=== MG5 RUN SUCCESS ==="',
        "else",
        '  echo "=== MG5 RUN FAILED ==="',
        "  exit 1",
        "fi",
        "",
        'echo "=== MG5 OUTPUT COLLECTION START ==="',
        "while IFS= read -r -d '' path; do",
        '  rel_path="${path#"$INPUTS_DIR/"}"',
        '  dest="$MG5_ARTIFACTS_DIR/$rel_path"',
        '  mkdir -p "$(dirname "$dest")"',
        '  cp -f "$path" "$dest"',
        '  echo "Copied MG5 artifact: $rel_path"',
        "done < <(",
        '  find "$INPUTS_DIR" -type f \\(',
        "    -name '*.hepmc' -o -name '*.hepmc.gz' -o",
        "    -name '*.lhe' -o -name '*.lhe.gz' -o",
        "    -iname '*banner*' -o -iname '*summary*' -o -iname '*.log'",
        "  \\) -print0",
        ")",
        'echo "=== MG5 OUTPUT COLLECTION END ==="',
        'echo "=== MG5 TASK END @ $(date) ==="',
    ]
    return "\n".join(lines)
