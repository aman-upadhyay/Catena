"""Python runner implementation."""

from __future__ import annotations

import shlex

from catena_common import config
from catena_common.models import JobRequest
from catena_common.paths import get_job_paths


def shell_join(parts: list[str]) -> str:
    """Return a shell-safe command string from argument parts."""

    return " ".join(shlex.quote(part) for part in parts)


def render_python_command(job_request: JobRequest) -> str:
    """Render the Python command for a job request."""

    if not job_request.entry_file:
        msg = "python tasks require entry_file"
        raise ValueError(msg)
    return shell_join(["python", job_request.entry_file, *job_request.cli_args])


def build_python_slurm_body(job_request: JobRequest) -> str:
    """Build the SLURM body for a Python task."""

    job_paths = get_job_paths(job_request.job_id)
    python_command = render_python_command(job_request)
    lines = [
        f"WORKDIR={shlex.quote(str(job_paths.job_dir))}",
        'INPUTS_DIR="$WORKDIR/inputs"',
        'OUTPUTS_DIR="$WORKDIR/outputs"',
        'PRE_RUN_LIST="$WORKDIR/.inputs_before.txt"',
        'POST_RUN_LIST="$WORKDIR/.inputs_after.txt"',
        'echo "=== PYTHON TASK START @ $(date) ==="',
        'echo "WORKDIR: $WORKDIR"',
        'echo "INPUTS_DIR: $INPUTS_DIR"',
        'echo "OUTPUTS_DIR: $OUTPUTS_DIR"',
        f"source {shlex.quote(config.CONDA_SH)}",
        "set +u",
        f"conda activate {shlex.quote(config.CATENA_ENV)}",
        "set -u",
        'echo "Active conda env: ${CONDA_DEFAULT_ENV:-unknown}"',
        'cd "$INPUTS_DIR"',
        'echo "Runner hostname: $(hostname)"',
        'echo "Runner pwd: $(pwd)"',
        'find "$INPUTS_DIR" -type f -printf \'%P\\n\' | sort > "$PRE_RUN_LIST"',
        "",
        f"if {python_command}; then",
        '  echo "=== PYTHON TASK SUCCESS ==="',
        '  find "$INPUTS_DIR" -type f -printf \'%P\\n\' | sort > "$POST_RUN_LIST"',
        '  while IFS= read -r rel_path; do',
        '    [ -n "$rel_path" ] || continue',
        '    if ! grep -Fqx "$rel_path" "$PRE_RUN_LIST"; then',
        '      mkdir -p "$OUTPUTS_DIR/$(dirname "$rel_path")"',
        '      cp -f "$INPUTS_DIR/$rel_path" "$OUTPUTS_DIR/$rel_path"',
        '      echo "Copied new output: $rel_path"',
        "    fi",
        '  done < "$POST_RUN_LIST"',
        "else",
        '  echo "=== PYTHON TASK FAILURE ==="',
        "  exit 1",
        "fi",
        'echo "=== PYTHON TASK END @ $(date) ==="',
    ]
    return "\n".join(lines)
