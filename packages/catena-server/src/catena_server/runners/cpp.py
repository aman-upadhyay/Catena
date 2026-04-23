"""C++ runner implementation."""

from __future__ import annotations

import shlex

from catena_common import config
from catena_common.models import JobRequest
from catena_common.paths import get_job_paths

DEFAULT_CPP_EXE = "job_exe"


def shell_join(parts: list[str]) -> str:
    """Return a shell-safe command string from argument parts."""

    return " ".join(shlex.quote(part) for part in parts)


def render_cpp_run_command(job_request: JobRequest, executable: str = DEFAULT_CPP_EXE) -> str:
    """Render the compiled executable command for a C++ job request."""

    return shell_join([f"./{executable}", *job_request.cli_args])


def render_cpp_compile_command(entry_file: str, executable: str = DEFAULT_CPP_EXE) -> str:
    """Render the C++ compile command used inside the SLURM script."""

    quoted_entry = shlex.quote(entry_file)
    quoted_executable = shlex.quote(executable)
    line_continuation = " " + "\\" + "\n    "
    return line_continuation.join(
        [
            f"g++ -O2 -std=c++17 {quoted_entry}",
            "$(root-config --cflags)",
            '-I"$CONDA_PREFIX/include" -L"$CONDA_PREFIX/lib"',
            "-Wl,-rpath,$CONDA_PREFIX/lib",
            "$(root-config --libs)",
            "-lDelphes -lcnpy -lz",
            f"-o {quoted_executable}",
        ]
    )


def build_cpp_slurm_body(job_request: JobRequest) -> str:
    """Build the SLURM body for a C++ task."""

    if not job_request.entry_file:
        msg = "cpp tasks require entry_file"
        raise ValueError(msg)

    job_paths = get_job_paths(job_request.job_id)
    compile_command = render_cpp_compile_command(job_request.entry_file)
    run_command = render_cpp_run_command(job_request)
    lines = [
        f"WORKDIR={shlex.quote(str(job_paths.job_dir))}",
        'INPUTS_DIR="$WORKDIR/inputs"',
        'OUTPUTS_DIR="$WORKDIR/outputs"',
        'PRE_RUN_LIST="$WORKDIR/.inputs_before.txt"',
        'POST_RUN_LIST="$WORKDIR/.inputs_after.txt"',
        'echo "=== CPP TASK START @ $(date) ==="',
        'echo "WORKDIR: $WORKDIR"',
        'echo "INPUTS_DIR: $INPUTS_DIR"',
        'echo "OUTPUTS_DIR: $OUTPUTS_DIR"',
        'echo "Runner hostname: $(hostname)"',
        f"CONDA_SH={shlex.quote(config.CONDA_SH)}",
        'source "$CONDA_SH"',
        "set +u",
        f"conda activate {shlex.quote(config.CATENA_ENV)}",
        "set -u",
        'echo "→ Using conda env: $CONDA_PREFIX"',
        'cd "$INPUTS_DIR"',
        'echo "Runner pwd: $(pwd)"',
        'find "$INPUTS_DIR" -type f -printf \'%P\\n\' | sort > "$PRE_RUN_LIST"',
        "",
        'echo "=== CPP COMPILE START ==="',
        'echo "Compile command:"',
        f"cat <<'CATENA_CPP_COMPILE_CMD'\n{compile_command}\nCATENA_CPP_COMPILE_CMD",
        f"if {compile_command}; then",
        '  echo "=== CPP COMPILE SUCCESS ==="',
        "else",
        '  echo "=== CPP COMPILE FAILED ==="',
        "  exit 1",
        "fi",
        "",
        'echo "=== CPP RUN START ==="',
        f"if {run_command}; then",
        '  echo "=== CPP RUN SUCCESS ==="',
        "else",
        '  echo "=== CPP RUN FAILED ==="',
        "  exit 1",
        "fi",
        "",
        'find "$INPUTS_DIR" -type f -printf \'%P\\n\' | sort > "$POST_RUN_LIST"',
        'while IFS= read -r rel_path; do',
        '  [ -n "$rel_path" ] || continue',
        '  if ! grep -Fqx "$rel_path" "$PRE_RUN_LIST"; then',
        '    mkdir -p "$OUTPUTS_DIR/$(dirname "$rel_path")"',
        '    cp -f "$INPUTS_DIR/$rel_path" "$OUTPUTS_DIR/$rel_path"',
        '    echo "Copied new output: $rel_path"',
        "  fi",
        'done < "$POST_RUN_LIST"',
        'echo "=== CPP TASK END @ $(date) ==="',
    ]
    return "\n".join(lines)
