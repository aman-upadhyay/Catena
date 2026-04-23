"""Pythia8 runner implementation."""

from __future__ import annotations

import shlex
from pathlib import PurePosixPath

from catena_common import config
from catena_common.models import JobRequest, validate_safe_relative_name
from catena_common.paths import get_job_paths


def shell_join(parts: list[str]) -> str:
    """Return a shell-safe command string from argument parts."""

    return " ".join(shlex.quote(part) for part in parts)


def _safe_target(value: str, field_name: str) -> str:
    """Validate a make target or binary name that should live in inputs/."""

    safe_value = validate_safe_relative_name(value)
    if "/" in safe_value:
        msg = f"extra['{field_name}'] must not contain path separators"
        raise ValueError(msg)
    return safe_value


def _extra_string(job_request: JobRequest, key: str, default: str) -> str:
    """Return an optional non-empty string from request.extra."""

    value = job_request.extra.get(key, default)
    if not isinstance(value, str) or not value:
        msg = f"extra['{key}'] must be a non-empty string when provided"
        raise ValueError(msg)
    return value


def pythia8_settings(job_request: JobRequest) -> tuple[str, str, str]:
    """Return validated source file, binary name, and make target."""

    if not job_request.entry_file:
        msg = "pythia8 tasks require entry_file"
        raise ValueError(msg)

    entry_file = validate_safe_relative_name(job_request.entry_file)
    if PurePosixPath(entry_file).suffix != ".cc":
        msg = "pythia8 entry_file must be a .cc source file"
        raise ValueError(msg)

    default_binary_name = PurePosixPath(entry_file).stem
    binary_name = _safe_target(_extra_string(job_request, "binary_name", default_binary_name), "binary_name")
    make_target = _safe_target(_extra_string(job_request, "make_target", binary_name), "make_target")
    return entry_file, binary_name, make_target


def render_make_command(make_target: str) -> str:
    """Render the make invocation for a Pythia8 task."""

    return shell_join(["make", make_target])


def render_run_command(binary_name: str, cli_args: list[str]) -> str:
    """Render the compiled Pythia8 executable invocation."""

    return shell_join([f"./{binary_name}", *cli_args])


def render_makefile(entry_file: str, binary_name: str) -> str:
    """Render the local Makefile used to build the requested Pythia8 source."""

    return "\n".join(
        [
            "-include Makefile.inc",
            "",
            f"SOURCE := {entry_file}",
            f"BINARY := {binary_name}",
            "PYTHIA := $(PREFIX_LIB)/libpythia8$(LIB_SUFFIX)",
            "PYTHIA_LIB := -L$(PREFIX_LIB) -Wl,-rpath,$(PREFIX_LIB) -lpythia8 -ldl",
            "PYTHIA_OPTIONAL_LIBS := $(LHAPDF6_LIB) $(FASTJET3_LIB) $(HEPMC2_LIB) $(HEPMC3_LIB) $(GZIP_LIB)",
            "",
            ".PHONY: all clean",
            "all: $(BINARY)",
            "",
            "$(BINARY): $(SOURCE) $(PYTHIA) Makefile.inc",
            "\t$(CXX) $(CXX_COMMON) $(SOURCE) -o $(BINARY) $(PYTHIA_LIB) $(PYTHIA_OPTIONAL_LIBS)",
            "",
            "%: %.cc $(PYTHIA) Makefile.inc",
            "\t$(CXX) $(CXX_COMMON) $< -o $@ $(PYTHIA_LIB) $(PYTHIA_OPTIONAL_LIBS)",
            "",
            "clean:",
            "\trm -f $(BINARY)",
            "",
        ]
    )


def build_pythia8_slurm_body(job_request: JobRequest) -> str:
    """Build the SLURM body for a Pythia8 task."""

    entry_file, binary_name, make_target = pythia8_settings(job_request)
    job_paths = get_job_paths(job_request.job_id)
    makefile = render_makefile(entry_file, binary_name)
    make_command = render_make_command(make_target)
    run_command = render_run_command(binary_name, job_request.cli_args)
    lines = [
        f"WORKDIR={shlex.quote(str(job_paths.job_dir))}",
        'INPUTS_DIR="$WORKDIR/inputs"',
        'OUTPUTS_DIR="$WORKDIR/outputs"',
        'PRE_RUN_LIST="$WORKDIR/.inputs_before.txt"',
        'POST_RUN_LIST="$WORKDIR/.inputs_after.txt"',
        f"CONDA_SH={shlex.quote(config.CONDA_SH)}",
        f"MAKEFILE_INC_SOURCE={shlex.quote(config.PYTHIA8_MAKEFILE_INC)}",
        f"ENTRY_FILE={shlex.quote(entry_file)}",
        f"BINARY_NAME={shlex.quote(binary_name)}",
        f"MAKE_TARGET={shlex.quote(make_target)}",
        'echo "=== PYTHIA8 TASK START @ $(date) ==="',
        'echo "WORKDIR: $WORKDIR"',
        'echo "INPUTS_DIR: $INPUTS_DIR"',
        'echo "OUTPUTS_DIR: $OUTPUTS_DIR"',
        'echo "Runner hostname: $(hostname)"',
        'source "$CONDA_SH"',
        "set +u",
        f"conda activate {shlex.quote(config.DLPS_ENV)}",
        "set -u",
        'echo "→ Using conda env: $CONDA_PREFIX"',
        'mkdir -p "$OUTPUTS_DIR"',
        'cd "$INPUTS_DIR"',
        'echo "Runner pwd: $(pwd)"',
        'echo "Pythia8 entry file: $ENTRY_FILE"',
        'echo "Pythia8 binary name: $BINARY_NAME"',
        'echo "Pythia8 make target: $MAKE_TARGET"',
        'echo "Makefile.inc source: $MAKEFILE_INC_SOURCE"',
        "",
        'if [ ! -f "$ENTRY_FILE" ]; then',
        '  echo "=== PYTHIA8 BUILD FAILED ==="',
        '  echo "Missing Pythia8 source file: $ENTRY_FILE" >&2',
        "  exit 1",
        "fi",
        'if [ ! -f "$MAKEFILE_INC_SOURCE" ]; then',
        '  echo "=== PYTHIA8 BUILD FAILED ==="',
        '  echo "Missing Pythia8 Makefile.inc: $MAKEFILE_INC_SOURCE" >&2',
        "  exit 1",
        "fi",
        "",
        'cp -f "$MAKEFILE_INC_SOURCE" Makefile.inc',
        "cat > Makefile <<'CATENA_PYTHIA8_MAKEFILE'",
        makefile,
        "CATENA_PYTHIA8_MAKEFILE",
        'echo "Generated Makefile:"',
        "sed 's/^/  /' Makefile",
        'find "$INPUTS_DIR" -type f -printf \'%P\\n\' | sort > "$PRE_RUN_LIST"',
        "",
        'echo "=== PYTHIA8 BUILD START ==="',
        'echo "Build command:"',
        f"echo {shlex.quote(make_command)}",
        f"if {make_command}; then",
        '  echo "=== PYTHIA8 BUILD SUCCESS ==="',
        "else",
        '  echo "=== PYTHIA8 BUILD FAILED ==="',
        "  exit 1",
        "fi",
        'if [ ! -x "$BINARY_NAME" ]; then',
        '  echo "=== PYTHIA8 BUILD FAILED ==="',
        '  echo "Expected executable was not created: $BINARY_NAME" >&2',
        "  exit 1",
        "fi",
        'cp -f "$BINARY_NAME" "$OUTPUTS_DIR/$BINARY_NAME"',
        'chmod +x "$OUTPUTS_DIR/$BINARY_NAME"',
        'echo "Copied built binary: $BINARY_NAME -> $OUTPUTS_DIR/$BINARY_NAME"',
        "",
        'echo "=== PYTHIA8 RUN START ==="',
        'echo "Run command:"',
        f"echo {shlex.quote(run_command)}",
        f"if {run_command}; then",
        '  echo "=== PYTHIA8 RUN SUCCESS ==="',
        "else",
        '  echo "=== PYTHIA8 RUN FAILED ==="',
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
        'echo "=== PYTHIA8 TASK END @ $(date) ==="',
    ]
    return "\n".join(lines)
