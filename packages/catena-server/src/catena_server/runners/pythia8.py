"""Pythia8 runner implementation."""

from __future__ import annotations

from dataclasses import dataclass
import re
import shlex
from pathlib import PurePosixPath

from catena_common import config
from catena_common.models import JobRequest, validate_safe_relative_name
from catena_common.paths import get_job_paths
from catena_server.runners.diagnostics import shell_diagnostics_lines

LHAPDF_USE_PATTERNS = (
    re.compile(r'#include\s*[<"]LHAPDF/LHAPDF\.h[>"]'),
    re.compile(r"\bLHAPDF::"),
    re.compile(r"\bmkPDF\s*\("),
)
LHAPDF_SET_PATTERN = re.compile(r'LHAPDF::mkPDF\s*\(\s*"([^"]+)"')


@dataclass(frozen=True, slots=True)
class ResolvedLHAPDFSettings:
    """Resolved LHAPDF behavior for a Pythia8 job."""

    use_lhapdf: bool
    use_source: str
    lhapdf_sets: list[str]
    sets_source: str
    auto_install_lhapdf: bool
    auto_install_source: str
    lhapdf_data_path: str
    data_path_source: str
    warning_message: str | None


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


def _extra_bool_optional(job_request: JobRequest, key: str) -> tuple[bool, bool | None]:
    """Return whether a nullable bool extra was present and its value."""

    if key not in job_request.extra:
        return False, None
    value = job_request.extra[key]
    if value is None:
        return True, None
    if not isinstance(value, bool):
        msg = f"extra['{key}'] must be a boolean or null when provided"
        raise ValueError(msg)
    return True, value


def _extra_string_optional(job_request: JobRequest, key: str) -> tuple[bool, str | None]:
    """Return whether a nullable string extra was present and its value."""

    if key not in job_request.extra:
        return False, None
    value = job_request.extra[key]
    if value is None:
        return True, None
    if not isinstance(value, str) or not value:
        msg = f"extra['{key}'] must be a non-empty string or null when provided"
        raise ValueError(msg)
    return True, value


def _extra_string_list_optional(job_request: JobRequest, key: str) -> tuple[bool, list[str] | None]:
    """Return whether a nullable string-list extra was present and its value."""

    if key not in job_request.extra:
        return False, None
    value = job_request.extra[key]
    if value is None:
        return True, None
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        msg = f"extra['{key}'] must be a list of non-empty strings or null when provided"
        raise ValueError(msg)
    return True, list(dict.fromkeys(value))


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


def read_staged_source(job_request: JobRequest) -> str:
    """Read the staged Pythia8 source file for inspection."""

    entry_file, _, _ = pythia8_settings(job_request)
    job_paths = get_job_paths(job_request.job_id)
    source_path = job_paths.inputs_dir / entry_file
    return source_path.read_text(encoding="utf-8", errors="replace")


def infer_lhapdf_use(source_text: str) -> bool:
    """Return True when the source text strongly suggests LHAPDF use."""

    return any(pattern.search(source_text) for pattern in LHAPDF_USE_PATTERNS)


def infer_lhapdf_sets(source_text: str) -> list[str]:
    """Extract obvious hardcoded LHAPDF set names from the source text."""

    return list(dict.fromkeys(match.group(1) for match in LHAPDF_SET_PATTERN.finditer(source_text)))


def resolve_lhapdf_settings(job_request: JobRequest, source_text: str) -> ResolvedLHAPDFSettings:
    """Resolve final LHAPDF behavior using explicit overrides and inference."""

    inferred_use = infer_lhapdf_use(source_text)
    inferred_sets = infer_lhapdf_sets(source_text)

    use_present, explicit_use = _extra_bool_optional(job_request, "use_lhapdf")
    if use_present and explicit_use is not None:
        use_lhapdf = explicit_use
        use_source = "explicit"
    elif inferred_use:
        use_lhapdf = True
        use_source = "inferred"
    else:
        use_lhapdf = False
        use_source = "default"

    sets_present, explicit_sets = _extra_string_list_optional(job_request, "lhapdf_sets")
    if sets_present:
        lhapdf_sets = explicit_sets or []
        sets_source = "explicit"
    elif inferred_sets:
        lhapdf_sets = inferred_sets
        sets_source = "inferred"
    else:
        lhapdf_sets = []
        sets_source = "default"

    auto_install_present, explicit_auto_install = _extra_bool_optional(job_request, "auto_install_lhapdf")
    if auto_install_present and explicit_auto_install is not None:
        auto_install_lhapdf = explicit_auto_install
        auto_install_source = "explicit"
    else:
        auto_install_lhapdf = True
        auto_install_source = "default"

    data_path_present, explicit_data_path = _extra_string_optional(job_request, "lhapdf_data_path")
    if data_path_present and explicit_data_path is not None:
        lhapdf_data_path = explicit_data_path
        data_path_source = "explicit"
    else:
        lhapdf_data_path = config.LHAPDF_DATA_PATH
        data_path_source = "default"

    if not PurePosixPath(lhapdf_data_path).is_absolute():
        msg = "extra['lhapdf_data_path'] must be an absolute path when provided"
        raise ValueError(msg)

    warning_message = None
    if use_lhapdf and not lhapdf_sets:
        warning_message = "LHAPDF appears to be used but no PDF set name could be inferred"

    return ResolvedLHAPDFSettings(
        use_lhapdf=use_lhapdf,
        use_source=use_source,
        lhapdf_sets=lhapdf_sets,
        sets_source=sets_source,
        auto_install_lhapdf=auto_install_lhapdf,
        auto_install_source=auto_install_source,
        lhapdf_data_path=lhapdf_data_path,
        data_path_source=data_path_source,
        warning_message=warning_message,
    )


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
    source_text = read_staged_source(job_request)
    lhapdf = resolve_lhapdf_settings(job_request, source_text)
    job_paths = get_job_paths(job_request.job_id)
    makefile = render_makefile(entry_file, binary_name)
    make_command = render_make_command(make_target)
    run_command = render_run_command(binary_name, job_request.cli_args)
    lhapdf_sets_text = "\n".join(lhapdf.lhapdf_sets)
    lines = [
        f"WORKDIR={shlex.quote(str(job_paths.job_dir))}",
        'INPUTS_DIR="$WORKDIR/inputs"',
        'OUTPUTS_DIR="$WORKDIR/outputs"',
        'PRE_RUN_LIST="$WORKDIR/.inputs_before.txt"',
        'POST_RUN_LIST="$WORKDIR/.inputs_after.txt"',
        'LHAPDF_SETS_FILE="$WORKDIR/.lhapdf_sets.txt"',
        f"CONDA_SH={shlex.quote(config.CONDA_SH)}",
        f"MAKEFILE_INC_SOURCE={shlex.quote(config.PYTHIA8_MAKEFILE_INC)}",
        f"LHAPDF_LOCK_ROOT={shlex.quote(config.LHAPDF_LOCK_DIR)}",
        f"ENTRY_FILE={shlex.quote(entry_file)}",
        f"BINARY_NAME={shlex.quote(binary_name)}",
        f"MAKE_TARGET={shlex.quote(make_target)}",
        f"LHAPDF_USE={shlex.quote(str(lhapdf.use_lhapdf).lower())}",
        f"LHAPDF_USE_SOURCE={shlex.quote(lhapdf.use_source)}",
        f"LHAPDF_SETS_SOURCE={shlex.quote(lhapdf.sets_source)}",
        f"AUTO_INSTALL_LHAPDF={shlex.quote(str(lhapdf.auto_install_lhapdf).lower())}",
        f"AUTO_INSTALL_LHAPDF_SOURCE={shlex.quote(lhapdf.auto_install_source)}",
        f"LHAPDF_DATA_PATH_VALUE={shlex.quote(lhapdf.lhapdf_data_path)}",
        f"LHAPDF_DATA_PATH_SOURCE={shlex.quote(lhapdf.data_path_source)}",
        f"LHAPDF_WARNING_MESSAGE={shlex.quote(lhapdf.warning_message or '')}",
        *shell_diagnostics_lines(job_request.job_id),
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
        "emit_lhapdf_failure() {",
        '  echo "=== PYTHIA8 LHAPDF FAILURE === $1"',
        '  echo "$1" >&2',
        "}",
        "",
        "lhapdf_set_exists() {",
        '  local set_name="$1"',
        '  if [ -d "$LHAPDF_DATA_PATH/$set_name" ] || [ -f "$LHAPDF_DATA_PATH/$set_name/$set_name.info" ]; then',
        "    return 0",
        "  fi",
        "  if command -v lhapdf >/dev/null 2>&1; then",
        '    lhapdf show "$set_name" >/dev/null 2>&1',
        "    return $?",
        "  fi",
        "  return 1",
        "}",
        "",
        "extract_lhapdf_runtime_reason() {",
        '  python - "$WORKDIR/err.log" "$WORKDIR/out.log" <<\'CATENA_LHAPDF_REASON\'',
        "from __future__ import annotations",
        "import re",
        "import sys",
        "combined = ''",
        "for path_str in sys.argv[1:]:",
        "    try:",
        "        with open(path_str, encoding='utf-8', errors='replace') as handle:",
        "            combined += handle.read()",
        "    except FileNotFoundError:",
        "        continue",
        'match = re.search(r"Info file not found for PDF set [\'\\\"]([^\'\\\"]+)[\'\\\"]", combined)',
        "if match:",
        "    print(f\"missing LHAPDF set {match.group(1)}\")",
        "elif 'LHAPDF::ReadError' in combined:",
        "    print('LHAPDF runtime error; set may be missing')",
        "CATENA_LHAPDF_REASON",
        "}",
        "",
        'if [ ! -f "$ENTRY_FILE" ]; then',
        '  echo "=== PYTHIA8 BUILD FAILED ==="',
        '  echo "Missing Pythia8 source file: $ENTRY_FILE" >&2',
        "  exit 1",
        "fi",
        'if [ ! -f "$MAKEFILE_INC_SOURCE" ]; then',
        '  echo "=== PYTHIA8 BUILD FAILED ==="',
        '  echo "Missing Pythia8 Makefile.inc: $MAKEFILE_INC_SOURCE" >&2',
        '  catena_log_missing_dependency pythia8_build DLPS Makefile.inc "$MAKEFILE_INC_SOURCE" "" "Pythia8 Makefile.inc is missing"',
        "  exit 1",
        "fi",
        "",
        'cat > "$LHAPDF_SETS_FILE" <<\'CATENA_LHAPDF_SETS\'',
        lhapdf_sets_text,
        "CATENA_LHAPDF_SETS",
        'echo "LHAPDF use source: $LHAPDF_USE_SOURCE"',
        'echo "LHAPDF final use: $LHAPDF_USE"',
        'echo "LHAPDF set source: $LHAPDF_SETS_SOURCE"',
        'echo "LHAPDF auto-install source: $AUTO_INSTALL_LHAPDF_SOURCE"',
        'echo "LHAPDF auto-install enabled: $AUTO_INSTALL_LHAPDF"',
        'echo "LHAPDF data path source: $LHAPDF_DATA_PATH_SOURCE"',
        "",
        'if [ "$LHAPDF_USE" = "true" ]; then',
        '  export LHAPDF_DATA_PATH="$LHAPDF_DATA_PATH_VALUE"',
        '  echo "LHAPDF_DATA_PATH: $LHAPDF_DATA_PATH"',
        '  if ! command -v lhapdf >/dev/null 2>&1; then',
        '    emit_lhapdf_failure "lhapdf CLI not found"',
        '    catena_log_missing_dependency pythia8_lhapdf DLPS lhapdf "command -v lhapdf" "" "lhapdf CLI not found"',
        '    echo "=== PYTHIA8 RUN FAILED ==="',
        "    exit 1",
        "  fi",
        '  mkdir -p "$LHAPDF_DATA_PATH" "$LHAPDF_LOCK_ROOT"',
        '  if [ -s "$LHAPDF_SETS_FILE" ]; then',
        '    echo "LHAPDF final sets:"',
        "    sed 's/^/  - /' \"$LHAPDF_SETS_FILE\"",
        '    while IFS= read -r set_name; do',
        '      [ -n "$set_name" ] || continue',
        '      echo "Checking LHAPDF set: $set_name"',
        '      if lhapdf_set_exists "$set_name"; then',
        '        echo "LHAPDF set already available: $set_name"',
        "        continue",
        "      fi",
        '      if [ "$AUTO_INSTALL_LHAPDF" != "true" ]; then',
        '        emit_lhapdf_failure "missing LHAPDF set $set_name"',
        '        catena_log_missing_dependency pythia8_lhapdf DLPS "$set_name" "lhapdf show $set_name" "" "missing LHAPDF set and auto-install disabled"',
        '        echo "=== PYTHIA8 RUN FAILED ==="',
        "        exit 1",
        "      fi",
        '      lock_name="$(printf "%s" "$set_name" | tr "/: " "___")"',
        '      lock_path="$LHAPDF_LOCK_ROOT/$lock_name.lock"',
        '      acquired_lock=0',
        "      for attempt in 1 2 3 4 5 6 7 8 9 10; do",
        '        if mkdir "$lock_path" 2>/dev/null; then',
        "          acquired_lock=1",
        "          break",
        "        fi",
        '        echo "Waiting for LHAPDF lock on $set_name (attempt $attempt/10)"',
        "        sleep 2",
        "      done",
        '      if [ "$acquired_lock" -ne 1 ]; then',
        '        emit_lhapdf_failure "failed to acquire LHAPDF install lock for $set_name"',
        '        catena_log_missing_dependency pythia8_lhapdf DLPS "$set_name" "mkdir $lock_path" "" "failed to acquire LHAPDF install lock"',
        '        echo "=== PYTHIA8 RUN FAILED ==="',
        "        exit 1",
        "      fi",
        '      if lhapdf_set_exists "$set_name"; then',
        '        echo "LHAPDF set became available while waiting: $set_name"',
        '        rmdir "$lock_path" || true',
        "        continue",
        "      fi",
        '      echo "Installing LHAPDF set: $set_name"',
        '      if lhapdf install "$set_name"; then',
        '        echo "Installed LHAPDF set: $set_name"',
        "      else",
        "        exit_code=$?",
        '        rmdir "$lock_path" || true',
        '        emit_lhapdf_failure "failed to install LHAPDF set $set_name"',
        '        catena_log_missing_dependency pythia8_lhapdf DLPS "$set_name" "lhapdf install $set_name" "$exit_code" "failed to install LHAPDF set"',
        '        echo "=== PYTHIA8 RUN FAILED ==="',
        "        exit 1",
        "      fi",
        '      rmdir "$lock_path" || true',
        '      if ! lhapdf_set_exists "$set_name"; then',
        '        emit_lhapdf_failure "missing LHAPDF set $set_name"',
        '        catena_log_missing_dependency pythia8_lhapdf DLPS "$set_name" "lhapdf show $set_name" "" "LHAPDF set missing after install attempt"',
        '        echo "=== PYTHIA8 RUN FAILED ==="',
        "        exit 1",
        "      fi",
        "    done < \"$LHAPDF_SETS_FILE\"",
        "  else",
        '    echo "=== PYTHIA8 LHAPDF WARNING === $LHAPDF_WARNING_MESSAGE"',
        "  fi",
        "else",
        '  echo "LHAPDF handling disabled for this job."',
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
        "  exit_code=$?",
        '  echo "=== PYTHIA8 BUILD FAILED ==="',
        f"  catena_log_missing_dependency pythia8_build {shlex.quote(config.DLPS_ENV)} pythia8-build-dependency {shlex.quote(make_command)} \"$exit_code\" \"make failed; see err.log\"",
        "  exit 1",
        "fi",
        'if [ ! -x "$BINARY_NAME" ]; then',
        '  echo "=== PYTHIA8 BUILD FAILED ==="',
        '  echo "Expected executable was not created: $BINARY_NAME" >&2',
        '  catena_log_missing_dependency pythia8_build DLPS "$BINARY_NAME" "$MAKE_TARGET" "" "expected executable was not created"',
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
        "  exit_code=$?",
        '  runtime_reason="$(extract_lhapdf_runtime_reason)"',
        '  if [ -n "$runtime_reason" ]; then',
        '    emit_lhapdf_failure "$runtime_reason"',
        f"    catena_log_missing_dependency pythia8_lhapdf {shlex.quote(config.DLPS_ENV)} lhapdf-runtime {shlex.quote(run_command)} \"$exit_code\" \"$runtime_reason\"",
        "  fi",
        '  echo "=== PYTHIA8 RUN FAILED ==="',
        f"  catena_log_missing_dependency pythia8_run {shlex.quote(config.DLPS_ENV)} pythia8-runtime-dependency {shlex.quote(run_command)} \"$exit_code\" \"Pythia8 executable failed; see err.log\"",
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
