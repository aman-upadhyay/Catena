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
    mg5_generate_command = render_mg5_command("$MG5_EXEC", "$MG5_GENERATE_CMDS")
    mg5_launch_command = render_mg5_command("$MG5_EXEC", "$MG5_LAUNCH_CMDS")
    lines = [
        f"WORKDIR={shlex.quote(str(job_paths.job_dir))}",
        'INPUTS_DIR="$WORKDIR/inputs"',
        'OUTPUTS_DIR="$WORKDIR/outputs"',
        'MG5_ARTIFACTS_DIR="$OUTPUTS_DIR/mg5_artifacts"',
        'MG5_GENERATE_CMDS="$WORKDIR/.mg5_generate.txt"',
        'MG5_LAUNCH_CMDS="$WORKDIR/.mg5_launch.txt"',
        'MG5_PROCESS_DIR_FILE="$WORKDIR/.mg5_process_dir"',
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
        "check_mg5_errors() {",
        '  if grep -E "InvalidCmd|No events file corresponding|Command .*not recognized|Command .*not valid" "$WORKDIR/out.log" "$WORKDIR/err.log" >/dev/null 2>&1; then',
        '    echo "=== MG5 RUN FAILED ==="',
        '    echo "MG5 reported command or launch errors; see out.log/err.log" >&2',
        "    exit 1",
        "  fi",
        "}",
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
        'echo "=== MG5 COMMAND PREP START ==="',
        'python - "$ENTRY_FILE" "$MG5_GENERATE_CMDS" "$MG5_LAUNCH_CMDS" "$MG5_PROCESS_DIR_FILE" <<\'CATENA_MG5_SPLIT\'',
        "from __future__ import annotations",
        "",
        "import re",
        "import shlex",
        "import sys",
        "from pathlib import Path",
        "",
        "entry_path = Path(sys.argv[1])",
        "generate_path = Path(sys.argv[2])",
        "launch_path = Path(sys.argv[3])",
        "process_dir_path = Path(sys.argv[4])",
        "lines = entry_path.read_text(encoding='utf-8').splitlines()",
        "",
        "output_dir = None",
        "launch_index = None",
        "launch_target = None",
        "output_modes = {'madevent', 'standalone', 'standalone_cpp', 'pythia8', 'plugin'}",
        "card_path_commands = {'run_card.dat', './run_card.dat', 'pythia8_card.dat', './pythia8_card.dat', 'param_card.dat', './param_card.dat'}",
        "",
        "def tokens_for(line: str) -> list[str]:",
        "    stripped = line.strip()",
        "    if not stripped or stripped.startswith('#'):",
        "        return []",
        "    try:",
        "        return shlex.split(stripped, comments=True)",
        "    except ValueError:",
        "        return stripped.split()",
        "",
        "for index, line in enumerate(lines):",
        "    tokens = tokens_for(line)",
        "    if not tokens:",
        "        continue",
        "    command = tokens[0].lower()",
        "    if command == 'output':",
        "        args = [token for token in tokens[1:] if not token.startswith('-')]",
        "        if args:",
        "            if args[0].lower() in output_modes and len(args) > 1:",
        "                output_dir = args[1]",
        "            else:",
        "                output_dir = args[0]",
        "    if command == 'launch':",
        "        launch_index = index",
        "        filtered = [token for token in tokens if token not in {'-i', '--interactive'}]",
        "        launch_args = [token for token in filtered[1:] if not token.startswith('-')]",
        "        if launch_args:",
        "            launch_target = launch_args[0]",
        "        break",
        "",
        "if launch_index is None:",
        "    generate_lines = lines",
        "    launch_lines: list[str] = []",
        "else:",
        "    generate_lines = lines[:launch_index]",
        "    launch_lines = []",
        "    first_launch_written = False",
        "    for raw_line in lines[launch_index:]:",
        "        stripped = raw_line.strip()",
        "        tokens = tokens_for(raw_line)",
        "        if not tokens:",
        "            launch_lines.append(raw_line)",
        "            continue",
        "        if tokens[0].lower() == 'launch' and not first_launch_written:",
        "            filtered = [token for token in tokens if token not in {'-i', '--interactive'}]",
        "            launch_lines.append(' '.join(shlex.quote(token) for token in filtered))",
        "            first_launch_written = True",
        "            continue",
        "        if stripped in card_path_commands:",
        "            print(f'Ignoring uploaded-card path command in launch block: {stripped}')",
        "            continue",
        "        shower_match = re.match(r'^\\s*shower\\s*=\\s*(.+?)\\s*$', raw_line, flags=re.IGNORECASE)",
        "        if shower_match:",
        "            launch_lines.append(f'shower={shower_match.group(1)}')",
        "            continue",
        "        launch_lines.append(raw_line)",
        "",
        "process_dir = output_dir or launch_target",
        "if not process_dir:",
        "    raise SystemExit('could not determine MG5 process directory from output/launch commands')",
        "",
        "generate_path.write_text('\\n'.join(generate_lines).rstrip() + '\\n', encoding='utf-8')",
        "launch_path.write_text('\\n'.join(launch_lines).rstrip() + ('\\n' if launch_lines else ''), encoding='utf-8')",
        "process_dir_path.write_text(process_dir + '\\n', encoding='utf-8')",
        "print(f'MG5 process directory: {process_dir}')",
        "print(f'MG5 generate command file: {generate_path}')",
        "print(f'MG5 launch command file: {launch_path}')",
        "CATENA_MG5_SPLIT",
        'echo "=== MG5 COMMAND PREP END ==="',
        'PROCESS_DIR="$(cat "$MG5_PROCESS_DIR_FILE")"',
        'echo "Detected MG5 process directory: $PROCESS_DIR"',
        "",
        'echo "=== MG5 RUN START ==="',
        'echo "=== MG5 GENERATE START ==="',
        'echo "Generate command:"',
        f"echo {shlex.quote(mg5_generate_command)}",
        f"if {mg5_generate_command}; then",
        '  echo "=== MG5 GENERATE SUCCESS ==="',
        "else",
        '  echo "=== MG5 RUN FAILED ==="',
        "  exit 1",
        "fi",
        "check_mg5_errors",
        'if [ ! -d "$PROCESS_DIR" ]; then',
        '  echo "=== MG5 RUN FAILED ==="',
        '  echo "MG5 process directory not found: $PROCESS_DIR" >&2',
        "  exit 1",
        "fi",
        "",
        'echo "=== MG5 CARD COPY START ==="',
        'if [ ! -d "$PROCESS_DIR/Cards" ]; then',
        '  echo "=== MG5 RUN FAILED ==="',
        '  echo "MG5 Cards directory not found: $PROCESS_DIR/Cards" >&2',
        "  exit 1",
        "fi",
        "for card in run_card.dat pythia8_card.dat param_card.dat; do",
        '  if [ -f "$card" ]; then',
        '    cp -f "$card" "$PROCESS_DIR/Cards/$card"',
        '    echo "Copied uploaded MG5 card: $card -> $PROCESS_DIR/Cards/$card"',
        "  else",
        '    echo "Uploaded MG5 card not present, keeping MG5 default: $card"',
        "  fi",
        "done",
        'echo "=== MG5 CARD COPY END ==="',
        "",
        'if [ -s "$MG5_LAUNCH_CMDS" ]; then',
        '  echo "=== MG5 LAUNCH START ==="',
        '  echo "Launch command:"',
        f"  echo {shlex.quote(mg5_launch_command)}",
        f"  if {mg5_launch_command}; then",
        '    echo "=== MG5 LAUNCH SUCCESS ==="',
        "  else",
        '    echo "=== MG5 RUN FAILED ==="',
        "    exit 1",
        "  fi",
        "  check_mg5_errors",
        "else",
        '  echo "No launch command found; generation-only MG5 command file completed."',
        "fi",
        'echo "=== MG5 RUN SUCCESS ==="',
        "",
        'echo "=== MG5 OUTPUT COLLECTION START ==="',
        'find "$INPUTS_DIR" -type f -print0 | while IFS= read -r -d \'\' path; do',
        '  rel_path="${path#"$INPUTS_DIR/"}"',
        '  lower_rel="$(printf "%s" "$rel_path" | tr "[:upper:]" "[:lower:]")"',
        '  case "$lower_rel" in',
        "    *.hepmc|*.hepmc.gz|*.lhe|*.lhe.gz|*banner*|*summary*|*.log)",
        '      dest="$MG5_ARTIFACTS_DIR/$rel_path"',
        '      mkdir -p "$(dirname "$dest")"',
        '      cp -f "$path" "$dest"',
        '      echo "Copied MG5 artifact: $rel_path"',
        "      ;;",
        "  esac",
        "done",
        'echo "=== MG5 OUTPUT COLLECTION END ==="',
        'echo "=== MG5 TASK END @ $(date) ==="',
    ]
    return "\n".join(lines)
