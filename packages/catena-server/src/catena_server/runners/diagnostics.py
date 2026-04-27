"""Shell snippets for best-effort runner diagnostics."""

from __future__ import annotations

import shlex

from catena_common import config


def shell_diagnostics_lines(job_id: str) -> list[str]:
    """Return shell lines that append JSONL diagnostics records when called."""

    return [
        f"CATENA_DIAGNOSTICS_LOG={shlex.quote(config.MISSING_TOOLS_LOG)}",
        f"CATENA_DIAGNOSTICS_JOB_ID={shlex.quote(job_id)}",
        "catena_log_missing_dependency() {",
        '  local operation_type="$1"',
        '  local env_name="$2"',
        '  local missing="$3"',
        '  local command_text="$4"',
        '  local exit_code="${5:-}"',
        '  local stderr_text="${6:-}"',
        '  mkdir -p "$(dirname "$CATENA_DIAGNOSTICS_LOG")" 2>/dev/null || true',
        "  python3 - \"$CATENA_DIAGNOSTICS_LOG\" \"$operation_type\" \"$env_name\" \"$missing\" \"$command_text\" \"$exit_code\" \"$stderr_text\" \"$CATENA_DIAGNOSTICS_JOB_ID\" <<'PY' 2>/dev/null || true",
        "import datetime",
        "import json",
        "import sys",
        "path, operation_type, env_name, missing, command_text, exit_code, stderr_text, job_id = sys.argv[1:9]",
        "record = {",
        "    'timestamp': datetime.datetime.now(datetime.UTC).isoformat(),",
        "    'scope': 'runner',",
        "    'operation_type': operation_type,",
        "    'job_id': job_id,",
        "    'env': env_name or None,",
        "    'missing': missing,",
        "    'category': 'runtime_dependency',",
        "    'command': command_text,",
        "    'exit_code': int(exit_code) if exit_code.isdigit() else None,",
        "    'stderr': stderr_text or None,",
        "    'message': None,",
        "}",
        "with open(path, 'a', encoding='utf-8') as handle:",
        "    handle.write(json.dumps(record, sort_keys=True) + '\\n')",
        "PY",
        "}",
    ]
