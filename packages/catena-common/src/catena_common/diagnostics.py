"""Best-effort local diagnostics logging for missing runtime dependencies."""

from __future__ import annotations

import json
import shlex
from datetime import UTC, datetime
from pathlib import Path
from typing import Sequence

from catena_common import config


def command_to_text(command: str | Sequence[str]) -> str:
    """Return a stable shell-style command string for diagnostics."""

    if isinstance(command, str):
        return command
    return shlex.join(str(part) for part in command)


def log_missing_dependency(
    *,
    operation_type: str,
    missing: str,
    command: str | Sequence[str],
    env: str | None = None,
    scope: str | None = None,
    job_id: str | None = None,
    category: str | None = None,
    exit_code: int | None = None,
    stderr: str | None = None,
    message: str | None = None,
) -> None:
    """Append a JSONL diagnostics record without affecting Catena behavior."""

    try:
        log_path = Path(config.MISSING_TOOLS_LOG)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "timestamp": datetime.now(UTC).isoformat(),
            "scope": scope,
            "operation_type": operation_type,
            "job_id": job_id,
            "env": env,
            "missing": missing,
            "category": category,
            "command": command_to_text(command),
            "exit_code": exit_code,
            "stderr": stderr,
            "message": message,
        }
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
    except Exception:
        # Diagnostics must never change command success/failure behavior.
        return
