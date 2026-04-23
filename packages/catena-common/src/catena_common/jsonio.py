"""Small JSON helpers shared across Catena packages."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def dumps_json(payload: Any, *, indent: int = 2, sort_keys: bool = False) -> str:
    """Serialize a payload to JSON."""

    return json.dumps(payload, indent=indent, sort_keys=sort_keys)


def dump_json_file(path: str | Path, payload: Any, *, indent: int = 2, sort_keys: bool = False) -> None:
    """Write a JSON payload to disk with a trailing newline."""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(dumps_json(payload, indent=indent, sort_keys=sort_keys) + "\n", encoding="utf-8")


def load_json_text(text: str) -> Any:
    """Parse JSON from a string."""

    return json.loads(text)


def load_json_file(path: str | Path) -> Any:
    """Parse JSON from a file."""

    return load_json_text(Path(path).read_text(encoding="utf-8"))
