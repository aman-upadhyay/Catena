"""Python runner stub."""

from __future__ import annotations

from dataclasses import dataclass

from catena.runners.base import Runner


@dataclass(slots=True)
class PythonRunner(Runner):
    """Placeholder runner for Python-based jobs."""

    name: str = "python"
