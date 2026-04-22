"""C++ runner stub."""

from __future__ import annotations

from dataclasses import dataclass

from catena.runners.base import Runner


@dataclass(slots=True)
class CppRunner(Runner):
    """Placeholder runner for C++ jobs."""

    name: str = "cpp"
