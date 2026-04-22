"""Base runner definitions for Catena."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class Runner:
    """Minimal base class for Catena runners."""

    name: str

    def describe(self) -> str:
        """Return a short human-readable label."""

        return f"{self.name} runner stub"
