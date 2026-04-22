"""MG5 + Pythia runner stub."""

from __future__ import annotations

from dataclasses import dataclass

from catena.runners.base import Runner


@dataclass(slots=True)
class MG5PythiaRunner(Runner):
    """Placeholder runner for MG5 + Pythia jobs."""

    name: str = "mg5-pythia"
