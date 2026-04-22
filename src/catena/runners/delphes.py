"""Delphes runner stub."""

from __future__ import annotations

from dataclasses import dataclass

from catena.runners.base import Runner


@dataclass(slots=True)
class DelphesRunner(Runner):
    """Placeholder runner for Delphes jobs."""

    name: str = "delphes"
