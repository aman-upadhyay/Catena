"""Sherpa runner stub."""

from __future__ import annotations

from dataclasses import dataclass

from catena.runners.base import Runner


@dataclass(slots=True)
class SherpaRunner(Runner):
    """Placeholder runner for Sherpa jobs."""

    name: str = "sherpa"
