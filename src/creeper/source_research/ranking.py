"""Deterministic research ranking from downstream yield, not hit count."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ResearchYield:
    hits: int = 0
    resolved_artifacts: int = 0
    productive_sources: int = 0
    final_eed: float = 0.0
    duplicate_hits: int = 0
    requests: int = 0

    def score(self) -> float:
        requests = max(1, self.requests)
        useful = (
            1000.0 * max(0.0, self.final_eed)
            + 50.0 * max(0, self.productive_sources)
            + 8.0 * max(0, self.resolved_artifacts)
        )
        bloat = max(0, self.hits - 4 * self.resolved_artifacts)
        penalty = 0.02 * bloat + 0.05 * max(0, self.duplicate_hits)
        return useful / requests - penalty / requests


__all__ = ["ResearchYield"]
