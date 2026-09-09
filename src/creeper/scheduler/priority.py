"""Stable source ranking based on measured engineering yield."""

from __future__ import annotations

from dataclasses import dataclass

from creeper.records.models import SourceStats


@dataclass(frozen=True)
class SourceDecision:
    source_id: str
    priority: float
    reason: str


def rank_sources(stats: list[SourceStats]) -> list[SourceDecision]:
    decisions = []
    for item in stats:
        priority = item.yield_per_hour
        if item.saturated:
            priority *= 0.25
        decisions.append(SourceDecision(item.source_id, priority, "yield_per_hour"))
    return sorted(decisions, key=lambda item: (-item.priority, item.source_id))
