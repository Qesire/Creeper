"""Stable source ranking based on measured engineering yield."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from numbers import Real
from typing import TYPE_CHECKING

from creeper.records.models import SourceStats

if TYPE_CHECKING:
    from creeper.scheduler.leases import WorkLease
    from creeper.sources.reservoirs import Reservoir


@dataclass(frozen=True)
class ResourceCost:
    """Estimated consumption of the four V2.2 scheduler resources."""

    general_network: float
    evidence_network: float
    cpu: float
    ssd: float

    def __post_init__(self) -> None:
        values = (self.general_network, self.evidence_network, self.cpu, self.ssd)
        if any(not isinstance(value, Real) or isinstance(value, bool) or value < 0 or not isfinite(value)
               for value in values):
            raise ValueError("resource costs must be finite non-negative numbers")

    def normalized(self, capacities: dict[str, float] | None = None) -> float:
        capacities = capacities or {
            "general_network": 1.0,
            "evidence_network": 1.0,
            "cpu": 1.0,
            "ssd": 1.0,
        }
        values = {
            "general_network": self.general_network,
            "evidence_network": self.evidence_network,
            "cpu": self.cpu,
            "ssd": self.ssd,
        }
        normalized = []
        for name, value in values.items():
            capacity = capacities.get(name, 1.0)
            if not isinstance(capacity, Real) or isinstance(capacity, bool) or capacity <= 0:
                raise ValueError("resource capacities must be positive numbers")
            normalized.append(value / capacity)
        return max(normalized, default=0.0)


@dataclass(frozen=True)
class LeaseCandidate:
    """A finite reservoir lease proposal scored by downstream value."""

    reservoir_id: str
    expected_novel_eed: float
    costs: ResourceCost
    reservoir: "Reservoir | None" = None
    lease: "WorkLease | None" = None
    evidence_mode: str = "discovery_only"
    evidence_provider: str = "wayback"
    expected_evidence_tasks: int = 0
    # Hard backlog-reservation bound. This is deliberately separate from
    # expected_evidence_tasks: planner expansion can create multiple disjoint
    # year scopes from one source observation even when the expected cost is
    # near one task/record.
    reservation_evidence_tasks: int | None = None
    source_key: str | None = None

    def __post_init__(self) -> None:
        if not self.reservoir_id.strip():
            raise ValueError("reservoir_id is required")
        if not isinstance(self.expected_novel_eed, Real) or isinstance(self.expected_novel_eed, bool):
            raise ValueError("expected_novel_eed must be numeric")
        if self.expected_novel_eed < 0 or not isfinite(self.expected_novel_eed):
            raise ValueError("expected_novel_eed must be finite and non-negative")
        if not isinstance(self.expected_evidence_tasks, int) or self.expected_evidence_tasks < 0:
            raise ValueError("expected_evidence_tasks must be non-negative")
        if (
            self.reservation_evidence_tasks is not None
            and (
                not isinstance(self.reservation_evidence_tasks, int)
                or isinstance(self.reservation_evidence_tasks, bool)
                or self.reservation_evidence_tasks < self.expected_evidence_tasks
            )
        ):
            raise ValueError(
                "reservation_evidence_tasks must be an integer not below expected_evidence_tasks"
            )
        if not self.evidence_provider.strip():
            raise ValueError("evidence_provider is required")
        if self.source_key is not None and not self.source_key.strip():
            raise ValueError("source_key must be non-empty when provided")


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
        decisions.append(SourceDecision(item.source_id, priority, "engineering_only_baseline_external"))
    return sorted(decisions, key=lambda item: (-item.priority, item.source_id))
