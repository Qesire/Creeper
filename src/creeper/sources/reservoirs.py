"""Immutable finite reservoirs and their estimates."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum

from creeper.scheduler.leases import StateTransitionError


class ReservoirState(StrEnum):
    DISCOVERED = "DISCOVERED"
    QUALIFYING = "QUALIFYING"
    READY = "READY"
    LEASED = "LEASED"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    PREEMPTED = "PREEMPTED"
    ABORTED = "ABORTED"
    EXHAUSTED = "EXHAUSTED"


@dataclass(frozen=True)
class ReservoirEstimate:
    capacity_lower: int
    capacity_upper: int | None = None
    sampled_records: int = 0

    def __post_init__(self) -> None:
        if self.capacity_lower < 0 or self.sampled_records < 0:
            raise ValueError("capacity and sample counts must be non-negative")
        if self.capacity_upper is not None and self.capacity_upper < self.capacity_lower:
            raise ValueError("capacity_upper must not be below capacity_lower")


_RESERVOIR_TRANSITIONS = {
    ReservoirState.DISCOVERED: frozenset({ReservoirState.QUALIFYING}),
    ReservoirState.QUALIFYING: frozenset({ReservoirState.READY, ReservoirState.ABORTED}),
    ReservoirState.READY: frozenset({ReservoirState.LEASED, ReservoirState.EXHAUSTED, ReservoirState.ABORTED}),
    ReservoirState.LEASED: frozenset({ReservoirState.RUNNING, ReservoirState.PAUSED, ReservoirState.PREEMPTED, ReservoirState.ABORTED}),
    ReservoirState.RUNNING: frozenset({ReservoirState.READY, ReservoirState.PAUSED, ReservoirState.PREEMPTED, ReservoirState.EXHAUSTED, ReservoirState.ABORTED}),
    ReservoirState.PAUSED: frozenset({ReservoirState.LEASED, ReservoirState.ABORTED}),
    ReservoirState.PREEMPTED: frozenset({ReservoirState.LEASED, ReservoirState.ABORTED}),
    ReservoirState.ABORTED: frozenset(),
    ReservoirState.EXHAUSTED: frozenset(),
}


@dataclass(frozen=True)
class Reservoir:
    reservoir_id: str
    domain_id: str
    adapter_id: str
    root_locator: str
    enumeration_kind: str
    capacity_lower: int
    capacity_upper: int | None = None
    evidence_mode: str = "discovery_only"
    cursor: str | None = None
    state: ReservoirState = ReservoirState.DISCOVERED

    def __post_init__(self) -> None:
        ReservoirEstimate(self.capacity_lower, self.capacity_upper)
        if not self.reservoir_id.strip() or not self.domain_id.strip() or not self.adapter_id.strip():
            raise ValueError("reservoir identity fields are required")

    def transition(self, state: ReservoirState) -> "Reservoir":
        state = ReservoirState(state)
        if state not in _RESERVOIR_TRANSITIONS[self.state]:
            raise StateTransitionError(f"invalid reservoir transition: {self.state} -> {state}")
        return replace(self, state=state)
