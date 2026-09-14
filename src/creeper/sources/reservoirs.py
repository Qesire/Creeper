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
        for name in ("capacity_lower", "sampled_records"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.capacity_upper is not None and (
            isinstance(self.capacity_upper, bool)
            or not isinstance(self.capacity_upper, int)
            or self.capacity_upper < self.capacity_lower
        ):
            raise ValueError(
                "capacity_upper must be an integer not below capacity_lower"
            )


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
        for name in (
            "reservoir_id",
            "domain_id",
            "adapter_id",
            "root_locator",
            "enumeration_kind",
            "evidence_mode",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} is required")
        if self.cursor is not None and not isinstance(self.cursor, str):
            raise ValueError("cursor must be a string when provided")
        object.__setattr__(self, "state", ReservoirState(self.state))

    def transition(self, state: ReservoirState) -> "Reservoir":
        state = ReservoirState(state)
        if state not in _RESERVOIR_TRANSITIONS[self.state]:
            raise StateTransitionError(f"invalid reservoir transition: {self.state} -> {state}")
        return replace(self, state=state)
