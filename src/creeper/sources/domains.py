"""Immutable source-domain metadata and lifecycle."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum

from creeper.scheduler.leases import StateTransitionError


class DomainState(StrEnum):
    UNEXPLORED = "UNEXPLORED"
    EXPLORING = "EXPLORING"
    PRODUCTIVE = "PRODUCTIVE"
    DECLINING = "DECLINING"
    DORMANT = "DORMANT"
    EXHAUSTED = "EXHAUSTED"


_DOMAIN_TRANSITIONS = {
    DomainState.UNEXPLORED: frozenset({DomainState.EXPLORING}),
    DomainState.EXPLORING: frozenset({DomainState.PRODUCTIVE, DomainState.DORMANT}),
    DomainState.PRODUCTIVE: frozenset({DomainState.DECLINING, DomainState.EXHAUSTED}),
    DomainState.DECLINING: frozenset({DomainState.PRODUCTIVE, DomainState.DORMANT, DomainState.EXHAUSTED}),
    DomainState.DORMANT: frozenset({DomainState.EXPLORING, DomainState.EXHAUSTED}),
    DomainState.EXHAUSTED: frozenset(),
}


@dataclass(frozen=True)
class SourceDomain:
    domain_id: str
    family: str
    discovery_mechanism: str
    temporal_scope: tuple[int, int]
    state: DomainState = DomainState.UNEXPLORED

    def __post_init__(self) -> None:
        for name in ("domain_id", "family", "discovery_mechanism"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} is required")
        if (
            not isinstance(self.temporal_scope, tuple)
            or len(self.temporal_scope) != 2
            or any(
                isinstance(value, bool) or not isinstance(value, int)
                for value in self.temporal_scope
            )
            or self.temporal_scope[0] > self.temporal_scope[1]
        ):
            raise ValueError("temporal_scope must be an ordered integer year pair")
        object.__setattr__(self, "state", DomainState(self.state))

    def transition(self, state: DomainState) -> "SourceDomain":
        state = DomainState(state)
        if state not in _DOMAIN_TRANSITIONS[self.state]:
            raise StateTransitionError(f"invalid domain transition: {self.state} -> {state}")
        return replace(self, state=state)
