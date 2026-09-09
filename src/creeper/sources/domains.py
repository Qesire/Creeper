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
        if not self.domain_id.strip() or not self.family.strip():
            raise ValueError("domain_id and family are required")
        if len(self.temporal_scope) != 2 or self.temporal_scope[0] > self.temporal_scope[1]:
            raise ValueError("temporal_scope must be an ordered year pair")

    def transition(self, state: DomainState) -> "SourceDomain":
        state = DomainState(state)
        if state not in _DOMAIN_TRANSITIONS[self.state]:
            raise StateTransitionError(f"invalid domain transition: {self.state} -> {state}")
        return replace(self, state=state)
