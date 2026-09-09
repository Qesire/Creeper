"""Deterministic scheduler for finite work leases."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from creeper.scheduler.credits import CreditLedger
from creeper.scheduler.leases import LeaseState, WorkLease
from creeper.scheduler.priority import LeaseCandidate
from creeper.sources.reservoirs import ReservoirState


@dataclass(frozen=True)
class RankedCandidate:
    candidate: LeaseCandidate
    priority: float

    @property
    def reservoir_id(self) -> str:
        return self.candidate.reservoir_id


class GlobalScheduler:
    """Rank and grant one bounded lease at a time.

    The scheduler intentionally has no network or persistence side effects. A
    caller owns persistence of the returned lease; this class only accounts
    for evidence reservation and tracks the in-memory reservoir state needed
    by the phase-1 scheduling boundary.
    """

    def __init__(
        self,
        ledger: CreditLedger,
        *,
        resource_capacities: Mapping[str, float] | None = None,
    ) -> None:
        self.ledger = ledger
        self.resource_capacities = dict(resource_capacities or {})
        self._reservoir_states: dict[str, ReservoirState] = {}

    def score(self, candidate: LeaseCandidate) -> float:
        denominator = candidate.costs.normalized(self.resource_capacities)
        return candidate.expected_novel_eed / (denominator or 1.0)

    def rank(self, candidates: Iterable[LeaseCandidate]) -> list[LeaseCandidate]:
        return sorted(
            candidates,
            key=lambda candidate: (-self.score(candidate), candidate.reservoir_id),
        )

    def _is_direct(self, candidate: LeaseCandidate) -> bool:
        mode = candidate.evidence_mode
        if candidate.reservoir is not None:
            mode = candidate.reservoir.evidence_mode
        return mode == "direct_year"

    def _lease_for(self, candidate: LeaseCandidate) -> WorkLease:
        if candidate.lease is not None:
            return candidate.lease
        capacity = candidate.reservoir.capacity_lower if candidate.reservoir is not None else 1
        return WorkLease.create(
            reservoir_id=candidate.reservoir_id,
            max_records=max(1, capacity),
            max_requests=max(1, candidate.expected_evidence_tasks),
            max_bytes=max(1, capacity),
            max_seconds=1.0,
        )

    def grant_next(
        self,
        candidates: Iterable[LeaseCandidate],
        *,
        owner: str = "global-scheduler",
    ) -> WorkLease | None:
        for candidate in self.rank(candidates):
            if candidate.reservoir is not None and candidate.reservoir.state is not ReservoirState.READY:
                continue
            if candidate.reservoir is not None:
                self._reservoir_states[candidate.reservoir_id] = candidate.reservoir.state
            tasks = 0 if self._is_direct(candidate) else candidate.expected_evidence_tasks
            reserved = False
            if tasks:
                reserved = self.ledger.reserve_evidence(candidate.evidence_provider, tasks)
                if not reserved:
                    continue
            lease = self._lease_for(candidate)
            try:
                granted = lease.grant(owner=owner) if lease.state is LeaseState.CREATED else lease
            except Exception:
                if reserved:
                    self.ledger.release_evidence(candidate.evidence_provider, tasks)
                raise
            self._reservoir_states[candidate.reservoir_id] = ReservoirState.LEASED
            return granted
        return None

    def reservoir_state(self, reservoir_id: str) -> ReservoirState | None:
        return self._reservoir_states.get(reservoir_id)
