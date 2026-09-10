"""Deterministic scheduler for finite work leases."""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Iterable, Mapping

from creeper.scheduler.credits import CreditLedger
from creeper.scheduler.leases import WorkLease
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
    """Rank candidates and reserve bounded evidence credits.

    Reservoir and lease ownership belong to ControlStore.  The compatibility
    ``grant_next`` method only performs ranking, credit reservation, and
    construction of a fresh lease for older callers.
    """

    def __init__(
        self,
        ledger: CreditLedger,
        *,
        resource_capacities: Mapping[str, float] | None = None,
    ) -> None:
        self.ledger = ledger
        self.resource_capacities = dict(resource_capacities or {})

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

    @staticmethod
    def _fresh_lease(candidate: LeaseCandidate, *, owner: str) -> WorkLease:
        template = candidate.lease
        capacity = candidate.reservoir.capacity_lower if candidate.reservoir is not None else 1
        lease = WorkLease.create(
            reservoir_id=candidate.reservoir_id,
            cursor_start=(
                template.cursor_start
                if template is not None
                else candidate.reservoir.cursor if candidate.reservoir is not None else None
            ),
            cursor_end=template.cursor_end if template is not None else None,
            max_records=template.max_records if template is not None else max(1, capacity),
            max_requests=(
                template.max_requests
                if template is not None
                else max(1, candidate.expected_evidence_tasks)
            ),
            max_bytes=template.max_bytes if template is not None else max(1, capacity),
            max_seconds=template.max_seconds if template is not None else 1.0,
            resource_class=template.resource_class if template is not None else "default",
            expected_evidence_tasks=candidate.expected_evidence_tasks,
            expected_novel_eed=candidate.expected_novel_eed,
            now=time.time(),
        )
        return lease.grant(owner=owner)

    def grant_next(
        self,
        candidates: Iterable[LeaseCandidate],
        *,
        owner: str = "global-scheduler",
    ) -> WorkLease | None:
        for candidate in self.rank(candidates):
            if candidate.reservoir is not None and candidate.reservoir.state is not ReservoirState.READY:
                continue
            tasks = 0 if self._is_direct(candidate) else candidate.expected_evidence_tasks
            reserved = False
            if tasks:
                reserved = self.ledger.reserve_evidence(candidate.evidence_provider, tasks)
                if not reserved:
                    continue
            try:
                granted = self._fresh_lease(candidate, owner=owner)
            except Exception:
                if reserved:
                    self.ledger.release_evidence(candidate.evidence_provider, tasks)
                raise
            return granted
        return None
