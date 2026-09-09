"""Finite, immutable work leases."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from uuid import uuid4


class StateTransitionError(ValueError):
    """Raised when a lifecycle transition is not permitted."""


class LeaseState(StrEnum):
    CREATED = "CREATED"
    GRANTED = "GRANTED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    PAUSED = "PAUSED"
    PREEMPTED = "PREEMPTED"
    ABORTED = "ABORTED"
    EXPIRED = "EXPIRED"


@dataclass(frozen=True)
class LeaseResult:
    lease_id: str
    records: int = 0
    requests: int = 0
    bytes_read: int = 0
    elapsed_seconds: float = 0.0
    next_cursor: str | None = None


@dataclass(frozen=True)
class WorkLease:
    lease_id: str
    reservoir_id: str
    cursor_start: str | None
    cursor_end: str | None
    max_records: int
    max_requests: int
    max_bytes: int
    max_seconds: float
    resource_class: str = "default"
    expected_evidence_tasks: int = 0
    expected_novel_eed: float = 0.0
    owner: str | None = None
    expires_at: float | None = None
    state: LeaseState = LeaseState.CREATED

    def __post_init__(self) -> None:
        if not self.reservoir_id.strip():
            raise ValueError("reservoir_id is required")
        if min(self.max_records, self.max_requests, self.max_bytes) < 0 or self.max_seconds < 0:
            raise ValueError("lease limits must be non-negative")
        if self.expires_at is not None and self.expires_at < 0:
            raise ValueError("expires_at must be non-negative")

    @classmethod
    def create(cls, *, reservoir_id: str, max_records: int, max_requests: int,
               max_bytes: int, max_seconds: float, now: float | None = None,
               expires_at: float | None = None, cursor_start: str | None = None,
               cursor_end: str | None = None, resource_class: str = "default",
               expected_evidence_tasks: int = 0, expected_novel_eed: float = 0.0) -> "WorkLease":
        if expires_at is None and now is not None:
            expires_at = now + max_seconds
        return cls(str(uuid4()), reservoir_id, cursor_start, cursor_end, max_records,
                   max_requests, max_bytes, max_seconds, resource_class,
                   expected_evidence_tasks, expected_novel_eed, None, expires_at)

    def allows(self, *, records: int, requests: int, bytes_read: int, elapsed_seconds: float) -> bool:
        return (0 <= records < self.max_records and 0 <= requests < self.max_requests
                and 0 <= bytes_read < self.max_bytes and 0 <= elapsed_seconds < self.max_seconds)

    def _move(self, current: LeaseState, target: LeaseState, **changes) -> "WorkLease":
        if self.state is not current:
            raise StateTransitionError(f"invalid lease transition: {self.state} -> {target}")
        return replace(self, state=target, **changes)

    def grant(self, *, owner: str) -> "WorkLease":
        if not owner.strip():
            raise ValueError("owner is required")
        return self._move(LeaseState.CREATED, LeaseState.GRANTED, owner=owner)

    def start(self) -> "WorkLease":
        return self._move(LeaseState.GRANTED, LeaseState.RUNNING)

    def complete(self) -> "WorkLease":
        return self._move(LeaseState.RUNNING, LeaseState.SUCCEEDED)

    def pause(self) -> "WorkLease":
        return self._move(LeaseState.RUNNING, LeaseState.PAUSED)

    def preempt(self) -> "WorkLease":
        return self._move(LeaseState.RUNNING, LeaseState.PREEMPTED)

    def abort(self) -> "WorkLease":
        if self.state not in {LeaseState.CREATED, LeaseState.GRANTED, LeaseState.RUNNING, LeaseState.PAUSED, LeaseState.PREEMPTED}:
            raise StateTransitionError(f"invalid lease transition: {self.state} -> {LeaseState.ABORTED}")
        return replace(self, state=LeaseState.ABORTED)

    def expire(self, now: float) -> LeaseState:
        if self.expires_at is None or now < self.expires_at:
            raise StateTransitionError("lease has not expired")
        if self.state not in {LeaseState.GRANTED, LeaseState.RUNNING, LeaseState.PAUSED, LeaseState.PREEMPTED}:
            raise StateTransitionError(f"invalid lease transition: {self.state} -> {LeaseState.EXPIRED}")
        return LeaseState.EXPIRED

    def expired(self) -> "WorkLease":
        if self.state is not LeaseState.EXPIRED:
            return replace(self, state=LeaseState.EXPIRED)
        return self

    def resume(self) -> "WorkLease":
        if self.state not in {LeaseState.PAUSED, LeaseState.PREEMPTED}:
            raise StateTransitionError(f"invalid lease transition: {self.state} -> {LeaseState.GRANTED}")
        return replace(self, state=LeaseState.GRANTED)

    def retry(self) -> "WorkLease":
        if self.state not in {LeaseState.EXPIRED, LeaseState.ABORTED, LeaseState.PREEMPTED}:
            raise StateTransitionError("only ended leases can be retried")
        return replace(self, lease_id=str(uuid4()), state=LeaseState.CREATED, owner=None)
