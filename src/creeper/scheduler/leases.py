"""Finite, immutable work leases."""

from __future__ import annotations

from dataclasses import dataclass, replace
import math
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

    def __post_init__(self) -> None:
        if not isinstance(self.lease_id, str) or not self.lease_id.strip():
            raise ValueError("lease_id is required")
        for name in ("records", "requests", "bytes_read"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if (
            isinstance(self.elapsed_seconds, bool)
            or not isinstance(self.elapsed_seconds, (int, float))
            or not math.isfinite(float(self.elapsed_seconds))
            or self.elapsed_seconds < 0
        ):
            raise ValueError("elapsed_seconds must be finite and non-negative")
        if self.next_cursor is not None and not isinstance(self.next_cursor, str):
            raise ValueError("next_cursor must be a string when provided")


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
        object.__setattr__(self, "state", LeaseState(self.state))
        if not isinstance(self.lease_id, str) or not self.lease_id.strip():
            raise ValueError("lease_id is required")
        if not isinstance(self.reservoir_id, str) or not self.reservoir_id.strip():
            raise ValueError("reservoir_id is required")
        if not isinstance(self.resource_class, str) or not self.resource_class.strip():
            raise ValueError("resource_class is required")
        for name in ("max_records", "max_requests", "max_bytes", "expected_evidence_tasks"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if (
            isinstance(self.max_seconds, bool)
            or not isinstance(self.max_seconds, (int, float))
            or not math.isfinite(float(self.max_seconds))
            or self.max_seconds < 0
        ):
            raise ValueError("max_seconds must be finite and non-negative")
        if (
            isinstance(self.expected_novel_eed, bool)
            or not isinstance(self.expected_novel_eed, (int, float))
            or not math.isfinite(float(self.expected_novel_eed))
            or self.expected_novel_eed < 0
        ):
            raise ValueError(
                "expected_novel_eed must be finite and non-negative"
            )
        if self.expires_at is not None and (
            isinstance(self.expires_at, bool)
            or not isinstance(self.expires_at, (int, float))
            or not math.isfinite(float(self.expires_at))
            or self.expires_at < 0
        ):
            raise ValueError("expires_at must be finite and non-negative")
        if self.owner is not None and (
            not isinstance(self.owner, str) or not self.owner.strip()
        ):
            raise ValueError("owner must be non-empty when provided")
        for name in ("cursor_start", "cursor_end"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, str):
                raise ValueError(f"{name} must be a string when provided")

    @classmethod
    def create(cls, *, reservoir_id: str, max_records: int, max_requests: int,
               max_bytes: int, max_seconds: float, now: float | None = None,
               expires_at: float | None = None, cursor_start: str | None = None,
               cursor_end: str | None = None, resource_class: str = "default",
               expected_evidence_tasks: int = 0, expected_novel_eed: float = 0.0) -> "WorkLease":
        if now is not None and (
            isinstance(now, bool)
            or not isinstance(now, (int, float))
            or not math.isfinite(float(now))
            or now < 0
        ):
            raise ValueError("now must be finite and non-negative")
        if expires_at is None and now is not None:
            expires_at = float(now) + float(max_seconds)
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
        if not isinstance(owner, str) or not owner.strip():
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
        if (
            isinstance(now, bool)
            or not isinstance(now, (int, float))
            or not math.isfinite(float(now))
            or now < 0
        ):
            raise ValueError("now must be finite and non-negative")
        if self.expires_at is None or now < self.expires_at:
            raise StateTransitionError("lease has not expired")
        if self.state not in {LeaseState.GRANTED, LeaseState.RUNNING, LeaseState.PAUSED, LeaseState.PREEMPTED}:
            raise StateTransitionError(f"invalid lease transition: {self.state} -> {LeaseState.EXPIRED}")
        return LeaseState.EXPIRED

    def expired(self) -> "WorkLease":
        if self.state is LeaseState.EXPIRED:
            return self
        if self.state not in {
            LeaseState.GRANTED,
            LeaseState.RUNNING,
            LeaseState.PAUSED,
            LeaseState.PREEMPTED,
        }:
            raise StateTransitionError(
                f"invalid lease transition: {self.state} -> {LeaseState.EXPIRED}"
            )
        return replace(self, state=LeaseState.EXPIRED)

    def resume(self) -> "WorkLease":
        if self.state not in {LeaseState.PAUSED, LeaseState.PREEMPTED}:
            raise StateTransitionError(f"invalid lease transition: {self.state} -> {LeaseState.GRANTED}")
        return replace(self, state=LeaseState.GRANTED)

    def retry(self) -> "WorkLease":
        if self.state not in {LeaseState.EXPIRED, LeaseState.ABORTED, LeaseState.PREEMPTED}:
            raise StateTransitionError("only ended leases can be retried")
        # A retry is a fresh lease identity. Carrying the terminal lease's old
        # deadline would make an expired lease immediately expire again after
        # it is granted. The caller/control plane must assign a new deadline.
        return replace(
            self,
            lease_id=str(uuid4()),
            state=LeaseState.CREATED,
            owner=None,
            expires_at=None,
        )
