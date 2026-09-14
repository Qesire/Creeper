"""Durable accounting records for production work exposures."""

from __future__ import annotations

from dataclasses import dataclass
import math
from enum import StrEnum


class ProductionExposureState(StrEnum):
    RUNNING = "RUNNING"
    READ_COMPLETE = "READ_COMPLETE"
    VALIDATING = "VALIDATING"
    FINAL_CLOSED = "FINAL_CLOSED"
    ABORTED = "ABORTED"
    EXPIRED = "EXPIRED"


@dataclass(frozen=True)
class ProductionExposure:
    exposure_id: str
    source_key: str
    reservoir_id: str
    lease_id: str | None
    task_id: str | None
    lane: str
    baseline_signature: str
    model_signature: str
    source_records: int
    source_requests: int
    source_bytes: int
    provider_requests: int
    provider_bytes: int
    source_elapsed_seconds: float
    provider_elapsed_seconds: float
    evidence_frontier: int
    accepted_host_years: int
    final_accepted_eed: float
    state: ProductionExposureState
    terminal_reason: str | None
    created_at: float
    updated_at: float
    closed_at: float | None

    def __post_init__(self) -> None:
        for name in (
            "exposure_id",
            "source_key",
            "reservoir_id",
            "lane",
            "baseline_signature",
            "model_signature",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} is required")
        for name in ("lease_id", "task_id", "terminal_reason"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, str):
                raise ValueError(f"{name} must be a string when provided")
        if not self.lease_id and not self.task_id:
            raise ValueError("lease_id or task_id is required")
        for name in (
            "source_records",
            "source_requests",
            "source_bytes",
            "provider_requests",
            "provider_bytes",
            "evidence_frontier",
            "accepted_host_years",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        for name in (
            "source_elapsed_seconds",
            "provider_elapsed_seconds",
            "final_accepted_eed",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value < 0
            ):
                raise ValueError(f"{name} must be finite and non-negative")
        object.__setattr__(self, "state", ProductionExposureState(self.state))
        for name in ("created_at", "updated_at"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value < 0
            ):
                raise ValueError(f"{name} must be finite and non-negative")
        if self.updated_at < self.created_at:
            raise ValueError("updated_at cannot precede created_at")
        if self.closed_at is not None:
            if (
                isinstance(self.closed_at, bool)
                or not isinstance(self.closed_at, (int, float))
                or not math.isfinite(float(self.closed_at))
                or self.closed_at < self.created_at
            ):
                raise ValueError("closed_at must be finite and not precede created_at")

    @property
    def bytes_read(self) -> int:
        return self.source_bytes + self.provider_bytes

    @property
    def elapsed_seconds(self) -> float:
        return self.source_elapsed_seconds + self.provider_elapsed_seconds

    @property
    def authority(self) -> tuple[str, str]:
        return self.baseline_signature, self.model_signature

    @property
    def terminal(self) -> bool:
        return self.state in {
            ProductionExposureState.FINAL_CLOSED,
            ProductionExposureState.ABORTED,
            ProductionExposureState.EXPIRED,
        }
