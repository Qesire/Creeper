"""Durable accounting records for production work exposures."""

from __future__ import annotations

from dataclasses import dataclass
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
