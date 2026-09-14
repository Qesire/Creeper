"""Finite per-source budgets."""

from dataclasses import dataclass
import math


@dataclass
class SourceBudget:
    max_records: int
    max_seconds: float
    records_used: int = 0
    seconds_used: float = 0.0
    max_requests: int | None = None
    max_bytes: int | None = None
    requests_used: int = 0
    bytes_used: int = 0

    def __post_init__(self) -> None:
        for name in ("max_records", "records_used", "requests_used", "bytes_used"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        for name in ("max_requests", "max_bytes"):
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise ValueError(f"{name} must be a non-negative integer when provided")
        for name in ("max_seconds", "seconds_used"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value < 0
            ):
                raise ValueError(f"{name} must be finite and non-negative")

    def allows(self) -> bool:
        return (self.records_used < self.max_records and self.seconds_used < self.max_seconds
                and (self.max_requests is None or self.requests_used < self.max_requests)
                and (self.max_bytes is None or self.bytes_used < self.max_bytes))
