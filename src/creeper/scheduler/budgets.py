"""Finite per-source budgets."""

from dataclasses import dataclass


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

    def allows(self) -> bool:
        return (self.records_used < self.max_records and self.seconds_used < self.max_seconds
                and (self.max_requests is None or self.requests_used < self.max_requests)
                and (self.max_bytes is None or self.bytes_used < self.max_bytes))
