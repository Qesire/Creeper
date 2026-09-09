"""Finite per-source budgets."""

from dataclasses import dataclass


@dataclass
class SourceBudget:
    max_records: int
    max_seconds: float
    records_used: int = 0
    seconds_used: float = 0.0

    def allows(self) -> bool:
        return self.records_used < self.max_records and self.seconds_used < self.max_seconds
