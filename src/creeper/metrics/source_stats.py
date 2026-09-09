"""Metric aggregation kept separate from the official competition score."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SourceMetric:
    source_id: str
    requests: int = 0
    baseline_external: int = 0
    incomplete_queries: int = 0
    elapsed_seconds: float = 0.0

    def as_dict(self) -> dict[str, int | float | str]:
        return {
            "source_id": self.source_id,
            "requests": self.requests,
            "baseline_external": self.baseline_external,
            "incomplete_queries": self.incomplete_queries,
            "elapsed_seconds": self.elapsed_seconds,
            "novel_per_hour": self.baseline_external / (self.elapsed_seconds / 3600)
            if self.elapsed_seconds
            else 0.0,
        }
