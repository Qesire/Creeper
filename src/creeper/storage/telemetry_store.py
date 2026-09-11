"""Small multi-process runtime telemetry store.

This database is operational observability only. It is deliberately separate
from ControlStore/EvidenceStore and must never be used as evidence, novelty, or
submission authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
import time
from typing import Mapping


@dataclass(frozen=True)
class TelemetrySnapshot:
    counters: dict[str, int]
    gauges: dict[str, float]
    gauge_updated_at: dict[str, float]


class RuntimeTelemetryStore:
    def __init__(self, path: Path, *, clock=time.time) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = Path(path)
        self.clock = clock
        self.connection = sqlite3.connect(path, timeout=30.0)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA busy_timeout=30000")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS runtime_counters (
                name TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS runtime_gauges (
                name TEXT PRIMARY KEY,
                value REAL NOT NULL,
                updated_at REAL NOT NULL
            ) WITHOUT ROWID;
            """
        )
        self.connection.commit()

    @staticmethod
    def _counter_rows(values: Mapping[str, int]) -> list[tuple[str, int]]:
        rows: list[tuple[str, int]] = []
        for name, raw in values.items():
            if not isinstance(name, str) or not name.strip():
                raise ValueError("telemetry counter names must be non-empty strings")
            if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
                raise ValueError("telemetry counter deltas must be non-negative integers")
            if raw:
                rows.append((name, raw))
        return rows

    @staticmethod
    def _gauge_rows(
        values: Mapping[str, int | float],
        *,
        updated_at: float,
    ) -> list[tuple[str, float, float]]:
        rows: list[tuple[str, float, float]] = []
        for name, raw in values.items():
            if not isinstance(name, str) or not name.strip():
                raise ValueError("telemetry gauge names must be non-empty strings")
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                raise ValueError("telemetry gauge values must be numeric")
            rows.append((name, float(raw), float(updated_at)))
        return rows

    def add_counters(self, values: Mapping[str, int]) -> None:
        rows = self._counter_rows(values)
        if not rows:
            return
        with self.connection:
            self.connection.executemany(
                """
                INSERT INTO runtime_counters(name, value)
                VALUES (?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    value = runtime_counters.value + excluded.value
                """,
                rows,
            )

    def set_gauges(self, values: Mapping[str, int | float]) -> None:
        rows = self._gauge_rows(values, updated_at=float(self.clock()))
        if not rows:
            return
        with self.connection:
            self.connection.executemany(
                """
                INSERT INTO runtime_gauges(name, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                rows,
            )

    def set_max_gauges(self, values: Mapping[str, int | float]) -> None:
        rows = self._gauge_rows(values, updated_at=float(self.clock()))
        if not rows:
            return
        with self.connection:
            self.connection.executemany(
                """
                INSERT INTO runtime_gauges(name, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    value = MAX(runtime_gauges.value, excluded.value),
                    updated_at = excluded.updated_at
                """,
                rows,
            )

    def set_min_gauges(self, values: Mapping[str, int | float]) -> None:
        rows = self._gauge_rows(values, updated_at=float(self.clock()))
        if not rows:
            return
        with self.connection:
            self.connection.executemany(
                """
                INSERT INTO runtime_gauges(name, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    value = MIN(runtime_gauges.value, excluded.value),
                    updated_at = excluded.updated_at
                """,
                rows,
            )

    def snapshot(self) -> TelemetrySnapshot:
        counters = {
            str(row["name"]): int(row["value"])
            for row in self.connection.execute(
                "SELECT name, value FROM runtime_counters ORDER BY name"
            )
        }
        gauge_rows = self.connection.execute(
            "SELECT name, value, updated_at FROM runtime_gauges ORDER BY name"
        ).fetchall()
        return TelemetrySnapshot(
            counters=counters,
            gauges={str(row["name"]): float(row["value"]) for row in gauge_rows},
            gauge_updated_at={
                str(row["name"]): float(row["updated_at"])
                for row in gauge_rows
            },
        )

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "RuntimeTelemetryStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
