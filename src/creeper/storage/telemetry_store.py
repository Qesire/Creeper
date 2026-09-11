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

            CREATE TABLE IF NOT EXISTS resource_samples (
                sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
                sampled_at REAL NOT NULL,
                rss_bytes INTEGER NOT NULL,
                disk_free_bytes INTEGER NOT NULL,
                governor_state TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_resource_samples_time
                ON resource_samples(sampled_at);
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

    def append_resource_sample(
        self,
        *,
        rss_bytes: int,
        disk_free_bytes: int,
        governor_state: str,
        sampled_at: float | None = None,
    ) -> None:
        if (
            isinstance(rss_bytes, bool)
            or not isinstance(rss_bytes, int)
            or rss_bytes < 0
            or isinstance(disk_free_bytes, bool)
            or not isinstance(disk_free_bytes, int)
            or disk_free_bytes < 0
        ):
            raise ValueError("resource byte samples must be non-negative integers")
        if not isinstance(governor_state, str) or not governor_state.strip():
            raise ValueError("governor_state must be a non-empty string")
        when = float(self.clock()) if sampled_at is None else float(sampled_at)
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO resource_samples(
                    sampled_at, rss_bytes, disk_free_bytes, governor_state
                ) VALUES (?, ?, ?, ?)
                """,
                (when, rss_bytes, disk_free_bytes, governor_state),
            )

    def resource_summary(
        self,
        *,
        start_time: float,
        end_time: float,
    ) -> dict[str, object]:
        if end_time < start_time:
            raise ValueError("resource summary end_time precedes start_time")
        row = self.connection.execute(
            """
            SELECT COUNT(*) AS samples,
                   MAX(rss_bytes) AS peak_rss_bytes,
                   AVG(rss_bytes) AS mean_rss_bytes,
                   MIN(disk_free_bytes) AS min_disk_free_bytes,
                   MAX(disk_free_bytes) AS max_disk_free_bytes
            FROM resource_samples
            WHERE sampled_at >= ? AND sampled_at <= ?
            """,
            (float(start_time), float(end_time)),
        ).fetchone()
        states = {
            str(item["governor_state"]): int(item["count"])
            for item in self.connection.execute(
                """
                SELECT governor_state, COUNT(*) AS count
                FROM resource_samples
                WHERE sampled_at >= ? AND sampled_at <= ?
                GROUP BY governor_state
                ORDER BY governor_state
                """,
                (float(start_time), float(end_time)),
            )
        }
        samples = int(row["samples"] or 0)
        return {
            "samples": samples,
            "peak_rss_bytes": (
                None if row["peak_rss_bytes"] is None else int(row["peak_rss_bytes"])
            ),
            "mean_rss_bytes": (
                None if row["mean_rss_bytes"] is None else float(row["mean_rss_bytes"])
            ),
            "min_disk_free_bytes": (
                None
                if row["min_disk_free_bytes"] is None
                else int(row["min_disk_free_bytes"])
            ),
            "max_disk_free_bytes": (
                None
                if row["max_disk_free_bytes"] is None
                else int(row["max_disk_free_bytes"])
            ),
            "governor_state_samples": states,
        }

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
