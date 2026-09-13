"""Durable, bounded-growth candidate research ledger."""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from creeper.authority.normalizer import normalize_official
from creeper.records.candidates import (
    CandidateRecord,
    CandidateSourceScope,
    CandidateStatus,
    classify_candidate_source,
)


@dataclass(frozen=True)
class CandidateLedgerEntry:
    hostname: str
    scope: CandidateSourceScope
    source_id: str
    source_locator: str
    source_year: int | None
    first_seen: float
    last_seen: float
    status: CandidateStatus
    resolution_reason: str
    observation_count: int


@dataclass(frozen=True)
class UnparsedCandidateEntry:
    raw_value: str
    source_id: str
    source_locator: str
    reason: str
    first_seen: float
    last_seen: float
    observation_count: int


class CandidateStore:
    """SQLite authority for candidate research state, never annual evidence.

    The current table is intentionally one row per normalized hostname/scope.
    Repeated source observations update timestamps/counters in place. A small
    transition table preserves audit-visible state changes without retaining
    every raw source record.
    """

    def __init__(self, path: Path, *, clock: Callable[[], float] = time.time):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = Path(path)
        self.clock = clock
        self.connection = sqlite3.connect(path, timeout=30.0)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA busy_timeout=30000")
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS candidate_records (
                hostname TEXT NOT NULL,
                scope TEXT NOT NULL,
                source_id TEXT NOT NULL,
                source_locator TEXT NOT NULL DEFAULT '',
                source_year INTEGER,
                first_seen REAL NOT NULL,
                last_seen REAL NOT NULL,
                status TEXT NOT NULL,
                resolution_reason TEXT NOT NULL DEFAULT '',
                observation_count INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(hostname, scope)
            ) WITHOUT ROWID;

            CREATE INDEX IF NOT EXISTS idx_candidate_records_status_hostname
                ON candidate_records(status, hostname, scope);

            CREATE TABLE IF NOT EXISTS candidate_status_history (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                hostname TEXT NOT NULL,
                scope TEXT NOT NULL,
                status TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                changed_at REAL NOT NULL,
                UNIQUE(hostname, scope, status)
            );

            CREATE TABLE IF NOT EXISTS unparsed_candidates (
                raw_value TEXT NOT NULL,
                source_id TEXT NOT NULL,
                source_locator TEXT NOT NULL DEFAULT '',
                reason TEXT NOT NULL,
                first_seen REAL NOT NULL,
                last_seen REAL NOT NULL,
                observation_count INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(raw_value, source_id, reason)
            ) WITHOUT ROWID;

            CREATE INDEX IF NOT EXISTS idx_unparsed_candidates_raw
                ON unparsed_candidates(raw_value, source_id);
            """
        )
        self.connection.commit()

    @staticmethod
    def _effective_scope(record: CandidateRecord) -> CandidateSourceScope:
        inferred = classify_candidate_source(record.source_id)
        if inferred in {
            CandidateSourceScope.ISC_REFERENCE,
            CandidateSourceScope.COMMON_CRAWL_CORPUS_EXCLUDED,
        }:
            return inferred
        return record.scope

    @staticmethod
    def _initial_status(scope: CandidateSourceScope) -> CandidateStatus:
        if scope is CandidateSourceScope.ISC_REFERENCE:
            return CandidateStatus.ISC_REFERENCE
        if scope is CandidateSourceScope.COMMON_CRAWL_CORPUS_EXCLUDED:
            return CandidateStatus.EXCLUDED_COMMON_CRAWL
        return CandidateStatus.ACTIVE_CANDIDATE

    def record_observation(
        self,
        record: CandidateRecord,
        *,
        observed_at: float | None = None,
        invalid_reason: str = "hostname_normalization_failed",
    ) -> CandidateLedgerEntry | None:
        self.record_observations(
            (record,),
            observed_at=observed_at,
            invalid_reason=invalid_reason,
        )
        hostname = normalize_official(record.hostname)
        if hostname is None:
            return None
        return self.get(hostname, self._effective_scope(record))

    def record_observations(
        self,
        records: Iterable[CandidateRecord],
        *,
        observed_at: float | None = None,
        invalid_reason: str = "hostname_normalization_failed",
    ) -> int:
        now = float(self.clock() if observed_at is None else observed_at)
        normalized_rows: list[tuple[object, ...]] = []
        invalid_rows: list[tuple[object, ...]] = []
        history_rows: list[tuple[object, ...]] = []
        for record in records:
            hostname = normalize_official(record.hostname)
            if hostname is None:
                invalid_rows.append(
                    (
                        str(record.hostname),
                        record.source_id,
                        record.source_locator or "",
                        invalid_reason,
                        now,
                        now,
                    )
                )
                continue
            scope = self._effective_scope(record)
            status = self._initial_status(scope)
            normalized_rows.append(
                (
                    hostname,
                    scope.value,
                    record.source_id,
                    record.source_locator or "",
                    record.source_year,
                    now,
                    now,
                    status.value,
                )
            )
            history_rows.append(
                (hostname, scope.value, status.value, "", now)
            )

        if not normalized_rows and not invalid_rows:
            return 0
        with self.connection:
            if normalized_rows:
                self.connection.executemany(
                    """
                    INSERT INTO candidate_records(
                        hostname, scope, source_id, source_locator, source_year,
                        first_seen, last_seen, status, resolution_reason,
                        observation_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', 1)
                    ON CONFLICT(hostname, scope) DO UPDATE SET
                        last_seen = excluded.last_seen,
                        observation_count = candidate_records.observation_count + 1,
                        source_locator = CASE
                            WHEN candidate_records.source_locator = ''
                            THEN excluded.source_locator
                            ELSE candidate_records.source_locator
                        END,
                        source_year = COALESCE(
                            candidate_records.source_year,
                            excluded.source_year
                        )
                    """,
                    normalized_rows,
                )
                self.connection.executemany(
                    """
                    INSERT OR IGNORE INTO candidate_status_history(
                        hostname, scope, status, reason, changed_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    history_rows,
                )
            if invalid_rows:
                self.connection.executemany(
                    """
                    INSERT INTO unparsed_candidates(
                        raw_value, source_id, source_locator, reason,
                        first_seen, last_seen, observation_count
                    ) VALUES (?, ?, ?, ?, ?, ?, 1)
                    ON CONFLICT(raw_value, source_id, reason) DO UPDATE SET
                        last_seen = excluded.last_seen,
                        observation_count = unparsed_candidates.observation_count + 1,
                        source_locator = CASE
                            WHEN unparsed_candidates.source_locator = ''
                            THEN excluded.source_locator
                            ELSE unparsed_candidates.source_locator
                        END
                    """,
                    invalid_rows,
                )
        return len(normalized_rows) + len(invalid_rows)

    def record_unparsed(
        self,
        raw_value: str,
        *,
        source_id: str,
        source_locator: str = "",
        reason: str,
        observed_at: float | None = None,
    ) -> None:
        if not source_id.strip():
            raise ValueError("unparsed candidate provenance requires source_id")
        if not reason.strip():
            raise ValueError("unparsed candidate requires a reason")
        now = float(self.clock() if observed_at is None else observed_at)
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO unparsed_candidates(
                    raw_value, source_id, source_locator, reason,
                    first_seen, last_seen, observation_count
                ) VALUES (?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(raw_value, source_id, reason) DO UPDATE SET
                    last_seen = excluded.last_seen,
                    observation_count = unparsed_candidates.observation_count + 1,
                    source_locator = CASE
                        WHEN unparsed_candidates.source_locator = ''
                        THEN excluded.source_locator
                        ELSE unparsed_candidates.source_locator
                    END
                """,
                (
                    str(raw_value),
                    source_id,
                    source_locator,
                    reason,
                    now,
                    now,
                ),
            )

    def _transition_many(
        self,
        hostnames: Iterable[str],
        status: CandidateStatus,
        *,
        reason: str,
        chunk_size: int = 500,
    ) -> int:
        if chunk_size < 1:
            raise ValueError("chunk_size must be positive")
        values = sorted({
            normalized
            for raw in hostnames
            if (normalized := normalize_official(raw)) is not None
        })
        if not values:
            return 0
        changed = 0
        now = float(self.clock())
        limit = min(int(chunk_size), 900)
        for start in range(0, len(values), limit):
            chunk = values[start:start + limit]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.connection.execute(
                f"""
                SELECT hostname, scope
                FROM candidate_records
                WHERE hostname IN ({placeholders})
                  AND status = ?
                ORDER BY hostname, scope
                """,
                (*chunk, CandidateStatus.ACTIVE_CANDIDATE.value),
            ).fetchall()
            if not rows:
                continue
            with self.connection:
                self.connection.executemany(
                    """
                    UPDATE candidate_records
                    SET status = ?, resolution_reason = ?, last_seen = ?
                    WHERE hostname = ? AND scope = ?
                      AND status = ?
                    """,
                    [
                        (
                            status.value,
                            reason,
                            now,
                            str(row["hostname"]),
                            str(row["scope"]),
                            CandidateStatus.ACTIVE_CANDIDATE.value,
                        )
                        for row in rows
                    ],
                )
                self.connection.executemany(
                    """
                    INSERT OR IGNORE INTO candidate_status_history(
                        hostname, scope, status, reason, changed_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            str(row["hostname"]),
                            str(row["scope"]),
                            status.value,
                            reason,
                            now,
                        )
                        for row in rows
                    ],
                )
            changed += len(rows)
        return changed

    def mark_annual_evidence_obtained(
        self,
        hostname: str,
        *,
        reason: str = "accepted_annual_evidence",
    ) -> int:
        return self.mark_annual_evidence_obtained_many((hostname,), reason=reason)

    def mark_annual_evidence_obtained_many(
        self,
        hostnames: Iterable[str],
        *,
        reason: str = "accepted_annual_evidence",
    ) -> int:
        return self._transition_many(
            hostnames,
            CandidateStatus.ANNUAL_EVIDENCE_OBTAINED,
            reason=reason,
        )

    def mark_baseline_overlap(
        self,
        hostname: str,
        *,
        reason: str = "official_baseline_overlap",
    ) -> int:
        return self.mark_baseline_overlap_many((hostname,), reason=reason)

    def mark_baseline_overlap_many(
        self,
        hostnames: Iterable[str],
        *,
        reason: str = "official_baseline_overlap",
    ) -> int:
        return self._transition_many(
            hostnames,
            CandidateStatus.BASELINE_OVERLAP,
            reason=reason,
        )

    @staticmethod
    def _entry(row: sqlite3.Row) -> CandidateLedgerEntry:
        return CandidateLedgerEntry(
            hostname=str(row["hostname"]),
            scope=CandidateSourceScope(str(row["scope"])),
            source_id=str(row["source_id"]),
            source_locator=str(row["source_locator"]),
            source_year=(
                None if row["source_year"] is None else int(row["source_year"])
            ),
            first_seen=float(row["first_seen"]),
            last_seen=float(row["last_seen"]),
            status=CandidateStatus(str(row["status"])),
            resolution_reason=str(row["resolution_reason"]),
            observation_count=int(row["observation_count"]),
        )

    def get(
        self,
        hostname: str,
        scope: CandidateSourceScope,
    ) -> CandidateLedgerEntry | None:
        normalized = normalize_official(hostname)
        if normalized is None:
            return None
        row = self.connection.execute(
            """
            SELECT *
            FROM candidate_records
            WHERE hostname = ? AND scope = ?
            """,
            (normalized, scope.value),
        ).fetchone()
        return None if row is None else self._entry(row)

    def iter_entries(
        self,
        *,
        status: CandidateStatus | None = None,
        batch_size: int = 2_000,
    ) -> Iterator[CandidateLedgerEntry]:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        if status is None:
            cursor = self.connection.execute(
                """
                SELECT *
                FROM candidate_records
                ORDER BY hostname, scope
                """
            )
        else:
            cursor = self.connection.execute(
                """
                SELECT *
                FROM candidate_records
                WHERE status = ?
                ORDER BY hostname, scope
                """,
                (status.value,),
            )
        while True:
            rows = cursor.fetchmany(int(batch_size))
            if not rows:
                break
            for row in rows:
                yield self._entry(row)

    def iter_active_candidates(
        self,
        *,
        batch_size: int = 2_000,
    ) -> Iterator[CandidateLedgerEntry]:
        yield from self.iter_entries(
            status=CandidateStatus.ACTIVE_CANDIDATE,
            batch_size=batch_size,
        )

    def iter_isc_reference(
        self,
        *,
        batch_size: int = 2_000,
    ) -> Iterator[CandidateLedgerEntry]:
        yield from self.iter_entries(
            status=CandidateStatus.ISC_REFERENCE,
            batch_size=batch_size,
        )

    def iter_unparsed(
        self,
        *,
        batch_size: int = 2_000,
    ) -> Iterator[UnparsedCandidateEntry]:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        cursor = self.connection.execute(
            """
            SELECT *
            FROM unparsed_candidates
            ORDER BY raw_value, source_id, reason
            """
        )
        while True:
            rows = cursor.fetchmany(int(batch_size))
            if not rows:
                break
            for row in rows:
                yield UnparsedCandidateEntry(
                    raw_value=str(row["raw_value"]),
                    source_id=str(row["source_id"]),
                    source_locator=str(row["source_locator"]),
                    reason=str(row["reason"]),
                    first_seen=float(row["first_seen"]),
                    last_seen=float(row["last_seen"]),
                    observation_count=int(row["observation_count"]),
                )

    def count(self, status: CandidateStatus | None = None) -> int:
        if status is None:
            row = self.connection.execute(
                "SELECT COUNT(*) FROM candidate_records"
            ).fetchone()
        else:
            row = self.connection.execute(
                "SELECT COUNT(*) FROM candidate_records WHERE status = ?",
                (status.value,),
            ).fetchone()
        return int(row[0])

    def unparsed_count(self) -> int:
        row = self.connection.execute(
            "SELECT COUNT(*) FROM unparsed_candidates"
        ).fetchone()
        return int(row[0])

    def history_count(self) -> int:
        return int(
            self.connection.execute(
                "SELECT COUNT(*) FROM candidate_status_history"
            ).fetchone()[0]
        )

    def close(self) -> None:
        self.connection.close()
