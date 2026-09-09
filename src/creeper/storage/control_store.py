"""Durable control-plane state for resumable evidence work."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from creeper.evidence.policies import CDXQueryState, EvidenceQueryKey, TemporalScope


TERMINAL_STATES = frozenset({
    CDXQueryState.PASS.value,
    CDXQueryState.EMPTY_EXHAUSTIVE.value,
    CDXQueryState.INVALID.value,
})
RETRYABLE_STATES = frozenset({
    CDXQueryState.INCOMPLETE.value,
    CDXQueryState.TRANSIENT_ERROR.value,
})
CLAIMABLE_STATES = frozenset({
    CDXQueryState.PENDING.value,
    *RETRYABLE_STATES,
})


@dataclass(frozen=True)
class EvidenceTask:
    key: EvidenceQueryKey
    state: str
    attempt: int
    retry_at: float | None
    lease_owner: str | None
    lease_until: float | None


class ControlStore:
    """SQLite-WAL authority for evidence task identity and ownership."""

    def __init__(
        self,
        path: Path,
        *,
        default_lease_seconds: float = 300.0,
        clock=time.time,
    ):
        if default_lease_seconds < 0:
            raise ValueError("default_lease_seconds must be non-negative")
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.default_lease_seconds = default_lease_seconds
        self.clock = clock
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS evidence_tasks (
                hostname TEXT NOT NULL,
                year_from INTEGER NOT NULL,
                year_to INTEGER NOT NULL,
                provider TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                state TEXT NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 0,
                retry_at REAL,
                lease_owner TEXT,
                lease_until REAL,
                PRIMARY KEY(hostname, year_from, year_to, provider, policy_version)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_evidence_tasks_claim
                ON evidence_tasks(state, retry_at, lease_until);
            CREATE TABLE IF NOT EXISTS runtime_checkpoints (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) WITHOUT ROWID;
            """
        )
        self.connection.commit()

    @staticmethod
    def _values(key: EvidenceQueryKey) -> tuple[object, ...]:
        scope = key.temporal_scope
        return (
            key.hostname,
            scope.year_from,
            scope.year_to,
            key.provider,
            key.policy_version,
        )

    @staticmethod
    def _key(row: sqlite3.Row) -> EvidenceQueryKey:
        return EvidenceQueryKey(
            row["hostname"],
            TemporalScope(row["year_from"], row["year_to"]),
            row["provider"],
            row["policy_version"],
        )

    @classmethod
    def _task(cls, row: sqlite3.Row) -> EvidenceTask:
        return EvidenceTask(
            key=cls._key(row),
            state=str(row["state"]),
            attempt=int(row["attempt"]),
            retry_at=row["retry_at"],
            lease_owner=row["lease_owner"],
            lease_until=row["lease_until"],
        )

    def enqueue_evidence_tasks(self, keys: Iterable[EvidenceQueryKey]) -> int:
        rows = [(*self._values(key), CDXQueryState.PENDING.value) for key in keys]
        if not rows:
            return 0
        before = self.connection.total_changes
        with self.connection:
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO evidence_tasks(
                    hostname, year_from, year_to, provider, policy_version, state
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return self.connection.total_changes - before

    def claim_evidence_tasks(
        self,
        *,
        owner: str,
        limit: int,
        lease_seconds: float | None = None,
        keys: Iterable[EvidenceQueryKey] | None = None,
    ) -> list[EvidenceTask]:
        if not owner:
            raise ValueError("owner is required")
        if limit < 1:
            return []
        if lease_seconds is None:
            lease_seconds = self.default_lease_seconds
        if lease_seconds < 0:
            raise ValueError("lease_seconds must be non-negative")
        now = float(self.clock())
        key_values = list(keys) if keys is not None else None
        clauses = [
            "state IN (?, ?, ?)",
            "(lease_until IS NULL OR lease_until <= ?)",
            "(retry_at IS NULL OR retry_at <= ?)",
        ]
        params: list[object] = [
            CDXQueryState.PENDING.value,
            CDXQueryState.INCOMPLETE.value,
            CDXQueryState.TRANSIENT_ERROR.value,
            now,
            now,
        ]
        if key_values is not None:
            if not key_values:
                return []
            key_clauses = []
            for key in key_values:
                key_clauses.append(
                    "(hostname = ? AND year_from = ? AND year_to = ? "
                    "AND provider = ? AND policy_version = ?)"
                )
                params.extend(self._values(key))
            clauses.append("(" + " OR ".join(key_clauses) + ")")
        params.append(limit)
        query = (
            "SELECT * FROM evidence_tasks WHERE "
            + " AND ".join(clauses)
            + " ORDER BY hostname, year_from, year_to, provider, policy_version LIMIT ?"
        )
        lease_until = now + float(lease_seconds)
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            rows = self.connection.execute(query, params).fetchall()
            for row in rows:
                self.connection.execute(
                    """
                    UPDATE evidence_tasks
                    SET lease_owner = ?, lease_until = ?, attempt = attempt + 1
                    WHERE hostname = ? AND year_from = ? AND year_to = ?
                      AND provider = ? AND policy_version = ?
                    """,
                    (owner, lease_until, *self._values(self._key(row))),
                )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        return [
            EvidenceTask(
                key=self._key(row),
                state=str(row["state"]),
                attempt=int(row["attempt"]) + 1,
                retry_at=row["retry_at"],
                lease_owner=owner,
                lease_until=lease_until,
            )
            for row in rows
        ]

    def finish_evidence_task(
        self,
        key: EvidenceQueryKey,
        state: CDXQueryState | str,
        *,
        owner: str | None = None,
        retry_at: float | None = None,
    ) -> None:
        value = state.value if isinstance(state, CDXQueryState) else str(state)
        if value not in TERMINAL_STATES | RETRYABLE_STATES:
            raise ValueError(f"unsupported evidence task state: {value}")
        where_owner = ""
        params: list[object] = [value, retry_at]
        if owner is not None:
            where_owner = " AND lease_owner = ?"
            params.append(owner)
        params.extend(self._values(key))
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE evidence_tasks
                SET state = ?, retry_at = ?, lease_owner = NULL, lease_until = NULL
                WHERE hostname = ? AND year_from = ? AND year_to = ?
                  AND provider = ? AND policy_version = ?
                """.replace(
                    "WHERE hostname", f"WHERE 1 = 1{where_owner} AND hostname"
                ),
                params,
            )
        if cursor.rowcount != 1:
            raise KeyError("evidence task not found or not owned by caller")

    def get_evidence_task(self, key: EvidenceQueryKey) -> EvidenceTask | None:
        row = self.connection.execute(
            """
            SELECT * FROM evidence_tasks
            WHERE hostname = ? AND year_from = ? AND year_to = ?
              AND provider = ? AND policy_version = ?
            """,
            self._values(key),
        ).fetchone()
        return None if row is None else self._task(row)

    def list_evidence_tasks(self) -> list[EvidenceTask]:
        rows = self.connection.execute(
            "SELECT * FROM evidence_tasks ORDER BY hostname, year_from, provider, policy_version"
        ).fetchall()
        return [self._task(row) for row in rows]

    def set_checkpoint(self, key: str, value: str) -> None:
        with self.connection:
            self.connection.execute(
                "INSERT INTO runtime_checkpoints(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def get_checkpoint(self, key: str) -> str | None:
        row = self.connection.execute(
            "SELECT value FROM runtime_checkpoints WHERE key = ?", (key,)
        ).fetchone()
        return None if row is None else str(row[0])

    def close(self) -> None:
        self.connection.close()
