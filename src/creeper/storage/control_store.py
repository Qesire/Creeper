"""Durable control-plane state for resumable evidence work."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

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
            CREATE TABLE IF NOT EXISTS source_domains (
                domain_id TEXT PRIMARY KEY,
                family TEXT NOT NULL,
                discovery_mechanism TEXT NOT NULL,
                temporal_from INTEGER NOT NULL,
                temporal_to INTEGER NOT NULL,
                state TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS reservoirs (
                reservoir_id TEXT PRIMARY KEY,
                domain_id TEXT NOT NULL,
                adapter_id TEXT NOT NULL,
                root_locator TEXT NOT NULL,
                enumeration_kind TEXT NOT NULL,
                capacity_lower INTEGER NOT NULL,
                capacity_upper INTEGER,
                cursor TEXT,
                evidence_mode TEXT NOT NULL,
                state TEXT NOT NULL,
                FOREIGN KEY(domain_id) REFERENCES source_domains(domain_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_reservoirs_domain
                ON reservoirs(domain_id, state);
            CREATE TABLE IF NOT EXISTS work_leases (
                lease_id TEXT PRIMARY KEY,
                reservoir_id TEXT NOT NULL,
                cursor_start TEXT,
                cursor_end TEXT,
                max_records INTEGER NOT NULL,
                max_requests INTEGER NOT NULL,
                max_bytes INTEGER NOT NULL,
                max_seconds REAL NOT NULL,
                resource_class TEXT NOT NULL,
                expected_evidence_tasks INTEGER NOT NULL DEFAULT 0,
                expected_novel_eed REAL NOT NULL DEFAULT 0,
                owner TEXT,
                expires_at REAL NOT NULL,
                state TEXT NOT NULL,
                FOREIGN KEY(reservoir_id) REFERENCES reservoirs(reservoir_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_work_leases_recovery
                ON work_leases(state, expires_at);
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

    @staticmethod
    def _value(value: Any, default: Any = None) -> Any:
        if value is None:
            return default
        return getattr(value, "value", value)

    @staticmethod
    def _field(obj: Any, name: str, default: Any = None) -> Any:
        return getattr(obj, name, default)

    @staticmethod
    def _runtime_types() -> tuple[Any, Any, Any, Any]:
        # Imported lazily so the evidence-only ControlStore remains importable
        # while the source/lease model slice is developed independently.
        from creeper.scheduler.leases import LeaseState
        from creeper.sources.domains import SourceDomain
        from creeper.sources.reservoirs import Reservoir, ReservoirState

        return SourceDomain, Reservoir, ReservoirState, LeaseState

    def save_domain(self, domain: Any) -> None:
        temporal = self._field(domain, "temporal_scope")
        if temporal is None or len(temporal) != 2:
            raise ValueError("domain temporal_scope must be a (from, to) pair")
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO source_domains(
                    domain_id, family, discovery_mechanism,
                    temporal_from, temporal_to, state
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(domain_id) DO UPDATE SET
                    family=excluded.family,
                    discovery_mechanism=excluded.discovery_mechanism,
                    temporal_from=excluded.temporal_from,
                    temporal_to=excluded.temporal_to,
                    state=excluded.state
                """,
                (
                    self._field(domain, "domain_id"),
                    self._value(self._field(domain, "family"), ""),
                    self._field(domain, "discovery_mechanism"),
                    temporal[0], temporal[1],
                    self._value(self._field(domain, "state"), "unexplored"),
                ),
            )

    def get_domain(self, domain_id: str) -> Any | None:
        row = self.connection.execute(
            "SELECT * FROM source_domains WHERE domain_id = ?", (domain_id,)
        ).fetchone()
        if row is None:
            return None
        SourceDomain, _, _, _ = self._runtime_types()
        from creeper.sources.domains import DomainState
        return SourceDomain(
            domain_id=row["domain_id"],
            family=row["family"],
            discovery_mechanism=row["discovery_mechanism"],
            temporal_scope=(row["temporal_from"], row["temporal_to"]),
            state=DomainState(row["state"]),
        )

    def save_reservoir(self, reservoir: Any) -> None:
        if self.get_domain(self._field(reservoir, "domain_id")) is None:
            raise KeyError(f"domain not found: {self._field(reservoir, 'domain_id')}")
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO reservoirs(
                    reservoir_id, domain_id, adapter_id, root_locator,
                    enumeration_kind, capacity_lower, capacity_upper, cursor,
                    evidence_mode, state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(reservoir_id) DO UPDATE SET
                    domain_id=excluded.domain_id, adapter_id=excluded.adapter_id,
                    root_locator=excluded.root_locator,
                    enumeration_kind=excluded.enumeration_kind,
                    capacity_lower=excluded.capacity_lower,
                    capacity_upper=excluded.capacity_upper,
                    cursor=excluded.cursor, evidence_mode=excluded.evidence_mode,
                    state=excluded.state
                """,
                (
                    self._field(reservoir, "reservoir_id"),
                    self._field(reservoir, "domain_id"),
                    self._field(reservoir, "adapter_id"),
                    self._field(reservoir, "root_locator"),
                    self._value(self._field(reservoir, "enumeration_kind"), ""),
                    self._field(reservoir, "capacity_lower"),
                    self._field(reservoir, "capacity_upper"),
                    self._field(reservoir, "cursor"),
                    self._value(self._field(reservoir, "evidence_mode"), "discovery_only"),
                    self._value(self._field(reservoir, "state"), "discovered"),
                ),
            )

    def get_reservoir(self, reservoir_id: str) -> Any | None:
        row = self.connection.execute(
            "SELECT * FROM reservoirs WHERE reservoir_id = ?", (reservoir_id,)
        ).fetchone()
        if row is None:
            return None
        _, Reservoir, ReservoirState, _ = self._runtime_types()
        return Reservoir(
            reservoir_id=row["reservoir_id"], domain_id=row["domain_id"],
            adapter_id=row["adapter_id"], root_locator=row["root_locator"],
            enumeration_kind=row["enumeration_kind"],
            capacity_lower=row["capacity_lower"], capacity_upper=row["capacity_upper"],
            cursor=row["cursor"], evidence_mode=row["evidence_mode"],
            state=ReservoirState(row["state"]),
        )

    def save_lease(self, lease: Any) -> None:
        from creeper.scheduler.leases import LeaseState

        state = self._value(self._field(lease, "state"), "created")
        if state == LeaseState.RUNNING.value:
            row = self.connection.execute(
                "SELECT state FROM work_leases WHERE lease_id = ?",
                (self._field(lease, "lease_id"),),
            ).fetchone()
            if row is None or row["state"] != LeaseState.GRANTED.value:
                raise ValueError("a lease must be persisted as GRANTED before RUNNING")
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO work_leases(
                    lease_id, reservoir_id, cursor_start, cursor_end, max_records,
                    max_requests, max_bytes, max_seconds, resource_class,
                    expected_evidence_tasks, expected_novel_eed, owner,
                    expires_at, state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(lease_id) DO UPDATE SET
                    reservoir_id=excluded.reservoir_id, cursor_start=excluded.cursor_start,
                    cursor_end=excluded.cursor_end, max_records=excluded.max_records,
                    max_requests=excluded.max_requests, max_bytes=excluded.max_bytes,
                    max_seconds=excluded.max_seconds, resource_class=excluded.resource_class,
                    expected_evidence_tasks=excluded.expected_evidence_tasks,
                    expected_novel_eed=excluded.expected_novel_eed, owner=excluded.owner,
                    expires_at=excluded.expires_at, state=excluded.state
                """,
                (
                    self._field(lease, "lease_id"), self._field(lease, "reservoir_id"),
                    self._field(lease, "cursor_start"), self._field(lease, "cursor_end"),
                    self._field(lease, "max_records"), self._field(lease, "max_requests"),
                    self._field(lease, "max_bytes"), self._field(lease, "max_seconds"),
                    self._value(self._field(lease, "resource_class"), "general"),
                    self._field(lease, "expected_evidence_tasks", 0),
                    self._field(lease, "expected_novel_eed", 0.0),
                    self._field(lease, "owner"), self._field(lease, "expires_at"), state,
                ),
            )

    def get_lease(self, lease_id: str) -> Any | None:
        row = self.connection.execute(
            "SELECT * FROM work_leases WHERE lease_id = ?", (lease_id,)
        ).fetchone()
        if row is None:
            return None
        _, _, _, LeaseState = self._runtime_types()
        from creeper.scheduler.leases import WorkLease
        return WorkLease(
            lease_id=row["lease_id"], reservoir_id=row["reservoir_id"],
            cursor_start=row["cursor_start"], cursor_end=row["cursor_end"],
            max_records=row["max_records"], max_requests=row["max_requests"],
            max_bytes=row["max_bytes"], max_seconds=row["max_seconds"],
            resource_class=row["resource_class"],
            expected_evidence_tasks=row["expected_evidence_tasks"],
            expected_novel_eed=row["expected_novel_eed"], owner=row["owner"],
            expires_at=row["expires_at"], state=LeaseState(row["state"]),
        )

    def recover_expired_leases(self, *, now: float | None = None) -> int:
        from creeper.scheduler.leases import LeaseState

        if now is None:
            now = float(self.clock())
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE work_leases SET state = ?
                WHERE expires_at < ? AND state IN (?, ?)
                """,
                (
                    LeaseState.EXPIRED.value,
                    float(now),
                    LeaseState.GRANTED.value,
                    LeaseState.RUNNING.value,
                ),
            )
        return cursor.rowcount

    def close(self) -> None:
        self.connection.close()
