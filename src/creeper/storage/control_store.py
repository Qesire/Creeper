"""Durable control-plane state for resumable evidence work."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from creeper.authority.baseline_index import YEAR_BITS
from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceQueryKey,
    EvidenceQueryResult,
    TemporalScope,
)


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
            CREATE INDEX IF NOT EXISTS idx_evidence_tasks_provider_claim
                ON evidence_tasks(provider, state, retry_at, lease_until);
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
            CREATE TABLE IF NOT EXISTS source_activations (
                source_key TEXT PRIMARY KEY,
                domain_id TEXT NOT NULL,
                reservoir_id TEXT NOT NULL UNIQUE,
                adapter_id TEXT NOT NULL,
                adapter_kind TEXT NOT NULL,
                root_locator TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                activation_state TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                FOREIGN KEY(domain_id) REFERENCES source_domains(domain_id),
                FOREIGN KEY(reservoir_id) REFERENCES reservoirs(reservoir_id)
            ) WITHOUT ROWID;
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

    def resolve_provider_coverage_masks(
        self,
        hostnames: Iterable[str],
        *,
        provider: str,
        policy_version: str,
        chunk_size: int = 800,
    ) -> dict[str, int]:
        """Return years already exhaustively covered by one evidence provider.

        PASS and EMPTY_EXHAUSTIVE tasks both imply that the provider completed
        the full temporal scope. PASS years with accepted captures are already
        represented in EvidenceStore; the remaining years in that completed
        scope are reusable negative knowledge. INVALID/retryable tasks do not
        establish coverage.
        """
        if not provider or not policy_version:
            raise ValueError("provider and policy_version are required")
        if chunk_size < 1:
            raise ValueError("chunk_size must be positive")
        values = list(dict.fromkeys(str(item) for item in hostnames if str(item)))
        result = {hostname: 0 for hostname in values}
        if not values:
            return result
        limit = min(int(chunk_size), 800)
        for start in range(0, len(values), limit):
            chunk = values[start:start + limit]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.connection.execute(
                f"""
                SELECT hostname, year_from, year_to
                FROM evidence_tasks
                WHERE hostname IN ({placeholders})
                  AND provider = ?
                  AND policy_version = ?
                  AND state IN (?, ?)
                """,
                [
                    *chunk,
                    provider,
                    policy_version,
                    CDXQueryState.PASS.value,
                    CDXQueryState.EMPTY_EXHAUSTIVE.value,
                ],
            ).fetchall()
            for row in rows:
                mask = result.get(str(row["hostname"]), 0)
                for year in range(int(row["year_from"]), int(row["year_to"]) + 1):
                    mask |= YEAR_BITS.get(year, 0)
                result[str(row["hostname"])] = mask
        return result

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

    def finish_evidence_tasks(
        self,
        results: Iterable[EvidenceQueryResult],
        *,
        owner: str,
    ) -> int:
        """Finish a batch of claimed evidence tasks atomically."""
        if not owner:
            raise ValueError("owner is required")
        return self._finish_evidence_results(results, owner=owner, retry_at=None)

    def finish_range_task(
        self,
        key: EvidenceQueryKey,
        state: CDXQueryState | str,
        *,
        followup_keys: Iterable[EvidenceQueryKey] = (),
        owner: str,
    ) -> int:
        """Finish a terminal range task and enqueue exact-year follow-ups atomically."""
        if not owner:
            raise ValueError("owner is required")
        if key.temporal_scope.year_from == key.temporal_scope.year_to:
            raise ValueError("range task must span multiple years")
        value = state.value if isinstance(state, CDXQueryState) else str(state)
        if value not in TERMINAL_STATES:
            raise ValueError("range task can only finish in a terminal state")
        exact = list(followup_keys)
        for followup in exact:
            scope = followup.temporal_scope
            if scope.year_from != scope.year_to:
                raise ValueError("range follow-up tasks must be exact-year keys")
            if (
                followup.hostname != key.hostname
                or followup.provider != key.provider
                or followup.policy_version != key.policy_version
                or not key.temporal_scope.year_from <= scope.year_from <= key.temporal_scope.year_to
            ):
                raise ValueError("range follow-up key does not match parent range")

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                """
                SELECT state FROM evidence_tasks
                WHERE hostname = ? AND year_from = ? AND year_to = ?
                  AND provider = ? AND policy_version = ? AND lease_owner = ?
                """,
                (*self._values(key), owner),
            ).fetchone()
            if row is None:
                raise KeyError("range task not found or not owned by caller")
            self.connection.execute(
                """
                UPDATE evidence_tasks
                SET state = ?, retry_at = NULL, lease_owner = NULL, lease_until = NULL
                WHERE hostname = ? AND year_from = ? AND year_to = ?
                  AND provider = ? AND policy_version = ? AND lease_owner = ?
                """,
                (value, *self._values(key), owner),
            )
            before = self.connection.total_changes
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO evidence_tasks(
                    hostname, year_from, year_to, provider, policy_version, state
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [(*self._values(followup), CDXQueryState.PENDING.value) for followup in exact],
            )
            created = self.connection.total_changes - before
            self.connection.commit()
            return created
        except BaseException:
            self.connection.rollback()
            raise

    def _finish_evidence_results(
        self,
        results: Iterable[EvidenceQueryResult],
        *,
        owner: str | None,
        retry_at: float | None,
    ) -> int:
        keyed_results = [result for result in results if result.key is not None]
        if not keyed_results:
            return 0
        updates: list[tuple[str, None, object, ...]] = []
        seen: set[EvidenceQueryKey] = set()
        for result in keyed_results:
            key = result.key
            assert key is not None
            if key in seen:
                raise ValueError(f"duplicate evidence task key: {key}")
            seen.add(key)
            value = result.state.value if isinstance(result.state, CDXQueryState) else str(result.state)
            if value not in TERMINAL_STATES | RETRYABLE_STATES:
                raise ValueError(f"unsupported evidence task state: {value}")
            updates.append((value, retry_at, *self._values(key)))

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            for result in keyed_results:
                key = result.key
                assert key is not None
                ownership = " AND lease_owner = ?" if owner is not None else ""
                params = (*self._values(key), owner) if owner is not None else self._values(key)
                row = self.connection.execute(
                    """
                    SELECT 1 FROM evidence_tasks
                    WHERE hostname = ? AND year_from = ? AND year_to = ?
                      AND provider = ? AND policy_version = ?
                    """ + ownership,
                    params,
                ).fetchone()
                if row is None:
                    raise KeyError("evidence task not found or not owned by caller")
            ownership = " AND lease_owner = ?" if owner is not None else ""
            self.connection.executemany(
                """
                UPDATE evidence_tasks
                SET state = ?, retry_at = ?, lease_owner = NULL, lease_until = NULL
                WHERE hostname = ? AND year_from = ? AND year_to = ?
                  AND provider = ? AND policy_version = ?
                """ + ownership,
                [(*update, owner) if owner is not None else update for update in updates],
            )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        return len(updates)

    def finish_evidence_task(
        self,
        key: EvidenceQueryKey,
        state: CDXQueryState | str,
        *,
        owner: str | None = None,
        retry_at: float | None = None,
    ) -> None:
        result = EvidenceQueryResult(
            hostname=key.hostname,
            year=key.temporal_scope.year_from,
            state=state if isinstance(state, CDXQueryState) else CDXQueryState(str(state)),
            key=key,
        )
        self._finish_evidence_results([result], owner=owner, retry_at=retry_at)

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

    def evidence_task_state_counts(self) -> dict[str, int]:
        """Aggregate the durable evidence backlog without materializing tasks."""
        counts = {state.value: 0 for state in CDXQueryState}
        for row in self.connection.execute(
            "SELECT state, COUNT(*) AS count FROM evidence_tasks GROUP BY state"
        ):
            counts[str(row["state"])] = int(row["count"])
        return counts

    def reservoir_state_counts(self) -> dict[str, int]:
        return {
            str(row["state"]): int(row["count"])
            for row in self.connection.execute(
                "SELECT state, COUNT(*) AS count FROM reservoirs GROUP BY state"
            )
        }

    def work_lease_state_counts(self) -> dict[str, int]:
        return {
            str(row["state"]): int(row["count"])
            for row in self.connection.execute(
                "SELECT state, COUNT(*) AS count FROM work_leases GROUP BY state"
            )
        }

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

    def save_activation(
        self,
        *,
        source_key: str,
        domain: Any,
        reservoir: Any,
        adapter_kind: str,
        config_hash: str,
        activation_state: str = "ACTIVE",
    ) -> None:
        """Atomically persist discovery-to-production activation lineage."""
        if not source_key.strip() or not adapter_kind.strip() or not config_hash.strip():
            raise ValueError("activation identity fields are required")
        temporal = self._field(domain, "temporal_scope")
        if temporal is None or len(temporal) != 2:
            raise ValueError("domain temporal_scope must be a (from, to) pair")
        now = float(self.clock())
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO source_domains(
                    domain_id, family, discovery_mechanism,
                    temporal_from, temporal_to, state
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(domain_id) DO NOTHING
                """,
                (
                    self._field(domain, "domain_id"),
                    self._value(self._field(domain, "family"), ""),
                    self._field(domain, "discovery_mechanism"),
                    temporal[0], temporal[1],
                    self._value(self._field(domain, "state"), "UNEXPLORED"),
                ),
            )
            self.connection.execute(
                """
                INSERT INTO reservoirs(
                    reservoir_id, domain_id, adapter_id, root_locator,
                    enumeration_kind, capacity_lower, capacity_upper, cursor,
                    evidence_mode, state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(reservoir_id) DO NOTHING
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
                    self._value(self._field(reservoir, "state"), "DISCOVERED"),
                ),
            )
            self.connection.execute(
                """
                INSERT INTO source_activations(
                    source_key, domain_id, reservoir_id, adapter_id, adapter_kind,
                    root_locator, config_hash, activation_state, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_key) DO UPDATE SET
                    updated_at=excluded.updated_at,
                    activation_state=excluded.activation_state
                """,
                (
                    source_key,
                    self._field(domain, "domain_id"),
                    self._field(reservoir, "reservoir_id"),
                    self._field(reservoir, "adapter_id"),
                    adapter_kind,
                    self._field(reservoir, "root_locator"),
                    config_hash,
                    activation_state,
                    now,
                    now,
                ),
            )

    def get_activation(self, source_key: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM source_activations WHERE source_key = ?", (source_key,)
        ).fetchone()
        return None if row is None else {key: row[key] for key in row.keys()}

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

    def grant_fresh_lease(
        self,
        reservoir_id: str,
        *,
        owner: str,
        max_records: int,
        max_requests: int,
        max_bytes: int,
        max_seconds: float,
        resource_class: str,
        expected_evidence_tasks: int,
        expected_novel_eed: float,
        now: float,
    ) -> Any | None:
        """Atomically claim a READY reservoir with a new cursor-backed lease."""
        from creeper.scheduler.leases import LeaseState, WorkLease
        from creeper.sources.reservoirs import ReservoirState

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            reservoir = self.connection.execute(
                "SELECT * FROM reservoirs WHERE reservoir_id = ?",
                (reservoir_id,),
            ).fetchone()
            if reservoir is None or reservoir["state"] != ReservoirState.READY.value:
                self.connection.commit()
                return None

            lease = WorkLease.create(
                reservoir_id=reservoir_id,
                cursor_start=reservoir["cursor"],
                max_records=max_records,
                max_requests=max_requests,
                max_bytes=max_bytes,
                max_seconds=max_seconds,
                resource_class=resource_class,
                expected_evidence_tasks=expected_evidence_tasks,
                expected_novel_eed=expected_novel_eed,
                now=float(now),
            ).grant(owner=owner)
            self.connection.execute(
                """
                INSERT INTO work_leases(
                    lease_id, reservoir_id, cursor_start, cursor_end, max_records,
                    max_requests, max_bytes, max_seconds, resource_class,
                    expected_evidence_tasks, expected_novel_eed, owner,
                    expires_at, state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lease.lease_id,
                    lease.reservoir_id,
                    lease.cursor_start,
                    lease.cursor_end,
                    lease.max_records,
                    lease.max_requests,
                    lease.max_bytes,
                    lease.max_seconds,
                    lease.resource_class,
                    lease.expected_evidence_tasks,
                    lease.expected_novel_eed,
                    lease.owner,
                    lease.expires_at,
                    LeaseState.GRANTED.value,
                ),
            )
            changed = self.connection.execute(
                """
                UPDATE reservoirs SET state = ?
                WHERE reservoir_id = ? AND state = ?
                """,
                (
                    ReservoirState.LEASED.value,
                    reservoir_id,
                    ReservoirState.READY.value,
                ),
            ).rowcount
            if changed != 1:
                raise RuntimeError("reservoir changed while granting lease")
            self.connection.commit()
            return lease
        except BaseException:
            self.connection.rollback()
            raise

    def finalize_lease(
        self,
        lease: Any,
        *,
        next_cursor: str | None,
        exhausted: bool,
    ) -> None:
        """Atomically finish a running lease and advance its reservoir.

        The lease row and reservoir row form one progress unit.  A successful
        non-EOF execution returns the reservoir to READY at ``next_cursor``;
        an EOF execution makes it EXHAUSTED.  Both updates are guarded by the
        persisted lease owner/state so a stale worker cannot advance a source.
        """
        from creeper.scheduler.leases import LeaseState
        from creeper.sources.reservoirs import ReservoirState

        lease_id = self._field(lease, "lease_id")
        reservoir_id = self._field(lease, "reservoir_id")
        owner = self._field(lease, "owner")
        if not lease_id or not reservoir_id or not owner:
            raise ValueError("a running lease with an owner is required")
        if not exhausted and next_cursor is None:
            raise ValueError("a non-exhausted lease must provide next_cursor")

        target = ReservoirState.EXHAUSTED if exhausted else ReservoirState.READY
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            lease_row = self.connection.execute(
                "SELECT state, owner, reservoir_id FROM work_leases WHERE lease_id = ?",
                (lease_id,),
            ).fetchone()
            if (
                lease_row is None
                or lease_row["state"] != LeaseState.RUNNING.value
                or lease_row["owner"] != owner
                or lease_row["reservoir_id"] != reservoir_id
            ):
                raise ValueError("lease is not running or is not owned by caller")
            lease_changed = self.connection.execute(
                "UPDATE work_leases SET state = ? WHERE lease_id = ? AND state = ? AND owner = ?",
                (LeaseState.SUCCEEDED.value, lease_id, LeaseState.RUNNING.value, owner),
            ).rowcount
            if lease_changed != 1:
                raise RuntimeError("lease changed while finalizing")
            reservoir_changed = self.connection.execute(
                """
                UPDATE reservoirs SET state = ?, cursor = ?
                WHERE reservoir_id = ? AND state IN (?, ?)
                """,
                (
                    target.value,
                    next_cursor,
                    reservoir_id,
                    ReservoirState.LEASED.value,
                    ReservoirState.RUNNING.value,
                ),
            ).rowcount
            if reservoir_changed != 1:
                raise ValueError("reservoir is not owned by running lease")
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    def abort_lease(self, lease: Any) -> None:
        """Abort a lease and restore its reservoir to the original cursor."""
        from creeper.scheduler.leases import LeaseState
        from creeper.sources.reservoirs import ReservoirState

        lease_id = self._field(lease, "lease_id")
        reservoir_id = self._field(lease, "reservoir_id")
        owner = self._field(lease, "owner")
        if not lease_id or not reservoir_id or not owner:
            raise ValueError("an owned lease is required")

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            lease_row = self.connection.execute(
                "SELECT state, owner, reservoir_id, cursor_start FROM work_leases "
                "WHERE lease_id = ?",
                (lease_id,),
            ).fetchone()
            if lease_row is None:
                raise KeyError(f"unknown lease: {lease_id}")
            if lease_row["owner"] != owner or lease_row["reservoir_id"] != reservoir_id:
                raise ValueError("lease is not owned by caller")
            if lease_row["state"] not in {
                LeaseState.GRANTED.value,
                LeaseState.RUNNING.value,
                LeaseState.PAUSED.value,
                LeaseState.PREEMPTED.value,
            }:
                raise ValueError("lease is not active")
            self.connection.execute(
                "UPDATE work_leases SET state = ? WHERE lease_id = ?",
                (LeaseState.ABORTED.value, lease_id),
            )
            changed = self.connection.execute(
                """
                UPDATE reservoirs SET state = ?, cursor = ?
                WHERE reservoir_id = ? AND state IN (?, ?, ?, ?)
                """,
                (
                    ReservoirState.READY.value,
                    lease_row["cursor_start"],
                    reservoir_id,
                    ReservoirState.LEASED.value,
                    ReservoirState.RUNNING.value,
                    ReservoirState.PAUSED.value,
                    ReservoirState.PREEMPTED.value,
                ),
            ).rowcount
            if changed != 1:
                raise ValueError("reservoir is not owned by active lease")
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

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
        from creeper.sources.reservoirs import ReservoirState

        if now is None:
            now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            rows = self.connection.execute(
                """
                SELECT lease_id, reservoir_id, cursor_start
                FROM work_leases
                WHERE expires_at <= ? AND state IN (?, ?)
                """,
                (
                    float(now),
                    LeaseState.GRANTED.value,
                    LeaseState.RUNNING.value,
                ),
            ).fetchall()
            for row in rows:
                self.connection.execute(
                    "UPDATE work_leases SET state = ? WHERE lease_id = ?",
                    (LeaseState.EXPIRED.value, row["lease_id"]),
                )
                self.connection.execute(
                    """
                    UPDATE reservoirs
                    SET state = ?, cursor = ?
                    WHERE reservoir_id = ? AND state IN (?, ?, ?, ?)
                    """,
                    (
                        ReservoirState.READY.value,
                        row["cursor_start"],
                        row["reservoir_id"],
                        ReservoirState.LEASED.value,
                        ReservoirState.RUNNING.value,
                        ReservoirState.PAUSED.value,
                        ReservoirState.PREEMPTED.value,
                    ),
                )
            self.connection.commit()
            return len(rows)
        except BaseException:
            self.connection.rollback()
            raise

    def close(self) -> None:
        self.connection.close()
