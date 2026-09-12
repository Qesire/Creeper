"""Crash-safe producer admission against a durable evidence backlog.

This module implements the same capacity-reservation pattern used by mature
queueing systems: producers reserve queue capacity before starting expensive
source work, then atomically convert reserved slots into durable tasks.

The evidence task table remains the authority for actual work. Reservations
exist only to prevent concurrent source producers from oversubscribing the
backlog during the interval between SourceLease grant and EvidenceTask enqueue.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable
from uuid import uuid4

from creeper.evidence.policies import CDXQueryState, EvidenceQueryKey
from creeper.storage.control_store import ControlStore


_NONTERMINAL = (
    CDXQueryState.PENDING.value,
    CDXQueryState.INCOMPLETE.value,
    CDXQueryState.TRANSIENT_ERROR.value,
)


@dataclass(frozen=True)
class CapacityReservation:
    reservation_id: str | None
    provider: str
    amount: int
    expires_at: float


class EvidenceBacklogAdmission:
    """Serialize producer capacity reservations through SQLite ``BEGIN IMMEDIATE``.

    A reservation is deliberately short-lived and bounded by its source lease.
    Process death can therefore waste capacity until expiry, but cannot cause
    over-admission. That is the preferable failure mode for a competition
    pipeline whose evidence queue must remain bounded.
    """

    def __init__(self, control_store: ControlStore):
        self.control_store = control_store
        self.connection = control_store.connection
        with self.connection:
            self.connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS evidence_capacity_reservations (
                    reservation_id TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    amount INTEGER NOT NULL CHECK(amount > 0),
                    expires_at REAL NOT NULL,
                    lease_id TEXT
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_evidence_capacity_reservations_provider
                    ON evidence_capacity_reservations(provider, expires_at);
                CREATE INDEX IF NOT EXISTS idx_evidence_capacity_reservations_lease
                    ON evidence_capacity_reservations(lease_id);
                """
            )

    def _now(self) -> float:
        return float(self.control_store.clock())

    def _purge_expired_locked(self, now: float) -> None:
        self.connection.execute(
            "DELETE FROM evidence_capacity_reservations WHERE expires_at <= ?",
            (now,),
        )

    def _durable_backlog_locked(self, provider: str) -> int:
        row = self.connection.execute(
            """
            SELECT COUNT(*)
            FROM evidence_tasks
            WHERE provider = ? AND state IN (?, ?, ?)
            """,
            (provider, *_NONTERMINAL),
        ).fetchone()
        return int(row[0])

    def _reserved_locked(self, provider: str, now: float) -> int:
        row = self.connection.execute(
            """
            SELECT COALESCE(SUM(amount), 0)
            FROM evidence_capacity_reservations
            WHERE provider = ? AND expires_at > ?
            """,
            (provider, now),
        ).fetchone()
        return int(row[0] or 0)

    def available_capacity(
        self,
        *,
        provider: str,
        capacity: int,
    ) -> int:
        """Return currently unoccupied durable backlog capacity.

        This is a scheduling hint only. The later `try_reserve` transaction
        remains the authority, so concurrent producers cannot over-admit even
        if this value becomes stale immediately after it is read.
        """
        if not provider:
            raise ValueError("provider is required")
        if not isinstance(capacity, int) or capacity < 0:
            raise ValueError("capacity must be a non-negative integer")
        now = self._now()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self._purge_expired_locked(now)
            occupied = (
                self._durable_backlog_locked(provider)
                + self._reserved_locked(provider, now)
            )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        return max(0, capacity - occupied)

    def try_reserve(
        self,
        *,
        provider: str,
        amount: int,
        capacity: int,
        ttl_seconds: float,
    ) -> CapacityReservation | None:
        if not provider:
            raise ValueError("provider is required")
        if not isinstance(amount, int) or amount < 0:
            raise ValueError("amount must be a non-negative integer")
        if not isinstance(capacity, int) or capacity < 0:
            raise ValueError("capacity must be a non-negative integer")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")

        now = self._now()
        expires_at = now + float(ttl_seconds)
        if amount == 0:
            return CapacityReservation(None, provider, 0, expires_at)

        reservation_id = f"evidence-capacity-{uuid4().hex}"
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self._purge_expired_locked(now)
            occupied = (
                self._durable_backlog_locked(provider)
                + self._reserved_locked(provider, now)
            )
            if occupied + amount > capacity:
                self.connection.commit()
                return None
            self.connection.execute(
                """
                INSERT INTO evidence_capacity_reservations(
                    reservation_id, provider, amount, expires_at
                ) VALUES (?, ?, ?, ?)
                """,
                (reservation_id, provider, amount, expires_at),
            )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        return CapacityReservation(reservation_id, provider, amount, expires_at)

    def bind_lease(self, reservation: CapacityReservation, lease_id: str) -> None:
        if reservation.reservation_id is None:
            return
        if not lease_id:
            raise ValueError("lease_id is required")
        with self.connection:
            changed = self.connection.execute(
                """
                UPDATE evidence_capacity_reservations
                SET lease_id = ?
                WHERE reservation_id = ?
                """,
                (lease_id, reservation.reservation_id),
            ).rowcount
        if changed != 1:
            raise KeyError("capacity reservation not found")

    def enqueue_reserved(
        self,
        reservation: CapacityReservation,
        keys: Iterable[EvidenceQueryKey],
        *,
        source_key: str | None = None,
        reservoir_id: str | None = None,
        lease_id: str | None = None,
    ) -> int:
        """Atomically transfer reserved capacity into durable task rows.

        Optional source lineage is operational metadata only. It is inserted in
        the same transaction but never changes reservation consumption or the
        returned count of newly created EvidenceTask rows.
        """
        rows = list(keys)
        if not rows:
            return 0
        if reservation.reservation_id is None:
            raise RuntimeError("external evidence work exceeded zero reservation")
        if any(key.provider != reservation.provider for key in rows):
            raise ValueError("all reserved evidence keys must use the reserved provider")
        lineage = (source_key, reservoir_id, lease_id)
        if any(value is not None for value in lineage):
            if not all(
                isinstance(value, str) and value.strip()
                for value in lineage
            ):
                raise ValueError(
                    "source_key, reservoir_id, and lease_id must be provided together"
                )

        now = self._now()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                """
                SELECT amount, expires_at
                FROM evidence_capacity_reservations
                WHERE reservation_id = ? AND provider = ?
                """,
                (reservation.reservation_id, reservation.provider),
            ).fetchone()
            if row is None or float(row["expires_at"]) <= now:
                raise RuntimeError("capacity reservation expired before task enqueue")
            remaining = int(row["amount"])
            before = self.connection.total_changes
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO evidence_tasks(
                    hostname, year_from, year_to, provider, policy_version, state
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        key.hostname,
                        key.temporal_scope.year_from,
                        key.temporal_scope.year_to,
                        key.provider,
                        key.policy_version,
                        CDXQueryState.PENDING.value,
                    )
                    for key in rows
                ],
            )
            inserted = self.connection.total_changes - before
            if source_key is not None:
                assert reservoir_id is not None and lease_id is not None
                self.connection.executemany(
                    """
                    INSERT OR IGNORE INTO evidence_task_origins(
                        hostname, year_from, year_to, provider, policy_version,
                        source_key, reservoir_id, lease_id, first_observed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            key.hostname,
                            key.temporal_scope.year_from,
                            key.temporal_scope.year_to,
                            key.provider,
                            key.policy_version,
                            source_key,
                            reservoir_id,
                            lease_id,
                            now,
                        )
                        for key in rows
                    ],
                )
            if inserted > remaining:
                raise RuntimeError(
                    "actual evidence work exceeded the SourceLease capacity reservation"
                )
            remaining -= inserted
            if remaining:
                self.connection.execute(
                    """
                    UPDATE evidence_capacity_reservations
                    SET amount = ?
                    WHERE reservation_id = ?
                    """,
                    (remaining, reservation.reservation_id),
                )
            else:
                self.connection.execute(
                    "DELETE FROM evidence_capacity_reservations WHERE reservation_id = ?",
                    (reservation.reservation_id,),
                )
            self.connection.commit()
            return inserted
        except BaseException:
            self.connection.rollback()
            raise

    def release(self, reservation: CapacityReservation | None) -> None:
        if reservation is None or reservation.reservation_id is None:
            return
        with self.connection:
            self.connection.execute(
                "DELETE FROM evidence_capacity_reservations WHERE reservation_id = ?",
                (reservation.reservation_id,),
            )

    def reserved(self, provider: str, *, now: float | None = None) -> int:
        current = self._now() if now is None else float(now)
        row = self.connection.execute(
            """
            SELECT COALESCE(SUM(amount), 0)
            FROM evidence_capacity_reservations
            WHERE provider = ? AND expires_at > ?
            """,
            (provider, current),
        ).fetchone()
        return int(row[0] or 0)
