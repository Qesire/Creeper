"""Durable authority core for Creeper's distributed execution fabric.

The store is intentionally independent from the current ControlStore.  It is a
side-by-side vNext control plane whose invariants can be validated locally
before any cloud worker becomes a correctness dependency.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from pathlib import Path
from uuid import uuid4

from creeper.distributed.models import (
    ProviderPermit,
    ResultBatch,
    TaskClass,
    TaskLease,
    WorkDefinition,
    WorkerDescriptor,
)


class StaleLeaseError(RuntimeError):
    """Raised when a request does not own the current lease generation."""


class BatchConflictError(RuntimeError):
    """Raised when one BatchID is replayed with different contents."""


class WorkerRejectedError(RuntimeError):
    """Raised when an unknown or revoked worker attempts authority actions."""


def _json(value) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


class DistributedAuthorityStore:
    """SQLite-WAL source of truth for distributed work ownership.

    Correctness properties enforced here:

    * UNIQUE(work_key) prevents duplicate logical work admission.
    * lease_generation is monotonically incremented on every claim/reclaim.
    * every mutating worker request is fenced by owner + generation + deadline.
    * UNIQUE(batch_id) makes result batches idempotent.
    * provider permits are issued from one global provider budget, independent
      of worker region.
    """

    def __init__(self, path: Path, *, clock=time.time) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        self.connection = sqlite3.connect(path, timeout=30.0)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA busy_timeout=30000")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS distributed_workers (
                worker_id TEXT PRIMARY KEY,
                runtime_class TEXT NOT NULL,
                region TEXT NOT NULL,
                architecture TEXT NOT NULL,
                memory_bytes INTEGER NOT NULL CHECK(memory_bytes >= 0),
                cpu_count INTEGER NOT NULL CHECK(cpu_count >= 1),
                network_class TEXT NOT NULL,
                capabilities_json TEXT NOT NULL,
                last_heartbeat REAL NOT NULL,
                revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0, 1))
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS distributed_work (
                task_id TEXT PRIMARY KEY,
                work_key TEXT NOT NULL UNIQUE,
                producer TEXT NOT NULL,
                task_class TEXT NOT NULL,
                input_identity TEXT NOT NULL,
                coverage_json TEXT NOT NULL,
                partition_key TEXT NOT NULL,
                algorithm_version TEXT NOT NULL,
                required_capabilities_json TEXT NOT NULL,
                priority REAL NOT NULL DEFAULT 0,
                state TEXT NOT NULL,
                lease_owner TEXT,
                lease_generation INTEGER NOT NULL DEFAULT 0
                    CHECK(lease_generation >= 0),
                lease_deadline REAL,
                attempt INTEGER NOT NULL DEFAULT 0 CHECK(attempt >= 0),
                cursor TEXT,
                last_error TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_distributed_work_claim
                ON distributed_work(state, priority DESC, created_at);

            CREATE TABLE IF NOT EXISTS distributed_result_batches (
                batch_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                generation INTEGER NOT NULL CHECK(generation >= 1),
                sequence_no INTEGER NOT NULL CHECK(sequence_no >= 0),
                payload_json TEXT NOT NULL,
                payload_digest TEXT NOT NULL,
                cursor_after TEXT,
                committed_at REAL NOT NULL,
                UNIQUE(task_id, sequence_no),
                FOREIGN KEY(task_id) REFERENCES distributed_work(task_id)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS distributed_request_nonces (
                worker_id TEXT NOT NULL,
                nonce TEXT NOT NULL,
                seen_at REAL NOT NULL,
                PRIMARY KEY(worker_id, nonce)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_distributed_request_nonces_seen
                ON distributed_request_nonces(seen_at);

            CREATE TABLE IF NOT EXISTS distributed_provider_budgets (
                provider TEXT PRIMARY KEY,
                requests_per_second REAL NOT NULL
                    CHECK(requests_per_second > 0),
                max_global_inflight INTEGER NOT NULL
                    CHECK(max_global_inflight >= 1),
                next_request_at REAL NOT NULL DEFAULT 0,
                cooldown_until REAL NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS distributed_provider_permits (
                permit_id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                generation INTEGER NOT NULL CHECK(generation >= 1),
                allowed_requests INTEGER NOT NULL CHECK(allowed_requests >= 1),
                max_inflight INTEGER NOT NULL CHECK(max_inflight >= 1),
                expires_at REAL NOT NULL,
                active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0, 1)),
                issued_at REAL NOT NULL,
                status_code INTEGER,
                FOREIGN KEY(provider)
                    REFERENCES distributed_provider_budgets(provider),
                FOREIGN KEY(worker_id)
                    REFERENCES distributed_workers(worker_id),
                FOREIGN KEY(task_id)
                    REFERENCES distributed_work(task_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_distributed_provider_permit_active
                ON distributed_provider_permits(provider, active, expires_at);
            """
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def consume_request_nonce(
        self,
        worker_id: str,
        nonce: str,
        *,
        retention_seconds: float = 600.0,
    ) -> bool:
        """Persistently consume a request nonce.

        Returns False for a replay. Nonces are retained longer than the normal
        HMAC timestamp-skew window so an Authority restart cannot reopen the
        replay window.
        """

        if not worker_id.strip() or not nonce.strip() or retention_seconds <= 0:
            raise ValueError("invalid nonce")
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self.connection.execute(
                "DELETE FROM distributed_request_nonces WHERE seen_at < ?",
                (now - float(retention_seconds),),
            )
            cursor = self.connection.execute(
                """
                INSERT OR IGNORE INTO distributed_request_nonces(
                    worker_id, nonce, seen_at
                ) VALUES (?, ?, ?)
                """,
                (worker_id, nonce, now),
            )
            self.connection.commit()
            return cursor.rowcount == 1
        except Exception:
            self.connection.rollback()
            raise

    def register_worker(self, worker: WorkerDescriptor) -> None:
        now = float(self.clock())
        self.connection.execute(
            """
            INSERT INTO distributed_workers(
                worker_id, runtime_class, region, architecture, memory_bytes,
                cpu_count, network_class, capabilities_json, last_heartbeat
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(worker_id) DO UPDATE SET
                runtime_class = excluded.runtime_class,
                region = excluded.region,
                architecture = excluded.architecture,
                memory_bytes = excluded.memory_bytes,
                cpu_count = excluded.cpu_count,
                network_class = excluded.network_class,
                capabilities_json = excluded.capabilities_json,
                last_heartbeat = excluded.last_heartbeat
            """,
            (
                worker.worker_id,
                worker.runtime_class,
                worker.region,
                worker.architecture,
                int(worker.memory_bytes),
                int(worker.cpu_count),
                worker.network_class,
                _json(sorted(worker.capabilities)),
                now,
            ),
        )
        self.connection.commit()

    def heartbeat(self, worker_id: str) -> None:
        now = float(self.clock())
        cursor = self.connection.execute(
            """
            UPDATE distributed_workers
            SET last_heartbeat = ?
            WHERE worker_id = ? AND revoked = 0
            """,
            (now, worker_id),
        )
        if cursor.rowcount != 1:
            self.connection.rollback()
            raise WorkerRejectedError(worker_id)
        self.connection.commit()

    def revoke_worker(self, worker_id: str) -> None:
        self.connection.execute(
            "UPDATE distributed_workers SET revoked = 1 WHERE worker_id = ?",
            (worker_id,),
        )
        self.connection.commit()

    def _worker_capabilities(self, worker_id: str) -> set[str]:
        row = self.connection.execute(
            """
            SELECT capabilities_json, revoked
            FROM distributed_workers
            WHERE worker_id = ?
            """,
            (worker_id,),
        ).fetchone()
        if row is None or int(row["revoked"]):
            raise WorkerRejectedError(worker_id)
        return set(json.loads(str(row["capabilities_json"])))

    @staticmethod
    def _work_from_row(row: sqlite3.Row) -> WorkDefinition:
        return WorkDefinition(
            producer=str(row["producer"]),
            task_class=TaskClass(str(row["task_class"])),
            input_identity=str(row["input_identity"]),
            coverage=json.loads(str(row["coverage_json"])),
            partition=str(row["partition_key"]),
            algorithm_version=str(row["algorithm_version"]),
            required_capabilities=tuple(
                json.loads(str(row["required_capabilities_json"]))
            ),
            priority=float(row["priority"]),
        )

    def admit_work(self, work: WorkDefinition) -> str:
        """Admit logical work exactly once and return its deterministic task id."""

        now = float(self.clock())
        task_id = work.work_key
        self.connection.execute(
            """
            INSERT OR IGNORE INTO distributed_work(
                task_id, work_key, producer, task_class, input_identity,
                coverage_json, partition_key, algorithm_version,
                required_capabilities_json, priority, state, created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'READY', ?, ?)
            """,
            (
                task_id,
                work.work_key,
                work.producer,
                work.task_class.value,
                work.input_identity,
                _json(dict(work.coverage)),
                work.partition,
                work.algorithm_version,
                _json(list(work.required_capabilities)),
                float(work.priority),
                now,
                now,
            ),
        )
        row = self.connection.execute(
            "SELECT * FROM distributed_work WHERE work_key = ?",
            (work.work_key,),
        ).fetchone()
        assert row is not None
        existing = self._work_from_row(row)
        if (
            existing.producer != work.producer
            or existing.task_class != work.task_class
            or existing.input_identity != work.input_identity
            or dict(existing.coverage) != dict(work.coverage)
            or existing.partition != work.partition
            or existing.algorithm_version != work.algorithm_version
            or tuple(existing.required_capabilities)
            != tuple(work.required_capabilities)
        ):
            self.connection.rollback()
            raise ValueError("existing work_key has incompatible work definition")
        if float(work.priority) > float(row["priority"]):
            self.connection.execute(
                """
                UPDATE distributed_work
                SET priority = ?, updated_at = ?
                WHERE work_key = ?
                """,
                (float(work.priority), now, work.work_key),
            )
        self.connection.commit()
        return task_id

    def claim_work(
        self,
        worker_id: str,
        *,
        lease_seconds: float = 300.0,
    ) -> TaskLease | None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        now = float(self.clock())
        capabilities = self._worker_capabilities(worker_id)
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            rows = self.connection.execute(
                """
                SELECT *
                FROM distributed_work
                WHERE state = 'READY'
                   OR (
                        state = 'LEASED'
                        AND lease_deadline IS NOT NULL
                        AND lease_deadline <= ?
                   )
                ORDER BY priority DESC, created_at, work_key
                LIMIT 256
                """,
                (now,),
            ).fetchall()
            chosen = None
            for row in rows:
                required = set(
                    json.loads(str(row["required_capabilities_json"]))
                )
                if required.issubset(capabilities):
                    chosen = row
                    break
            if chosen is None:
                self.connection.commit()
                return None

            generation = int(chosen["lease_generation"]) + 1
            attempt = int(chosen["attempt"]) + 1
            deadline = now + float(lease_seconds)
            self.connection.execute(
                """
                UPDATE distributed_work
                SET state = 'LEASED',
                    lease_owner = ?,
                    lease_generation = ?,
                    lease_deadline = ?,
                    attempt = ?,
                    updated_at = ?
                WHERE task_id = ?
                """,
                (
                    worker_id,
                    generation,
                    deadline,
                    attempt,
                    now,
                    str(chosen["task_id"]),
                ),
            )
            self.connection.commit()
            return TaskLease(
                task_id=str(chosen["task_id"]),
                work_key=str(chosen["work_key"]),
                worker_id=worker_id,
                generation=generation,
                lease_deadline=deadline,
                attempt=attempt,
                work=self._work_from_row(chosen),
                cursor=(
                    None
                    if chosen["cursor"] is None
                    else str(chosen["cursor"])
                ),
            )
        except Exception:
            self.connection.rollback()
            raise

    def _assert_active_lease(
        self,
        task_id: str,
        worker_id: str,
        generation: int,
        *,
        now: float,
    ) -> sqlite3.Row:
        worker = self.connection.execute(
            """
            SELECT revoked FROM distributed_workers
            WHERE worker_id = ?
            """,
            (worker_id,),
        ).fetchone()
        if worker is None or int(worker["revoked"]):
            raise WorkerRejectedError(worker_id)

        row = self.connection.execute(
            "SELECT * FROM distributed_work WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        if (
            row is None
            or str(row["state"]) != "LEASED"
            or str(row["lease_owner"]) != worker_id
            or int(row["lease_generation"]) != int(generation)
            or row["lease_deadline"] is None
            or float(row["lease_deadline"]) <= now
        ):
            raise StaleLeaseError(
                f"stale lease task={task_id} worker={worker_id} "
                f"generation={generation}"
            )
        return row

    def renew_task(
        self,
        task_id: str,
        *,
        worker_id: str,
        generation: int,
        lease_seconds: float = 300.0,
    ) -> TaskLease:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self._assert_active_lease(
                task_id,
                worker_id,
                generation,
                now=now,
            )
            deadline = now + float(lease_seconds)
            self.connection.execute(
                """
                UPDATE distributed_work
                SET lease_deadline = ?, updated_at = ?
                WHERE task_id = ?
                """,
                (deadline, now, task_id),
            )
            self.connection.commit()
            return TaskLease(
                task_id=task_id,
                work_key=str(row["work_key"]),
                worker_id=worker_id,
                generation=int(generation),
                lease_deadline=deadline,
                attempt=int(row["attempt"]),
                work=self._work_from_row(row),
                cursor=None if row["cursor"] is None else str(row["cursor"]),
            )
        except Exception:
            self.connection.rollback()
            raise

    @staticmethod
    def _batch_payload(batch: ResultBatch) -> str:
        return _json(
            {
                # Generation is a fencing credential, not part of BatchID or
                # logical batch content. A new lease generation must therefore
                # receive ALREADY_COMMITTED when it replays an identical
                # task/sequence batch after an ACK was lost.
                "task_id": batch.task_id,
                "sequence_no": int(batch.sequence_no),
                "results": [dict(item) for item in batch.results],
                "cursor_after": batch.cursor_after,
            }
        )

    def commit_result_batch(
        self,
        batch: ResultBatch,
        *,
        worker_id: str,
    ) -> bool:
        """Commit a batch once.

        Returns True for the first logical commit and False for an exact replay.
        A replay with the same BatchID but different contents is rejected.
        """

        now = float(self.clock())
        payload = self._batch_payload(batch)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self.connection.execute(
                """
                SELECT payload_json, payload_digest
                FROM distributed_result_batches
                WHERE batch_id = ?
                """,
                (batch.batch_id,),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["payload_digest"]) != digest
                    or str(existing["payload_json"]) != payload
                ):
                    raise BatchConflictError(batch.batch_id)
                self.connection.commit()
                return False

            self._assert_active_lease(
                batch.task_id,
                worker_id,
                batch.generation,
                now=now,
            )
            self.connection.execute(
                """
                INSERT INTO distributed_result_batches(
                    batch_id, task_id, generation, sequence_no, payload_json,
                    payload_digest, cursor_after, committed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch.batch_id,
                    batch.task_id,
                    int(batch.generation),
                    int(batch.sequence_no),
                    payload,
                    digest,
                    batch.cursor_after,
                    now,
                ),
            )
            self.connection.execute(
                """
                UPDATE distributed_work
                SET cursor = COALESCE(?, cursor), updated_at = ?
                WHERE task_id = ?
                """,
                (batch.cursor_after, now, batch.task_id),
            )
            self.connection.commit()
            return True
        except Exception:
            self.connection.rollback()
            raise

    def finish_task(
        self,
        task_id: str,
        *,
        worker_id: str,
        generation: int,
    ) -> None:
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self._assert_active_lease(
                task_id,
                worker_id,
                generation,
                now=now,
            )
            self.connection.execute(
                """
                UPDATE distributed_work
                SET state = 'COMPLETE',
                    lease_owner = NULL,
                    lease_deadline = NULL,
                    updated_at = ?
                WHERE task_id = ?
                """,
                (now, task_id),
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def fail_task(
        self,
        task_id: str,
        *,
        worker_id: str,
        generation: int,
        error: str,
        retryable: bool = True,
    ) -> None:
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self._assert_active_lease(
                task_id,
                worker_id,
                generation,
                now=now,
            )
            self.connection.execute(
                """
                UPDATE distributed_work
                SET state = ?,
                    lease_owner = NULL,
                    lease_deadline = NULL,
                    last_error = ?,
                    updated_at = ?
                WHERE task_id = ?
                """,
                ("READY" if retryable else "FAILED", error, now, task_id),
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def configure_provider_budget(
        self,
        provider: str,
        *,
        requests_per_second: float,
        max_global_inflight: int,
    ) -> None:
        if (
            not provider.strip()
            or requests_per_second <= 0
            or max_global_inflight < 1
        ):
            raise ValueError("invalid provider budget")
        now = float(self.clock())
        self.connection.execute(
            """
            INSERT INTO distributed_provider_budgets(
                provider, requests_per_second, max_global_inflight,
                next_request_at, cooldown_until, updated_at
            ) VALUES (?, ?, ?, 0, 0, ?)
            ON CONFLICT(provider) DO UPDATE SET
                requests_per_second = excluded.requests_per_second,
                max_global_inflight = excluded.max_global_inflight,
                updated_at = excluded.updated_at
            """,
            (
                provider.strip(),
                float(requests_per_second),
                int(max_global_inflight),
                now,
            ),
        )
        self.connection.commit()

    def issue_provider_permit(
        self,
        provider: str,
        *,
        worker_id: str,
        task_id: str,
        generation: int,
        ttl_seconds: float = 30.0,
    ) -> ProviderPermit | None:
        """Issue one globally paced external-request permit.

        The first implementation intentionally grants one request per permit.
        This makes global rate/inflight correctness explicit before introducing
        larger batched permits or adaptive scheduling.
        """

        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self._assert_active_lease(
                task_id,
                worker_id,
                generation,
                now=now,
            )
            budget = self.connection.execute(
                """
                SELECT * FROM distributed_provider_budgets
                WHERE provider = ?
                """,
                (provider,),
            ).fetchone()
            if budget is None:
                raise KeyError(f"provider budget not configured: {provider}")

            self.connection.execute(
                """
                UPDATE distributed_provider_permits
                SET active = 0
                WHERE provider = ? AND active = 1 AND expires_at <= ?
                """,
                (provider, now),
            )
            if now < float(budget["cooldown_until"]):
                self.connection.commit()
                return None
            if now < float(budget["next_request_at"]):
                self.connection.commit()
                return None

            active = int(
                self.connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM distributed_provider_permits
                    WHERE provider = ? AND active = 1 AND expires_at > ?
                    """,
                    (provider, now),
                ).fetchone()[0]
            )
            max_inflight = int(budget["max_global_inflight"])
            if active >= max_inflight:
                self.connection.commit()
                return None

            permit_id = str(uuid4())
            expires_at = now + float(ttl_seconds)
            self.connection.execute(
                """
                INSERT INTO distributed_provider_permits(
                    permit_id, provider, worker_id, task_id, generation,
                    allowed_requests, max_inflight, expires_at, active, issued_at
                ) VALUES (?, ?, ?, ?, ?, 1, ?, ?, 1, ?)
                """,
                (
                    permit_id,
                    provider,
                    worker_id,
                    task_id,
                    int(generation),
                    max_inflight,
                    expires_at,
                    now,
                ),
            )
            next_request_at = max(
                now,
                float(budget["next_request_at"]),
            ) + 1.0 / float(budget["requests_per_second"])
            self.connection.execute(
                """
                UPDATE distributed_provider_budgets
                SET next_request_at = ?, updated_at = ?
                WHERE provider = ?
                """,
                (next_request_at, now, provider),
            )
            self.connection.commit()
            return ProviderPermit(
                permit_id=permit_id,
                provider=provider,
                worker_id=worker_id,
                task_id=task_id,
                generation=int(generation),
                allowed_requests=1,
                max_inflight=max_inflight,
                expires_at=expires_at,
            )
        except Exception:
            self.connection.rollback()
            raise

    def report_provider_permit(
        self,
        permit_id: str,
        *,
        worker_id: str,
        status_code: int | None = None,
        cooldown_seconds: float = 0.0,
    ) -> None:
        if cooldown_seconds < 0:
            raise ValueError("cooldown_seconds must be non-negative")
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                """
                SELECT * FROM distributed_provider_permits
                WHERE permit_id = ?
                """,
                (permit_id,),
            ).fetchone()
            if row is None or str(row["worker_id"]) != worker_id:
                raise WorkerRejectedError(worker_id)
            self.connection.execute(
                """
                UPDATE distributed_provider_permits
                SET active = 0, status_code = ?
                WHERE permit_id = ?
                """,
                (status_code, permit_id),
            )
            if status_code in {429, 503}:
                delay = float(cooldown_seconds)
                budget = self.connection.execute(
                    """
                    SELECT cooldown_until
                    FROM distributed_provider_budgets
                    WHERE provider = ?
                    """,
                    (str(row["provider"]),),
                ).fetchone()
                assert budget is not None
                self.connection.execute(
                    """
                    UPDATE distributed_provider_budgets
                    SET cooldown_until = ?, updated_at = ?
                    WHERE provider = ?
                    """,
                    (
                        max(float(budget["cooldown_until"]), now + delay),
                        now,
                        str(row["provider"]),
                    ),
                )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def provider_budget_snapshot(self, provider: str) -> dict[str, float | int]:
        now = float(self.clock())
        row = self.connection.execute(
            """
            SELECT * FROM distributed_provider_budgets
            WHERE provider = ?
            """,
            (provider,),
        ).fetchone()
        if row is None:
            raise KeyError(provider)
        active = int(
            self.connection.execute(
                """
                SELECT COUNT(*)
                FROM distributed_provider_permits
                WHERE provider = ? AND active = 1 AND expires_at > ?
                """,
                (provider, now),
            ).fetchone()[0]
        )
        return {
            "requests_per_second": float(row["requests_per_second"]),
            "max_global_inflight": int(row["max_global_inflight"]),
            "active_inflight": active,
            "next_request_at": float(row["next_request_at"]),
            "cooldown_until": float(row["cooldown_until"]),
        }

    def task_row(self, task_id: str) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM distributed_work WHERE task_id = ?",
            (task_id,),
        ).fetchone()

    def batch_count(self, task_id: str) -> int:
        return int(
            self.connection.execute(
                """
                SELECT COUNT(*) FROM distributed_result_batches
                WHERE task_id = ?
                """,
                (task_id,),
            ).fetchone()[0]
        )
