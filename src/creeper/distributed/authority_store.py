"""Durable authority store for Creeper Fabric v2.

SQLite is the reference/single-authority backend. Production multi-authority
deployments use the same protocol with PostgreSQL; correctness never depends on
an in-memory queue or transport delivery.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from pathlib import Path
from uuid import uuid4

from creeper.distributed.edition import FABRIC_PROTOCOL_VERSION
from creeper.distributed.identity import canonical_json
from creeper.distributed.models import (
    ArtifactRef,
    ProviderPermit,
    ResultBatch,
    TaskClass,
    TaskLease,
    WorkDefinition,
    WorkerDescriptor,
)


class StaleLeaseError(RuntimeError):
    pass


class BatchConflictError(RuntimeError):
    pass


class BatchSequenceError(RuntimeError):
    pass


class WorkerRejectedError(RuntimeError):
    pass


class FabricProtocolMismatchError(RuntimeError):
    pass


class ProviderAccessDeniedError(RuntimeError):
    pass


class WorkerEgressBudgetExceededError(RuntimeError):
    pass


def _json(value) -> str:
    return canonical_json(value)


class DistributedAuthorityStore:
    """SQLite-WAL reference authority with fencing and transactional outbox."""

    def __init__(self, path: Path, *, clock=time.time) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        self.connection = sqlite3.connect(self.path, timeout=30.0)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA busy_timeout=30000")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self._initialize()

    def _initialize(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS fabric_workers (
                worker_id TEXT PRIMARY KEY,
                worker_instance_id TEXT NOT NULL,
                runtime_class TEXT NOT NULL,
                region TEXT NOT NULL,
                architecture TEXT NOT NULL,
                memory_bytes INTEGER NOT NULL CHECK(memory_bytes >= 0),
                cpu_count INTEGER NOT NULL CHECK(cpu_count >= 1),
                network_class TEXT NOT NULL,
                capabilities_json TEXT NOT NULL,
                producers_json TEXT NOT NULL,
                allowed_providers_json TEXT NOT NULL,
                daily_egress_budget_bytes INTEGER NOT NULL DEFAULT 0
                    CHECK(daily_egress_budget_bytes >= 0),
                protocol_version TEXT NOT NULL,
                edition_version TEXT NOT NULL,
                last_heartbeat REAL NOT NULL,
                revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0,1))
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS fabric_work (
                task_id TEXT PRIMARY KEY,
                work_key TEXT NOT NULL UNIQUE,
                producer TEXT NOT NULL,
                task_class TEXT NOT NULL,
                input_identity TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                partition_key TEXT NOT NULL,
                algorithm_version TEXT NOT NULL,
                required_capabilities_json TEXT NOT NULL,
                required_providers_json TEXT NOT NULL,
                priority REAL NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL CHECK(max_attempts >= 1),
                not_before REAL NOT NULL DEFAULT 0,
                state TEXT NOT NULL,
                lease_owner TEXT,
                lease_owner_instance TEXT,
                lease_generation INTEGER NOT NULL DEFAULT 0,
                lease_deadline REAL,
                attempt INTEGER NOT NULL DEFAULT 0,
                cursor TEXT,
                next_sequence_no INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_fabric_work_claim
                ON fabric_work(state, not_before, priority DESC, created_at);

            CREATE TABLE IF NOT EXISTS fabric_result_batches (
                batch_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                sequence_no INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                payload_digest TEXT NOT NULL,
                cursor_after TEXT,
                final INTEGER NOT NULL DEFAULT 0 CHECK(final IN (0,1)),
                committed_at REAL NOT NULL,
                consumed_at REAL,
                consume_attempts INTEGER NOT NULL DEFAULT 0,
                consume_error TEXT,
                quarantined INTEGER NOT NULL DEFAULT 0 CHECK(quarantined IN (0,1)),
                UNIQUE(task_id, sequence_no),
                FOREIGN KEY(task_id) REFERENCES fabric_work(task_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_fabric_result_unconsumed
                ON fabric_result_batches(consumed_at, committed_at);

            CREATE TABLE IF NOT EXISTS fabric_artifacts (
                artifact_id TEXT PRIMARY KEY,
                sha256 TEXT NOT NULL UNIQUE,
                uri TEXT NOT NULL,
                size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
                content_type TEXT NOT NULL,
                compression TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                first_task_id TEXT NOT NULL,
                first_seen_at REAL NOT NULL,
                FOREIGN KEY(first_task_id) REFERENCES fabric_work(task_id)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS fabric_batch_artifacts (
                batch_id TEXT NOT NULL,
                artifact_id TEXT NOT NULL,
                PRIMARY KEY(batch_id, artifact_id),
                FOREIGN KEY(batch_id) REFERENCES fabric_result_batches(batch_id),
                FOREIGN KEY(artifact_id) REFERENCES fabric_artifacts(artifact_id)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS fabric_outbox (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                published_at REAL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_fabric_outbox_pending
                ON fabric_outbox(published_at, created_at);

            CREATE TABLE IF NOT EXISTS fabric_request_nonces (
                worker_id TEXT NOT NULL,
                nonce TEXT NOT NULL,
                seen_at REAL NOT NULL,
                PRIMARY KEY(worker_id, nonce)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_fabric_nonce_seen
                ON fabric_request_nonces(seen_at);

            CREATE TABLE IF NOT EXISTS fabric_provider_budgets (
                provider TEXT PRIMARY KEY,
                requests_per_second REAL NOT NULL CHECK(requests_per_second > 0),
                max_global_inflight INTEGER NOT NULL CHECK(max_global_inflight >= 1),
                require_qualified_region INTEGER NOT NULL DEFAULT 1
                    CHECK(require_qualified_region IN (0,1)),
                next_request_at REAL NOT NULL DEFAULT 0,
                cooldown_until REAL NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS fabric_provider_regions (
                provider TEXT NOT NULL,
                region TEXT NOT NULL,
                state TEXT NOT NULL,
                samples INTEGER NOT NULL DEFAULT 0,
                successes INTEGER NOT NULL DEFAULT 0,
                timeouts INTEGER NOT NULL DEFAULT 0,
                throttles INTEGER NOT NULL DEFAULT 0,
                policy_blocks INTEGER NOT NULL DEFAULT 0,
                total_latency_ms REAL NOT NULL DEFAULT 0,
                response_bytes INTEGER NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL,
                PRIMARY KEY(provider, region)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS fabric_provider_permits (
                permit_id TEXT PRIMARY KEY,
                request_id TEXT NOT NULL,
                provider TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                worker_instance_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                allowed_requests INTEGER NOT NULL CHECK(allowed_requests >= 1),
                max_inflight INTEGER NOT NULL CHECK(max_inflight >= 1),
                expires_at REAL NOT NULL,
                active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
                issued_at REAL NOT NULL,
                status_code INTEGER,
                UNIQUE(worker_id, request_id),
                FOREIGN KEY(provider) REFERENCES fabric_provider_budgets(provider),
                FOREIGN KEY(worker_id) REFERENCES fabric_workers(worker_id),
                FOREIGN KEY(task_id) REFERENCES fabric_work(task_id)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS idx_fabric_permit_active
                ON fabric_provider_permits(provider, active, expires_at);

            CREATE TABLE IF NOT EXISTS fabric_worker_egress_daily (
                worker_id TEXT NOT NULL,
                day_key INTEGER NOT NULL,
                response_bytes INTEGER NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL,
                PRIMARY KEY(worker_id, day_key),
                FOREIGN KEY(worker_id) REFERENCES fabric_workers(worker_id)
            ) WITHOUT ROWID;
            """
        )

    def close(self) -> None:
        self.connection.close()

    def _emit_locked(self, event_type: str, aggregate_id: str, payload: dict) -> None:
        now = float(self.clock())
        self.connection.execute(
            """
            INSERT INTO fabric_outbox(
                event_id, event_type, aggregate_id, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (uuid4().hex, event_type, aggregate_id, _json(payload), now),
        )

    def consume_request_nonce(
        self,
        worker_id: str,
        nonce: str,
        *,
        retention_seconds: float = 900.0,
    ) -> bool:
        if retention_seconds <= 0:
            raise ValueError("retention_seconds must be positive")
        now = float(self.clock())
        cutoff = now - float(retention_seconds)
        with self.connection:
            self.connection.execute(
                "DELETE FROM fabric_request_nonces WHERE seen_at < ?",
                (cutoff,),
            )
            try:
                self.connection.execute(
                    """
                    INSERT INTO fabric_request_nonces(worker_id, nonce, seen_at)
                    VALUES (?, ?, ?)
                    """,
                    (worker_id, nonce, now),
                )
            except sqlite3.IntegrityError:
                return False
        return True

    def register_worker(self, descriptor: WorkerDescriptor) -> None:
        if descriptor.protocol_version != FABRIC_PROTOCOL_VERSION:
            raise FabricProtocolMismatchError(
                f"worker protocol {descriptor.protocol_version!r} != "
                f"{FABRIC_PROTOCOL_VERSION!r}"
            )
        now = float(self.clock())
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO fabric_workers(
                    worker_id, worker_instance_id, runtime_class, region,
                    architecture, memory_bytes, cpu_count, network_class,
                    capabilities_json, producers_json, allowed_providers_json,
                    daily_egress_budget_bytes, protocol_version, edition_version,
                    last_heartbeat, revoked
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(worker_id) DO UPDATE SET
                    worker_instance_id=excluded.worker_instance_id,
                    runtime_class=excluded.runtime_class,
                    region=excluded.region,
                    architecture=excluded.architecture,
                    memory_bytes=excluded.memory_bytes,
                    cpu_count=excluded.cpu_count,
                    network_class=excluded.network_class,
                    capabilities_json=excluded.capabilities_json,
                    producers_json=excluded.producers_json,
                    allowed_providers_json=excluded.allowed_providers_json,
                    daily_egress_budget_bytes=excluded.daily_egress_budget_bytes,
                    protocol_version=excluded.protocol_version,
                    edition_version=excluded.edition_version,
                    last_heartbeat=excluded.last_heartbeat,
                    revoked=0
                """,
                (
                    descriptor.worker_id,
                    descriptor.worker_instance_id,
                    descriptor.runtime_class,
                    descriptor.region,
                    descriptor.architecture,
                    descriptor.memory_bytes,
                    descriptor.cpu_count,
                    descriptor.network_class,
                    _json(descriptor.capabilities),
                    _json(descriptor.producers),
                    _json(descriptor.allowed_providers),
                    descriptor.daily_egress_budget_bytes,
                    descriptor.protocol_version,
                    descriptor.edition_version,
                    now,
                ),
            )
            # A new worker process incarnation cannot inherit leases from an old
            # incarnation with the same stable worker_id.
            self.connection.execute(
                """
                UPDATE fabric_work
                SET state='PENDING', lease_owner=NULL, lease_owner_instance=NULL,
                    lease_deadline=NULL, updated_at=?
                WHERE state='LEASED' AND lease_owner=?
                  AND lease_owner_instance<>?
                """,
                (now, descriptor.worker_id, descriptor.worker_instance_id),
            )
            self._emit_locked(
                "WORKER_REGISTERED",
                descriptor.worker_id,
                {
                    "worker_id": descriptor.worker_id,
                    "worker_instance_id": descriptor.worker_instance_id,
                    "region": descriptor.region,
                },
            )

    def _worker_row(self, worker_id: str, worker_instance_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM fabric_workers WHERE worker_id=?",
            (worker_id,),
        ).fetchone()
        if (
            row is None
            or int(row["revoked"])
            or str(row["worker_instance_id"]) != worker_instance_id
        ):
            raise WorkerRejectedError("unknown, revoked, or stale worker instance")
        return row

    def heartbeat(self, worker_id: str, worker_instance_id: str) -> None:
        self._worker_row(worker_id, worker_instance_id)
        with self.connection:
            self.connection.execute(
                "UPDATE fabric_workers SET last_heartbeat=? WHERE worker_id=?",
                (float(self.clock()), worker_id),
            )

    def revoke_worker(self, worker_id: str) -> None:
        now = float(self.clock())
        with self.connection:
            changed = self.connection.execute(
                "UPDATE fabric_workers SET revoked=1, last_heartbeat=? WHERE worker_id=?",
                (now, worker_id),
            ).rowcount
            if changed != 1:
                raise KeyError(worker_id)
            self.connection.execute(
                """
                UPDATE fabric_work
                SET state='PENDING', lease_owner=NULL, lease_owner_instance=NULL,
                    lease_deadline=NULL, updated_at=?
                WHERE state='LEASED' AND lease_owner=?
                """,
                (now, worker_id),
            )
            self._emit_locked("WORKER_REVOKED", worker_id, {"worker_id": worker_id})

    def admit_work(self, work: WorkDefinition) -> tuple[str, bool]:
        now = float(self.clock())
        task_id = uuid4().hex
        with self.connection:
            try:
                self.connection.execute(
                    """
                    INSERT INTO fabric_work(
                        task_id, work_key, producer, task_class, input_identity,
                        payload_json, partition_key, algorithm_version,
                        required_capabilities_json, required_providers_json,
                        priority, max_attempts, not_before, state,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)
                    """,
                    (
                        task_id,
                        work.work_key,
                        work.producer,
                        work.task_class.value,
                        work.input_identity,
                        _json(work.payload),
                        work.partition,
                        work.algorithm_version,
                        _json(work.required_capabilities),
                        _json(work.required_providers),
                        work.priority,
                        work.max_attempts,
                        work.not_before,
                        now,
                        now,
                    ),
                )
                self._emit_locked(
                    "WORK_ADMITTED",
                    task_id,
                    {"task_id": task_id, "work_key": work.work_key},
                )
                return task_id, True
            except sqlite3.IntegrityError:
                row = self.connection.execute(
                    "SELECT task_id FROM fabric_work WHERE work_key=?",
                    (work.work_key,),
                ).fetchone()
                if row is None:
                    raise
                return str(row["task_id"]), False

    @staticmethod
    def _json_tuple(raw: str) -> tuple[str, ...]:
        value = json.loads(raw)
        return tuple(str(item) for item in value)

    def _work_from_row(self, row: sqlite3.Row) -> WorkDefinition:
        return WorkDefinition(
            producer=str(row["producer"]),
            task_class=TaskClass(str(row["task_class"])),
            input_identity=str(row["input_identity"]),
            payload=dict(json.loads(str(row["payload_json"]))),
            partition=str(row["partition_key"]),
            algorithm_version=str(row["algorithm_version"]),
            required_capabilities=self._json_tuple(
                str(row["required_capabilities_json"])
            ),
            required_providers=self._json_tuple(
                str(row["required_providers_json"])
            ),
            priority=float(row["priority"]),
            max_attempts=int(row["max_attempts"]),
            not_before=float(row["not_before"]),
        )

    @staticmethod
    def _eligible(row: sqlite3.Row, worker: sqlite3.Row) -> bool:
        required_caps = set(json.loads(str(row["required_capabilities_json"])))
        worker_caps = set(json.loads(str(worker["capabilities_json"])))
        if not required_caps.issubset(worker_caps):
            return False
        required_providers = set(json.loads(str(row["required_providers_json"])))
        worker_providers = set(json.loads(str(worker["allowed_providers_json"])))
        if not required_providers.issubset(worker_providers):
            return False
        producers = set(json.loads(str(worker["producers_json"])))
        if producers and str(row["producer"]) not in producers:
            return False
        return True

    def claim_work(
        self,
        worker_id: str,
        worker_instance_id: str,
        *,
        lease_seconds: float = 300.0,
    ) -> TaskLease | None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            worker = self._worker_row(worker_id, worker_instance_id)
            rows = self.connection.execute(
                """
                SELECT *
                FROM fabric_work
                WHERE (
                        state IN ('PENDING','RETRY')
                        OR (state='LEASED' AND lease_deadline <= ?)
                      )
                  AND not_before <= ?
                  AND attempt < max_attempts
                ORDER BY priority DESC, created_at ASC
                LIMIT 128
                """,
                (now, now),
            ).fetchall()
            chosen = next((row for row in rows if self._eligible(row, worker)), None)
            if chosen is None:
                self.connection.rollback()
                return None
            task_id = str(chosen["task_id"])
            generation = int(chosen["lease_generation"]) + 1
            attempt = int(chosen["attempt"]) + 1
            deadline = now + float(lease_seconds)
            changed = self.connection.execute(
                """
                UPDATE fabric_work
                SET state='LEASED', lease_owner=?, lease_owner_instance=?,
                    lease_generation=?, lease_deadline=?, attempt=?,
                    updated_at=?
                WHERE task_id=? AND lease_generation=?
                """,
                (
                    worker_id,
                    worker_instance_id,
                    generation,
                    deadline,
                    attempt,
                    now,
                    task_id,
                    int(chosen["lease_generation"]),
                ),
            ).rowcount
            if changed != 1:
                self.connection.rollback()
                return None
            self._emit_locked(
                "WORK_LEASED",
                task_id,
                {
                    "task_id": task_id,
                    "worker_id": worker_id,
                    "worker_instance_id": worker_instance_id,
                    "generation": generation,
                },
            )
            self.connection.commit()
            return TaskLease(
                task_id=task_id,
                work_key=str(chosen["work_key"]),
                worker_id=worker_id,
                worker_instance_id=worker_instance_id,
                generation=generation,
                lease_deadline=deadline,
                attempt=attempt,
                work=self._work_from_row(chosen),
                cursor=None if chosen["cursor"] is None else str(chosen["cursor"]),
                next_sequence_no=int(chosen["next_sequence_no"]),
            )
        except BaseException:
            if self.connection.in_transaction:
                self.connection.rollback()
            raise

    def _assert_active_lease(
        self,
        task_id: str,
        *,
        worker_id: str,
        worker_instance_id: str,
        generation: int,
    ) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM fabric_work WHERE task_id=?",
            (task_id,),
        ).fetchone()
        now = float(self.clock())
        if (
            row is None
            or str(row["state"]) != "LEASED"
            or str(row["lease_owner"]) != worker_id
            or str(row["lease_owner_instance"]) != worker_instance_id
            or int(row["lease_generation"]) != generation
            or row["lease_deadline"] is None
            or float(row["lease_deadline"]) <= now
        ):
            raise StaleLeaseError("worker no longer owns the active lease generation")
        return row

    def renew_task(
        self,
        task_id: str,
        *,
        worker_id: str,
        worker_instance_id: str,
        generation: int,
        lease_seconds: float,
    ) -> TaskLease:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        with self.connection:
            row = self._assert_active_lease(
                task_id,
                worker_id=worker_id,
                worker_instance_id=worker_instance_id,
                generation=generation,
            )
            deadline = float(self.clock()) + float(lease_seconds)
            self.connection.execute(
                """
                UPDATE fabric_work
                SET lease_deadline=?, updated_at=?
                WHERE task_id=?
                """,
                (deadline, float(self.clock()), task_id),
            )
        return TaskLease(
            task_id=task_id,
            work_key=str(row["work_key"]),
            worker_id=worker_id,
            worker_instance_id=worker_instance_id,
            generation=generation,
            lease_deadline=deadline,
            attempt=int(row["attempt"]),
            work=self._work_from_row(row),
            cursor=None if row["cursor"] is None else str(row["cursor"]),
            next_sequence_no=int(row["next_sequence_no"]),
        )

    def _batch_payload(self, batch: ResultBatch) -> str:
        return _json(
            {
                "task_id": batch.task_id,
                "generation": batch.generation,
                "sequence_no": batch.sequence_no,
                "results": [dict(item) for item in batch.results],
                "artifacts": [
                    {
                        "uri": item.uri,
                        "sha256": item.sha256,
                        "size_bytes": item.size_bytes,
                        "content_type": item.content_type,
                        "compression": item.compression,
                        "metadata": dict(item.metadata),
                    }
                    for item in batch.artifacts
                ],
                "cursor_after": batch.cursor_after,
                "final": batch.final,
            }
        )

    def commit_result_batch(
        self,
        batch: ResultBatch,
        *,
        worker_id: str,
        worker_instance_id: str,
    ) -> bool:
        payload = self._batch_payload(batch)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self.connection.execute(
                "SELECT payload_digest FROM fabric_result_batches WHERE batch_id=?",
                (batch.batch_id,),
            ).fetchone()
            if existing is not None:
                if str(existing["payload_digest"]) != digest:
                    raise BatchConflictError("batch replay changed payload")
                self.connection.rollback()
                return False
            row = self._assert_active_lease(
                batch.task_id,
                worker_id=worker_id,
                worker_instance_id=worker_instance_id,
                generation=batch.generation,
            )
            if int(row["next_sequence_no"]) != batch.sequence_no:
                raise BatchSequenceError(
                    f"expected sequence {row['next_sequence_no']}, "
                    f"got {batch.sequence_no}"
                )
            self.connection.execute(
                """
                INSERT INTO fabric_result_batches(
                    batch_id, task_id, generation, sequence_no, payload_json,
                    payload_digest, cursor_after, final, committed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch.batch_id,
                    batch.task_id,
                    batch.generation,
                    batch.sequence_no,
                    payload,
                    digest,
                    batch.cursor_after,
                    int(batch.final),
                    now,
                ),
            )
            for artifact in batch.artifacts:
                self.connection.execute(
                    """
                    INSERT INTO fabric_artifacts(
                        artifact_id, sha256, uri, size_bytes, content_type,
                        compression, metadata_json, first_task_id, first_seen_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(artifact_id) DO NOTHING
                    """,
                    (
                        artifact.artifact_id,
                        artifact.sha256.lower(),
                        artifact.uri,
                        artifact.size_bytes,
                        artifact.content_type,
                        artifact.compression,
                        _json(dict(artifact.metadata)),
                        batch.task_id,
                        now,
                    ),
                )
                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO fabric_batch_artifacts(batch_id, artifact_id)
                    VALUES (?, ?)
                    """,
                    (batch.batch_id, artifact.artifact_id),
                )
            next_state = "COMPLETE" if batch.final else "LEASED"
            self.connection.execute(
                """
                UPDATE fabric_work
                SET cursor=?, next_sequence_no=next_sequence_no+1,
                    state=?, lease_owner=CASE WHEN ? THEN NULL ELSE lease_owner END,
                    lease_owner_instance=CASE WHEN ? THEN NULL ELSE lease_owner_instance END,
                    lease_deadline=CASE WHEN ? THEN NULL ELSE lease_deadline END,
                    updated_at=?
                WHERE task_id=?
                """,
                (
                    batch.cursor_after,
                    next_state,
                    int(batch.final),
                    int(batch.final),
                    int(batch.final),
                    now,
                    batch.task_id,
                ),
            )
            self._emit_locked(
                "RESULT_COMMITTED",
                batch.task_id,
                {
                    "task_id": batch.task_id,
                    "batch_id": batch.batch_id,
                    "sequence_no": batch.sequence_no,
                    "final": batch.final,
                },
            )
            self.connection.commit()
            return True
        except BaseException:
            if self.connection.in_transaction:
                self.connection.rollback()
            raise

    def finish_task(
        self,
        task_id: str,
        *,
        worker_id: str,
        worker_instance_id: str,
        generation: int,
    ) -> None:
        now = float(self.clock())
        with self.connection:
            self._assert_active_lease(
                task_id,
                worker_id=worker_id,
                worker_instance_id=worker_instance_id,
                generation=generation,
            )
            self.connection.execute(
                """
                UPDATE fabric_work
                SET state='COMPLETE', lease_owner=NULL,
                    lease_owner_instance=NULL, lease_deadline=NULL, updated_at=?
                WHERE task_id=?
                """,
                (now, task_id),
            )
            self._emit_locked("WORK_COMPLETED", task_id, {"task_id": task_id})

    def fail_task(
        self,
        task_id: str,
        *,
        worker_id: str,
        worker_instance_id: str,
        generation: int,
        error: str,
        retryable: bool = True,
        retry_after_seconds: float = 0.0,
    ) -> None:
        if retry_after_seconds < 0:
            raise ValueError("retry_after_seconds must be non-negative")
        now = float(self.clock())
        with self.connection:
            row = self._assert_active_lease(
                task_id,
                worker_id=worker_id,
                worker_instance_id=worker_instance_id,
                generation=generation,
            )
            can_retry = retryable and int(row["attempt"]) < int(row["max_attempts"])
            state = "RETRY" if can_retry else "DEAD"
            self.connection.execute(
                """
                UPDATE fabric_work
                SET state=?, lease_owner=NULL, lease_owner_instance=NULL,
                    lease_deadline=NULL, not_before=?, last_error=?, updated_at=?
                WHERE task_id=?
                """,
                (
                    state,
                    now + (float(retry_after_seconds) if can_retry else 0.0),
                    error[:2000],
                    now,
                    task_id,
                ),
            )
            self._emit_locked(
                "WORK_FAILED",
                task_id,
                {"task_id": task_id, "retryable": can_retry, "state": state},
            )

    def unconsumed_batches(self, *, limit: int = 100) -> tuple[sqlite3.Row, ...]:
        if limit < 1:
            raise ValueError("limit must be positive")
        return tuple(
            self.connection.execute(
                """
                SELECT *
                FROM fabric_result_batches
                WHERE consumed_at IS NULL AND quarantined=0
                ORDER BY committed_at, task_id, sequence_no
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        )

    def mark_batch_consumed(self, batch_id: str) -> bool:
        with self.connection:
            return (
                self.connection.execute(
                    """
                    UPDATE fabric_result_batches
                    SET consumed_at=COALESCE(consumed_at, ?)
                    WHERE batch_id=?
                    """,
                    (float(self.clock()), batch_id),
                ).rowcount
                == 1
            )

    def mark_batch_consume_failed(
        self,
        batch_id: str,
        error: str,
        *,
        max_attempts: int = 20,
    ) -> bool:
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        with self.connection:
            row = self.connection.execute(
                """
                SELECT consume_attempts
                FROM fabric_result_batches
                WHERE batch_id=? AND consumed_at IS NULL
                """,
                (batch_id,),
            ).fetchone()
            if row is None:
                return False
            attempts = int(row["consume_attempts"]) + 1
            quarantined = attempts >= max_attempts
            self.connection.execute(
                """
                UPDATE fabric_result_batches
                SET consume_attempts=?, consume_error=?, quarantined=?
                WHERE batch_id=?
                """,
                (attempts, error[:2000], int(quarantined), batch_id),
            )
            return quarantined

    def pending_outbox(self, *, limit: int = 100) -> tuple[sqlite3.Row, ...]:
        if limit < 1:
            raise ValueError("limit must be positive")
        return tuple(
            self.connection.execute(
                """
                SELECT *
                FROM fabric_outbox
                WHERE published_at IS NULL
                ORDER BY created_at, event_id
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        )

    def mark_outbox_published(self, event_id: str) -> bool:
        with self.connection:
            return (
                self.connection.execute(
                    """
                    UPDATE fabric_outbox
                    SET published_at=COALESCE(published_at, ?)
                    WHERE event_id=?
                    """,
                    (float(self.clock()), event_id),
                ).rowcount
                == 1
            )

    def configure_provider_budget(
        self,
        provider: str,
        *,
        requests_per_second: float,
        max_global_inflight: int,
        require_qualified_region: bool = True,
    ) -> None:
        if not provider.strip() or requests_per_second <= 0 or max_global_inflight < 1:
            raise ValueError("invalid provider budget")
        now = float(self.clock())
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO fabric_provider_budgets(
                    provider, requests_per_second, max_global_inflight,
                    require_qualified_region, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(provider) DO UPDATE SET
                    requests_per_second=excluded.requests_per_second,
                    max_global_inflight=excluded.max_global_inflight,
                    require_qualified_region=excluded.require_qualified_region,
                    updated_at=excluded.updated_at
                """,
                (
                    provider,
                    float(requests_per_second),
                    int(max_global_inflight),
                    int(require_qualified_region),
                    now,
                ),
            )

    def record_provider_region_observation(
        self,
        provider: str,
        *,
        worker_id: str,
        worker_instance_id: str,
        task_id: str,
        generation: int,
        connect_success: bool,
        status_code: int | None,
        latency_ms: float,
        response_bytes: int,
        timeout: bool = False,
        policy_block: bool = False,
    ) -> str:
        worker = self._worker_row(worker_id, worker_instance_id)
        self._assert_active_lease(
            task_id,
            worker_id=worker_id,
            worker_instance_id=worker_instance_id,
            generation=generation,
        )
        region = str(worker["region"])
        success = bool(connect_success and status_code is not None and status_code < 500)
        throttle = status_code in {429, 503}
        now = float(self.clock())
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO fabric_provider_regions(
                    provider, region, state, samples, successes, timeouts,
                    throttles, policy_blocks, total_latency_ms, response_bytes,
                    updated_at
                ) VALUES (?, ?, 'UNKNOWN', 1, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, region) DO UPDATE SET
                    samples=samples+1,
                    successes=successes+excluded.successes,
                    timeouts=timeouts+excluded.timeouts,
                    throttles=throttles+excluded.throttles,
                    policy_blocks=policy_blocks+excluded.policy_blocks,
                    total_latency_ms=total_latency_ms+excluded.total_latency_ms,
                    response_bytes=response_bytes+excluded.response_bytes,
                    updated_at=excluded.updated_at
                """,
                (
                    provider,
                    region,
                    int(success),
                    int(timeout),
                    int(throttle),
                    int(policy_block),
                    max(0.0, float(latency_ms)),
                    max(0, int(response_bytes)),
                    now,
                ),
            )
            row = self.connection.execute(
                """
                SELECT samples, successes, timeouts, throttles, policy_blocks
                FROM fabric_provider_regions
                WHERE provider=? AND region=?
                """,
                (provider, region),
            ).fetchone()
            assert row is not None
            samples = int(row["samples"])
            successes = int(row["successes"])
            blocked = int(row["policy_blocks"])
            failures = int(row["timeouts"]) + int(row["throttles"]) + blocked
            if blocked:
                state = "BLOCKED"
            elif samples >= 3 and successes >= 2 and failures <= samples // 2:
                state = "QUALIFIED"
            elif samples >= 3 and successes == 0:
                state = "UNQUALIFIED"
            else:
                state = "UNKNOWN"
            self.connection.execute(
                """
                UPDATE fabric_provider_regions
                SET state=?, updated_at=?
                WHERE provider=? AND region=?
                """,
                (state, now, provider, region),
            )
        return state

    def _egress_day_key(self, now: float) -> int:
        return int(now // 86400)

    def _egress_available(self, worker: sqlite3.Row, now: float) -> bool:
        budget = int(worker["daily_egress_budget_bytes"])
        if budget <= 0:
            return True
        row = self.connection.execute(
            """
            SELECT response_bytes
            FROM fabric_worker_egress_daily
            WHERE worker_id=? AND day_key=?
            """,
            (str(worker["worker_id"]), self._egress_day_key(now)),
        ).fetchone()
        return row is None or int(row["response_bytes"]) < budget

    def issue_provider_permit(
        self,
        provider: str,
        *,
        worker_id: str,
        worker_instance_id: str,
        task_id: str,
        generation: int,
        request_id: str,
        ttl_seconds: float = 30.0,
    ) -> ProviderPermit | None:
        if not request_id.strip() or ttl_seconds <= 0:
            raise ValueError("invalid provider permit request")
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            worker = self._worker_row(worker_id, worker_instance_id)
            self._assert_active_lease(
                task_id,
                worker_id=worker_id,
                worker_instance_id=worker_instance_id,
                generation=generation,
            )
            allowed = set(json.loads(str(worker["allowed_providers_json"])))
            if provider not in allowed:
                raise ProviderAccessDeniedError(provider)
            if not self._egress_available(worker, now):
                raise WorkerEgressBudgetExceededError(worker_id)
            prior = self.connection.execute(
                """
                SELECT *
                FROM fabric_provider_permits
                WHERE worker_id=? AND request_id=?
                """,
                (worker_id, request_id),
            ).fetchone()
            if prior is not None:
                self.connection.rollback()
                return ProviderPermit(
                    permit_id=str(prior["permit_id"]),
                    request_id=str(prior["request_id"]),
                    provider=str(prior["provider"]),
                    worker_id=str(prior["worker_id"]),
                    worker_instance_id=str(prior["worker_instance_id"]),
                    task_id=str(prior["task_id"]),
                    generation=int(prior["generation"]),
                    allowed_requests=int(prior["allowed_requests"]),
                    max_inflight=int(prior["max_inflight"]),
                    expires_at=float(prior["expires_at"]),
                )
            budget = self.connection.execute(
                "SELECT * FROM fabric_provider_budgets WHERE provider=?",
                (provider,),
            ).fetchone()
            if budget is None:
                raise ProviderAccessDeniedError(
                    f"provider budget is not configured: {provider}"
                )
            self.connection.execute(
                """
                UPDATE fabric_provider_permits
                SET active=0
                WHERE provider=? AND active=1 AND expires_at <= ?
                """,
                (provider, now),
            )
            if int(budget["require_qualified_region"]):
                region = self.connection.execute(
                    """
                    SELECT state FROM fabric_provider_regions
                    WHERE provider=? AND region=?
                    """,
                    (provider, str(worker["region"])),
                ).fetchone()
                if region is None or str(region["state"]) != "QUALIFIED":
                    self.connection.rollback()
                    return None
            active = int(
                self.connection.execute(
                    """
                    SELECT COUNT(*) AS n
                    FROM fabric_provider_permits
                    WHERE provider=? AND active=1
                    """,
                    (provider,),
                ).fetchone()["n"]
            )
            if (
                active >= int(budget["max_global_inflight"])
                or now < float(budget["next_request_at"])
                or now < float(budget["cooldown_until"])
            ):
                self.connection.rollback()
                return None
            next_at = now + 1.0 / float(budget["requests_per_second"])
            self.connection.execute(
                """
                UPDATE fabric_provider_budgets
                SET next_request_at=?, updated_at=?
                WHERE provider=?
                """,
                (next_at, now, provider),
            )
            permit = ProviderPermit(
                permit_id=uuid4().hex,
                request_id=request_id,
                provider=provider,
                worker_id=worker_id,
                worker_instance_id=worker_instance_id,
                task_id=task_id,
                generation=generation,
                allowed_requests=1,
                max_inflight=int(budget["max_global_inflight"]),
                expires_at=now + float(ttl_seconds),
            )
            self.connection.execute(
                """
                INSERT INTO fabric_provider_permits(
                    permit_id, request_id, provider, worker_id,
                    worker_instance_id, task_id, generation, allowed_requests,
                    max_inflight, expires_at, issued_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    permit.permit_id,
                    permit.request_id,
                    permit.provider,
                    permit.worker_id,
                    permit.worker_instance_id,
                    permit.task_id,
                    permit.generation,
                    permit.allowed_requests,
                    permit.max_inflight,
                    permit.expires_at,
                    now,
                ),
            )
            self.connection.commit()
            return permit
        except BaseException:
            if self.connection.in_transaction:
                self.connection.rollback()
            raise

    def report_provider_permit(
        self,
        permit_id: str,
        *,
        worker_id: str,
        worker_instance_id: str,
        status_code: int | None,
        cooldown_seconds: float = 0.0,
        response_bytes: int = 0,
    ) -> None:
        if cooldown_seconds < 0 or response_bytes < 0:
            raise ValueError("invalid provider report")
        now = float(self.clock())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self._worker_row(worker_id, worker_instance_id)
            row = self.connection.execute(
                "SELECT * FROM fabric_provider_permits WHERE permit_id=?",
                (permit_id,),
            ).fetchone()
            if (
                row is None
                or str(row["worker_id"]) != worker_id
                or str(row["worker_instance_id"]) != worker_instance_id
            ):
                raise ProviderAccessDeniedError("unknown provider permit")
            if not int(row["active"]):
                self.connection.rollback()
                return
            self.connection.execute(
                """
                UPDATE fabric_provider_permits
                SET active=0, status_code=?
                WHERE permit_id=?
                """,
                (status_code, permit_id),
            )
            if cooldown_seconds:
                self.connection.execute(
                    """
                    UPDATE fabric_provider_budgets
                    SET cooldown_until=MAX(cooldown_until, ?), updated_at=?
                    WHERE provider=?
                    """,
                    (now + float(cooldown_seconds), now, str(row["provider"])),
                )
            day_key = self._egress_day_key(now)
            self.connection.execute(
                """
                INSERT INTO fabric_worker_egress_daily(
                    worker_id, day_key, response_bytes, updated_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(worker_id, day_key) DO UPDATE SET
                    response_bytes=response_bytes+excluded.response_bytes,
                    updated_at=excluded.updated_at
                """,
                (worker_id, day_key, int(response_bytes), now),
            )
            self.connection.commit()
        except BaseException:
            if self.connection.in_transaction:
                self.connection.rollback()
            raise

    def task_row(self, task_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM fabric_work WHERE task_id=?",
            (task_id,),
        ).fetchone()
        if row is None:
            raise KeyError(task_id)
        return row

    def status_snapshot(self) -> dict[str, int]:
        states = {
            str(row["state"]): int(row["n"])
            for row in self.connection.execute(
                "SELECT state, COUNT(*) AS n FROM fabric_work GROUP BY state"
            )
        }
        return {
            "workers": int(
                self.connection.execute(
                    "SELECT COUNT(*) AS n FROM fabric_workers WHERE revoked=0"
                ).fetchone()["n"]
            ),
            "pending": states.get("PENDING", 0) + states.get("RETRY", 0),
            "leased": states.get("LEASED", 0),
            "complete": states.get("COMPLETE", 0),
            "dead": states.get("DEAD", 0),
            "unconsumed_batches": int(
                self.connection.execute(
                    "SELECT COUNT(*) AS n FROM fabric_result_batches WHERE consumed_at IS NULL"
                ).fetchone()["n"]
            ),
            "pending_outbox": int(
                self.connection.execute(
                    "SELECT COUNT(*) AS n FROM fabric_outbox WHERE published_at IS NULL"
                ).fetchone()["n"]
            ),
        }
