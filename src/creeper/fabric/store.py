"""Fabric authority interfaces and local SQLite compatibility backend."""

from __future__ import annotations

import json
import math
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Protocol

from .models import (
    FABRIC_PROTOCOL_VERSION,
    FabricCapability,
    FabricTaskState,
    FabricWorkClass,
    LeaseToken,
    ProviderPermit,
    ResultBatch,
    WorkerDescriptor,
    WorkSpec,
    canonical_json,
)
from .schema import SQLITE_DDL


class FabricError(RuntimeError):
    pass


class WorkerRejectedError(FabricError):
    pass


class StaleLeaseError(FabricError):
    pass


class BatchConflictError(FabricError):
    pass


class BatchSequenceError(FabricError):
    pass


class FabricStore(Protocol):
    def register_worker(self, worker: WorkerDescriptor) -> None: ...

    def submit_work(
        self,
        work: WorkSpec,
        *,
        available_at: float | None = None,
    ) -> str: ...

    def claim(
        self,
        worker_id: str,
        *,
        queue: str = "default",
        lease_seconds: float = 60.0,
    ) -> LeaseToken | None: ...

    def renew(self, lease: LeaseToken, *, lease_seconds: float = 60.0) -> LeaseToken: ...

    def commit_batch(self, lease: LeaseToken, batch: ResultBatch) -> bool: ...

    def complete(self, lease: LeaseToken) -> None: ...

    def fail(
        self,
        lease: LeaseToken,
        *,
        error: str,
        retryable: bool,
        retry_delay_seconds: float = 0.0,
    ) -> None: ...


def _work_payload(work: WorkSpec) -> dict[str, object]:
    return {
        "work_class": work.work_class.value,
        "producer": work.producer,
        "algorithm_version": work.algorithm_version,
        "partition_key": work.partition_key,
        "input_identity": work.input_identity,
        "coverage": dict(work.coverage),
        "required_capabilities": [item.value for item in work.required_capabilities],
        "priority": float(work.priority),
        "queue": work.queue,
        "max_attempts": work.max_attempts,
        "provider": work.provider,
        "min_memory_bytes": work.min_memory_bytes,
        "network_class": work.network_class,
    }


def _work_from_row(row: sqlite3.Row) -> WorkSpec:
    return WorkSpec(
        work_class=FabricWorkClass(str(row["work_class"])),
        producer=str(row["producer"]),
        algorithm_version=str(row["algorithm_version"]),
        partition_key=str(row["partition_key"]),
        input_identity=str(row["input_identity"]),
        coverage=json.loads(str(row["coverage_json"])),
        required_capabilities=tuple(
            FabricCapability(item)
            for item in json.loads(str(row["required_capabilities_json"]))
        ),
        priority=float(row["priority"]),
        queue=str(row["queue_name"]),
        max_attempts=int(row["max_attempts"]),
        provider=(None if row["provider"] is None else str(row["provider"])),
        min_memory_bytes=int(row["min_memory_bytes"]),
        network_class=(
            None if row["network_class"] is None else str(row["network_class"])
        ),
    )


def _worker_payload(worker: WorkerDescriptor) -> dict[str, object]:
    return {
        "worker_id": worker.worker_id,
        "region": worker.region,
        "runtime_class": worker.runtime_class,
        "architecture": worker.architecture,
        "network_class": worker.network_class,
        "cpu_count": worker.cpu_count,
        "memory_bytes": worker.memory_bytes,
        "capabilities": [item.value for item in worker.capabilities],
        "allowed_providers": list(worker.allowed_providers),
        "max_concurrency": worker.max_concurrency,
        "labels": dict(worker.labels),
        "protocol_version": worker.protocol_version,
        "edition": worker.edition,
    }


def _worker_from_json(value: str) -> WorkerDescriptor:
    data = json.loads(value)
    return WorkerDescriptor(
        worker_id=str(data["worker_id"]),
        region=str(data["region"]),
        runtime_class=str(data["runtime_class"]),
        architecture=str(data["architecture"]),
        network_class=str(data["network_class"]),
        cpu_count=int(data["cpu_count"]),
        memory_bytes=int(data["memory_bytes"]),
        capabilities=tuple(FabricCapability(item) for item in data["capabilities"]),
        allowed_providers=tuple(str(item) for item in data["allowed_providers"]),
        max_concurrency=int(data["max_concurrency"]),
        labels={str(k): str(v) for k, v in dict(data.get("labels", {})).items()},
        protocol_version=str(data["protocol_version"]),
        edition=str(data["edition"]),
    )


class SQLiteFabricStore:
    """Single-host compatibility authority implementing the Fabric v2 protocol.

    Multiple local processes may use the WAL database, but this backend is not
    a multi-host production authority. Production uses PostgreSQL.
    """

    def __init__(
        self,
        path: Path,
        *,
        clock=time.time,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        self.connection = sqlite3.connect(self.path, timeout=30.0)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA busy_timeout=30000")
        self.connection.executescript(SQLITE_DDL)

    def close(self) -> None:
        self.connection.close()

    def _now(self) -> float:
        value = float(self.clock())
        if not math.isfinite(value) or value < 0:
            raise ValueError("fabric clock must be finite and non-negative")
        return value

    def _emit_locked(
        self,
        *,
        topic: str,
        aggregate_type: str,
        aggregate_id: str,
        aggregate_sequence: int,
        payload: dict[str, object],
        now: float,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO fabric_outbox_v2(
                event_id, topic, aggregate_type, aggregate_id,
                aggregate_sequence, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                topic,
                aggregate_type,
                aggregate_id,
                int(aggregate_sequence),
                canonical_json(payload),
                now,
            ),
        )

    def register_worker(self, worker: WorkerDescriptor) -> None:
        if worker.protocol_version != FABRIC_PROTOCOL_VERSION:
            raise WorkerRejectedError(
                f"unsupported worker protocol: {worker.protocol_version}"
            )
        now = self._now()
        payload = canonical_json(_worker_payload(worker))
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO fabric_workers_v2(
                    worker_id, descriptor_json, protocol_version, edition,
                    last_heartbeat, revoked
                ) VALUES (?, ?, ?, ?, ?, 0)
                ON CONFLICT(worker_id) DO UPDATE SET
                    descriptor_json=excluded.descriptor_json,
                    protocol_version=excluded.protocol_version,
                    edition=excluded.edition,
                    last_heartbeat=excluded.last_heartbeat
                """,
                (
                    worker.worker_id,
                    payload,
                    worker.protocol_version,
                    worker.edition,
                    now,
                ),
            )

    def heartbeat(self, worker_id: str) -> None:
        now = self._now()
        with self.connection:
            changed = self.connection.execute(
                """
                UPDATE fabric_workers_v2
                SET last_heartbeat=?
                WHERE worker_id=? AND revoked=0
                """,
                (now, worker_id),
            ).rowcount
        if changed != 1:
            raise WorkerRejectedError(f"unknown or revoked worker: {worker_id}")

    def revoke_worker(self, worker_id: str) -> None:
        now = self._now()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            changed = self.connection.execute(
                "UPDATE fabric_workers_v2 SET revoked=1 WHERE worker_id=?",
                (worker_id,),
            ).rowcount
            if changed != 1:
                raise KeyError(worker_id)
            self.connection.execute(
                """
                UPDATE fabric_tasks_v2
                SET state='READY', lease_owner=NULL, lease_deadline=NULL,
                    available_at=?, updated_at=?,
                    last_error='worker revoked while lease active'
                WHERE state='LEASED' AND lease_owner=?
                """,
                (now, now, worker_id),
            )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    def submit_work(
        self,
        work: WorkSpec,
        *,
        available_at: float | None = None,
    ) -> str:
        now = self._now()
        when = now if available_at is None else float(available_at)
        if not math.isfinite(when) or when < 0:
            raise ValueError("available_at must be finite and non-negative")
        task_id = str(uuid.uuid4())
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self.connection.execute(
                """
                INSERT OR IGNORE INTO fabric_tasks_v2(
                    task_id, work_key, work_class, producer, algorithm_version,
                    partition_key, input_identity, coverage_json,
                    required_capabilities_json, priority, queue_name,
                    max_attempts, provider, min_memory_bytes, network_class,
                    state, available_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                          'READY', ?, ?, ?)
                """,
                (
                    task_id,
                    work.work_key,
                    work.work_class.value,
                    work.producer,
                    work.algorithm_version,
                    work.partition_key,
                    work.input_identity,
                    canonical_json(dict(work.coverage)),
                    canonical_json(
                        [item.value for item in work.required_capabilities]
                    ),
                    float(work.priority),
                    work.queue,
                    work.max_attempts,
                    work.provider,
                    work.min_memory_bytes,
                    work.network_class,
                    when,
                    now,
                    now,
                ),
            )
            row = self.connection.execute(
                "SELECT task_id FROM fabric_tasks_v2 WHERE work_key=?",
                (work.work_key,),
            ).fetchone()
            if row is None:
                raise RuntimeError("fabric work disappeared after admission")
            durable_task_id = str(row["task_id"])
            if durable_task_id == task_id:
                capability = (
                    work.required_capabilities[0].value.lower()
                    if work.required_capabilities
                    else "generic"
                )
                self._emit_locked(
                    topic=f"fabric.task.ready.{capability}",
                    aggregate_type="task",
                    aggregate_id=task_id,
                    aggregate_sequence=0,
                    payload={"task_id": task_id, "work_key": work.work_key},
                    now=now,
                )
            self.connection.commit()
            return durable_task_id
        except BaseException:
            self.connection.rollback()
            raise

    def _worker_locked(self, worker_id: str) -> WorkerDescriptor:
        row = self.connection.execute(
            """
            SELECT descriptor_json, revoked, protocol_version
            FROM fabric_workers_v2
            WHERE worker_id=?
            """,
            (worker_id,),
        ).fetchone()
        if (
            row is None
            or int(row["revoked"]) != 0
            or str(row["protocol_version"]) != FABRIC_PROTOCOL_VERSION
        ):
            raise WorkerRejectedError(f"unknown or revoked worker: {worker_id}")
        return _worker_from_json(str(row["descriptor_json"]))

    def _reclaim_expired_locked(self, now: float) -> None:
        rows = self.connection.execute(
            """
            SELECT task_id, attempt, max_attempts
            FROM fabric_tasks_v2
            WHERE state='LEASED' AND lease_deadline <= ?
            """,
            (now,),
        ).fetchall()
        for row in rows:
            terminal = int(row["attempt"]) >= int(row["max_attempts"])
            state = "FAILED" if terminal else "READY"
            self.connection.execute(
                """
                UPDATE fabric_tasks_v2
                SET state=?, lease_owner=NULL, lease_deadline=NULL,
                    available_at=?, updated_at=?,
                    last_error='lease expired'
                WHERE task_id=? AND state='LEASED'
                """,
                (state, now, now, str(row["task_id"])),
            )

    @staticmethod
    def _worker_matches(worker: WorkerDescriptor, row: sqlite3.Row) -> bool:
        required = {
            FabricCapability(item)
            for item in json.loads(str(row["required_capabilities_json"]))
        }
        if not required.issubset(set(worker.capabilities)):
            return False
        if int(row["min_memory_bytes"]) > worker.memory_bytes:
            return False
        required_network = row["network_class"]
        if (
            required_network is not None
            and str(required_network) != worker.network_class
        ):
            return False
        provider = row["provider"]
        if provider is not None and str(provider) not in worker.allowed_providers:
            return False
        return True

    def claim(
        self,
        worker_id: str,
        *,
        queue: str = "default",
        lease_seconds: float = 60.0,
    ) -> LeaseToken | None:
        if not queue.strip():
            raise ValueError("queue must be non-empty")
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or not math.isfinite(float(lease_seconds))
            or lease_seconds <= 0
        ):
            raise ValueError("lease_seconds must be finite and positive")
        now = self._now()
        deadline = now + float(lease_seconds)
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            worker = self._worker_locked(worker_id)
            self._reclaim_expired_locked(now)
            rows = self.connection.execute(
                """
                SELECT *
                FROM fabric_tasks_v2
                WHERE state='READY'
                  AND queue_name=?
                  AND available_at <= ?
                  AND attempt < max_attempts
                ORDER BY priority DESC, available_at, created_at, task_id
                LIMIT 256
                """,
                (queue, now),
            ).fetchall()
            selected = next(
                (row for row in rows if self._worker_matches(worker, row)),
                None,
            )
            if selected is None:
                self.connection.commit()
                return None

            task_id = str(selected["task_id"])
            changed = self.connection.execute(
                """
                UPDATE fabric_tasks_v2
                SET state='LEASED', lease_owner=?,
                    lease_epoch=lease_epoch+1, lease_deadline=?,
                    attempt=attempt+1, updated_at=?
                WHERE task_id=? AND state='READY'
                """,
                (worker_id, deadline, now, task_id),
            ).rowcount
            if changed != 1:
                raise RuntimeError("local fabric claim lost atomic ownership")
            row = self.connection.execute(
                "SELECT * FROM fabric_tasks_v2 WHERE task_id=?",
                (task_id,),
            ).fetchone()
            assert row is not None
            self.connection.execute(
                """
                UPDATE fabric_workers_v2 SET last_heartbeat=? WHERE worker_id=?
                """,
                (now, worker_id),
            )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        work = _work_from_row(row)
        cursor = (
            None
            if row["cursor_json"] is None
            else json.loads(str(row["cursor_json"]))
        )
        return LeaseToken(
            task_id=task_id,
            work_key=str(row["work_key"]),
            worker_id=worker_id,
            lease_epoch=int(row["lease_epoch"]),
            lease_deadline=float(row["lease_deadline"]),
            attempt=int(row["attempt"]),
            work=work,
            cursor=cursor,
            next_sequence_no=int(row["next_sequence_no"]),
        )

    def _assert_lease_locked(
        self,
        lease: LeaseToken,
        *,
        now: float,
    ) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM fabric_tasks_v2 WHERE task_id=?",
            (lease.task_id,),
        ).fetchone()
        if (
            row is None
            or str(row["state"]) != FabricTaskState.LEASED.value
            or str(row["lease_owner"]) != lease.worker_id
            or int(row["lease_epoch"]) != lease.lease_epoch
            or row["lease_deadline"] is None
            or float(row["lease_deadline"]) <= now
        ):
            raise StaleLeaseError(
                f"worker no longer owns task epoch: {lease.task_id}"
            )
        return row

    def renew(
        self,
        lease: LeaseToken,
        *,
        lease_seconds: float = 60.0,
    ) -> LeaseToken:
        if not math.isfinite(float(lease_seconds)) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be finite and positive")
        now = self._now()
        deadline = now + float(lease_seconds)
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self._assert_lease_locked(lease, now=now)
            self.connection.execute(
                """
                UPDATE fabric_tasks_v2
                SET lease_deadline=?, updated_at=?
                WHERE task_id=?
                """,
                (deadline, now, lease.task_id),
            )
            self.connection.execute(
                "UPDATE fabric_workers_v2 SET last_heartbeat=? WHERE worker_id=?",
                (now, lease.worker_id),
            )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        return LeaseToken(
            task_id=lease.task_id,
            work_key=lease.work_key,
            worker_id=lease.worker_id,
            lease_epoch=lease.lease_epoch,
            lease_deadline=deadline,
            attempt=lease.attempt,
            work=lease.work,
            cursor=lease.cursor,
            next_sequence_no=lease.next_sequence_no,
        )

    def commit_batch(self, lease: LeaseToken, batch: ResultBatch) -> bool:
        if batch.task_id != lease.task_id or batch.lease_epoch != lease.lease_epoch:
            raise StaleLeaseError("result batch does not match lease identity")
        now = self._now()
        payload = canonical_json(
            {
                "results": batch.results,
                "cursor_after": batch.cursor_after,
            }
        )
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self._assert_lease_locked(lease, now=now)
            existing = self.connection.execute(
                """
                SELECT payload_digest
                FROM fabric_result_batches_v2
                WHERE task_id=? AND sequence_no=?
                """,
                (batch.task_id, batch.sequence_no),
            ).fetchone()
            if existing is not None:
                if str(existing["payload_digest"]) != batch.payload_digest:
                    raise BatchConflictError(
                        "result batch sequence replayed with different payload"
                    )
                self.connection.commit()
                return False
            expected = int(row["next_sequence_no"])
            if batch.sequence_no != expected:
                raise BatchSequenceError(
                    f"expected sequence {expected}, got {batch.sequence_no}"
                )
            self.connection.execute(
                """
                INSERT INTO fabric_result_batches_v2(
                    task_id, sequence_no, lease_epoch, payload_json,
                    payload_digest, cursor_after_json, committed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch.task_id,
                    batch.sequence_no,
                    batch.lease_epoch,
                    payload,
                    batch.payload_digest,
                    (
                        None
                        if batch.cursor_after is None
                        else canonical_json(dict(batch.cursor_after))
                    ),
                    now,
                ),
            )
            self.connection.execute(
                """
                UPDATE fabric_tasks_v2
                SET cursor_json=?, next_sequence_no=next_sequence_no+1,
                    updated_at=?
                WHERE task_id=?
                """,
                (
                    None
                    if batch.cursor_after is None
                    else canonical_json(dict(batch.cursor_after)),
                    now,
                    batch.task_id,
                ),
            )
            self._emit_locked(
                topic="fabric.result.committed",
                aggregate_type="task",
                aggregate_id=batch.task_id,
                aggregate_sequence=batch.sequence_no,
                payload={
                    "task_id": batch.task_id,
                    "sequence_no": batch.sequence_no,
                    "lease_epoch": batch.lease_epoch,
                    "payload_digest": batch.payload_digest,
                },
                now=now,
            )
            self.connection.commit()
            return True
        except BaseException:
            self.connection.rollback()
            raise

    def complete(self, lease: LeaseToken) -> None:
        now = self._now()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self._assert_lease_locked(lease, now=now)
            sequence = int(row["next_sequence_no"])
            self.connection.execute(
                """
                UPDATE fabric_tasks_v2
                SET state='SUCCEEDED', lease_owner=NULL, lease_deadline=NULL,
                    updated_at=?
                WHERE task_id=?
                """,
                (now, lease.task_id),
            )
            self._emit_locked(
                topic="fabric.task.completed",
                aggregate_type="task",
                aggregate_id=lease.task_id,
                aggregate_sequence=sequence,
                payload={
                    "task_id": lease.task_id,
                    "work_key": lease.work_key,
                    "lease_epoch": lease.lease_epoch,
                },
                now=now,
            )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    def fail(
        self,
        lease: LeaseToken,
        *,
        error: str,
        retryable: bool,
        retry_delay_seconds: float = 0.0,
    ) -> None:
        if not isinstance(error, str) or not error.strip():
            raise ValueError("error must be non-empty")
        if not math.isfinite(float(retry_delay_seconds)) or retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must be finite and non-negative")
        now = self._now()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self._assert_lease_locked(lease, now=now)
            exhausted = int(row["attempt"]) >= int(row["max_attempts"])
            retry = bool(retryable and not exhausted)
            state = "READY" if retry else "FAILED"
            available = now + float(retry_delay_seconds) if retry else now
            self.connection.execute(
                """
                UPDATE fabric_tasks_v2
                SET state=?, lease_owner=NULL, lease_deadline=NULL,
                    available_at=?, last_error=?, updated_at=?
                WHERE task_id=?
                """,
                (state, available, error.strip(), now, lease.task_id),
            )
            self._emit_locked(
                topic=(
                    "fabric.task.retry"
                    if retry
                    else "fabric.task.failed"
                ),
                aggregate_type="task",
                aggregate_id=lease.task_id,
                aggregate_sequence=int(row["next_sequence_no"]),
                payload={
                    "task_id": lease.task_id,
                    "lease_epoch": lease.lease_epoch,
                    "retryable": retry,
                    "error": error.strip(),
                },
                now=now,
            )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    def configure_provider(
        self,
        provider: str,
        *,
        requests_per_second: float,
        max_global_inflight: int,
        require_qualified_region: bool = True,
    ) -> None:
        if not provider.strip():
            raise ValueError("provider must be non-empty")
        if (
            not math.isfinite(float(requests_per_second))
            or requests_per_second <= 0
        ):
            raise ValueError("requests_per_second must be finite and positive")
        if (
            isinstance(max_global_inflight, bool)
            or not isinstance(max_global_inflight, int)
            or max_global_inflight < 1
        ):
            raise ValueError("max_global_inflight must be positive")
        now = self._now()
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO fabric_provider_budgets_v2(
                    provider, requests_per_second, max_global_inflight,
                    require_qualified_region, next_request_at,
                    cooldown_until, updated_at
                ) VALUES (?, ?, ?, ?, 0, 0, ?)
                ON CONFLICT(provider) DO UPDATE SET
                    requests_per_second=excluded.requests_per_second,
                    max_global_inflight=excluded.max_global_inflight,
                    require_qualified_region=excluded.require_qualified_region,
                    updated_at=excluded.updated_at
                """,
                (
                    provider.strip(),
                    float(requests_per_second),
                    max_global_inflight,
                    int(bool(require_qualified_region)),
                    now,
                ),
            )

    def set_provider_region(
        self,
        provider: str,
        region: str,
        *,
        qualified: bool,
    ) -> None:
        if not provider.strip() or not region.strip():
            raise ValueError("provider and region must be non-empty")
        now = self._now()
        with self.connection:
            if (
                self.connection.execute(
                    "SELECT 1 FROM fabric_provider_budgets_v2 WHERE provider=?",
                    (provider,),
                ).fetchone()
                is None
            ):
                raise KeyError(provider)
            self.connection.execute(
                """
                INSERT INTO fabric_provider_regions_v2(
                    provider, region, qualified, updated_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(provider, region) DO UPDATE SET
                    qualified=excluded.qualified,
                    updated_at=excluded.updated_at
                """,
                (provider, region, int(bool(qualified)), now),
            )

    def acquire_provider_permit(
        self,
        lease: LeaseToken,
        provider: str,
        *,
        allowed_requests: int = 1,
        ttl_seconds: float = 30.0,
    ) -> ProviderPermit | None:
        if not provider.strip():
            raise ValueError("provider must be non-empty")
        if (
            isinstance(allowed_requests, bool)
            or not isinstance(allowed_requests, int)
            or allowed_requests < 1
        ):
            raise ValueError("allowed_requests must be positive")
        if not math.isfinite(float(ttl_seconds)) or ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be finite and positive")
        now = self._now()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self._assert_lease_locked(lease, now=now)
            worker = self._worker_locked(lease.worker_id)
            if provider not in worker.allowed_providers:
                raise WorkerRejectedError(
                    f"worker is not allowed to access provider: {provider}"
                )
            budget = self.connection.execute(
                """
                SELECT *
                FROM fabric_provider_budgets_v2
                WHERE provider=?
                """,
                (provider,),
            ).fetchone()
            if budget is None:
                raise KeyError(f"unknown provider budget: {provider}")
            if int(budget["require_qualified_region"]):
                region = self.connection.execute(
                    """
                    SELECT qualified
                    FROM fabric_provider_regions_v2
                    WHERE provider=? AND region=?
                    """,
                    (provider, worker.region),
                ).fetchone()
                if region is None or int(region["qualified"]) != 1:
                    self.connection.commit()
                    return None
            self.connection.execute(
                """
                UPDATE fabric_provider_permits_v2
                SET active=0
                WHERE provider=? AND active=1 AND expires_at <= ?
                """,
                (provider, now),
            )
            active = int(
                self.connection.execute(
                    """
                    SELECT COUNT(*) AS n
                    FROM fabric_provider_permits_v2
                    WHERE provider=? AND active=1 AND expires_at > ?
                    """,
                    (provider, now),
                ).fetchone()["n"]
            )
            if (
                active >= int(budget["max_global_inflight"])
                or now < float(budget["next_request_at"])
                or now < float(budget["cooldown_until"])
            ):
                self.connection.commit()
                return None
            permit_id = str(uuid.uuid4())
            expires = now + float(ttl_seconds)
            next_request = now + (
                float(allowed_requests) / float(budget["requests_per_second"])
            )
            self.connection.execute(
                """
                UPDATE fabric_provider_budgets_v2
                SET next_request_at=?, updated_at=?
                WHERE provider=?
                """,
                (next_request, now, provider),
            )
            self.connection.execute(
                """
                INSERT INTO fabric_provider_permits_v2(
                    permit_id, provider, worker_id, task_id, lease_epoch,
                    allowed_requests, expires_at, issued_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    permit_id,
                    provider,
                    lease.worker_id,
                    lease.task_id,
                    lease.lease_epoch,
                    allowed_requests,
                    expires,
                    now,
                ),
            )
            self.connection.commit()
            return ProviderPermit(
                permit_id=permit_id,
                provider=provider,
                worker_id=lease.worker_id,
                task_id=lease.task_id,
                lease_epoch=lease.lease_epoch,
                allowed_requests=allowed_requests,
                expires_at=expires,
            )
        except BaseException:
            self.connection.rollback()
            raise

    def report_provider_permit(
        self,
        permit: ProviderPermit,
        *,
        status_code: int | None,
        response_bytes: int = 0,
        throttled: bool = False,
        cooldown_seconds: float = 0.0,
    ) -> None:
        if (
            isinstance(response_bytes, bool)
            or not isinstance(response_bytes, int)
            or response_bytes < 0
        ):
            raise ValueError("response_bytes must be non-negative")
        if not math.isfinite(float(cooldown_seconds)) or cooldown_seconds < 0:
            raise ValueError("cooldown_seconds must be finite and non-negative")
        now = self._now()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                """
                SELECT p.*, w.descriptor_json
                FROM fabric_provider_permits_v2 AS p
                JOIN fabric_workers_v2 AS w ON w.worker_id=p.worker_id
                WHERE p.permit_id=?
                """,
                (permit.permit_id,),
            ).fetchone()
            if row is None:
                raise KeyError(permit.permit_id)
            if (
                str(row["worker_id"]) != permit.worker_id
                or str(row["task_id"]) != permit.task_id
                or int(row["lease_epoch"]) != permit.lease_epoch
            ):
                raise StaleLeaseError("provider permit identity mismatch")
            if int(row["active"]) == 0:
                self.connection.commit()
                return
            worker = _worker_from_json(str(row["descriptor_json"]))
            success = status_code is not None and 200 <= status_code < 400
            self.connection.execute(
                """
                UPDATE fabric_provider_permits_v2
                SET active=0, status_code=?, response_bytes=?
                WHERE permit_id=?
                """,
                (status_code, response_bytes, permit.permit_id),
            )
            self.connection.execute(
                """
                INSERT INTO fabric_provider_regions_v2(
                    provider, region, qualified, samples, successes,
                    throttles, failures, updated_at
                ) VALUES (?, ?, 0, 1, ?, ?, ?, ?)
                ON CONFLICT(provider, region) DO UPDATE SET
                    samples=samples+1,
                    successes=successes+excluded.successes,
                    throttles=throttles+excluded.throttles,
                    failures=failures+excluded.failures,
                    updated_at=excluded.updated_at
                """,
                (
                    permit.provider,
                    worker.region,
                    int(success),
                    int(bool(throttled)),
                    int(not success and not throttled),
                    now,
                ),
            )
            if throttled and cooldown_seconds > 0:
                self.connection.execute(
                    """
                    UPDATE fabric_provider_budgets_v2
                    SET cooldown_until=MAX(cooldown_until, ?), updated_at=?
                    WHERE provider=?
                    """,
                    (now + float(cooldown_seconds), now, permit.provider),
                )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    def consume_request_nonce(
        self,
        worker_id: str,
        nonce: str,
        *,
        retention_seconds: float = 900.0,
    ) -> bool:
        if not worker_id.strip() or not nonce.strip():
            raise ValueError("worker_id and nonce are required")
        if (
            not math.isfinite(float(retention_seconds))
            or retention_seconds <= 0
        ):
            raise ValueError("retention_seconds must be finite and positive")
        now = self._now()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self.connection.execute(
                """
                DELETE FROM fabric_request_nonces_v2
                WHERE seen_at < ?
                """,
                (now - float(retention_seconds),),
            )
            inserted = (
                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO fabric_request_nonces_v2(
                        worker_id, nonce, seen_at
                    ) VALUES (?, ?, ?)
                    """,
                    (worker_id, nonce, now),
                ).rowcount
                == 1
            )
            self.connection.commit()
            return inserted
        except BaseException:
            self.connection.rollback()
            raise

    def task_row(self, task_id: str) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM fabric_tasks_v2 WHERE task_id=?",
            (task_id,),
        ).fetchone()

    def unpublished_events(self, *, limit: int = 100) -> tuple[sqlite3.Row, ...]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("limit must be positive")
        return tuple(
            self.connection.execute(
                """
                SELECT *
                FROM fabric_outbox_v2
                WHERE published_at IS NULL
                ORDER BY created_at, event_id
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        )

    def mark_event_published(self, event_id: str) -> None:
        now = self._now()
        with self.connection:
            changed = self.connection.execute(
                """
                UPDATE fabric_outbox_v2
                SET published_at=?
                WHERE event_id=? AND published_at IS NULL
                """,
                (now, event_id),
            ).rowcount
        if changed not in {0, 1}:
            raise RuntimeError("unexpected outbox publication update")

    def consume_event(self, consumer_id: str, event_id: str) -> bool:
        if not consumer_id.strip() or not event_id.strip():
            raise ValueError("consumer_id and event_id are required")
        now = self._now()
        with self.connection:
            return (
                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO fabric_inbox_v2(
                        consumer_id, event_id, consumed_at
                    ) VALUES (?, ?, ?)
                    """,
                    (consumer_id, event_id, now),
                ).rowcount
                == 1
            )
