"""PostgreSQL production authority backend for Fabric v2.

The module imports psycopg lazily so the default single-node installation does
not require distributed extras.
"""

from __future__ import annotations

import math
import uuid
from datetime import datetime, timezone

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
)
from .schema import POSTGRES_CLAIM_SQL, POSTGRES_DDL
from .store import (
    BatchConflictError,
    BatchSequenceError,
    StaleLeaseError,
    WorkerRejectedError,
)


def _require_psycopg():
    try:
        import psycopg
        from psycopg.rows import dict_row
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - optional production extra
        raise RuntimeError(
            "PostgreSQL Fabric backend requires the distributed optional dependency"
        ) from exc
    return psycopg, dict_row, Jsonb


def _epoch_seconds(value: datetime) -> float:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.timestamp()


def _work_from_row(row: dict[str, object]) -> WorkSpec:
    required_raw = row["required_capabilities"]
    required = tuple(FabricCapability(str(item)) for item in required_raw)
    return WorkSpec(
        work_class=FabricWorkClass(str(row["work_class"])),
        producer=str(row["producer"]),
        algorithm_version=str(row["algorithm_version"]),
        partition_key=str(row["partition_key"]),
        input_identity=str(row["input_identity"]),
        coverage=dict(row["coverage"]),
        required_capabilities=required,
        priority=float(row["priority"]),
        queue=str(row["queue_name"]),
        max_attempts=int(row["max_attempts"]),
        provider=None if row["provider"] is None else str(row["provider"]),
        min_memory_bytes=int(row["min_memory_bytes"]),
        network_class=(
            None if row["network_class"] is None else str(row["network_class"])
        ),
    )


class PostgresFabricStore:
    """Production multi-host Fabric authority."""

    def __init__(self, dsn: str) -> None:
        if not isinstance(dsn, str) or not dsn.strip():
            raise ValueError("PostgreSQL DSN must be non-empty")
        psycopg, dict_row, Jsonb = _require_psycopg()
        self._psycopg = psycopg
        self._Jsonb = Jsonb
        self.connection = psycopg.connect(dsn, row_factory=dict_row)

    def close(self) -> None:
        self.connection.close()

    def initialize(self) -> None:
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(POSTGRES_DDL)

    def _emit(
        self,
        cursor,
        *,
        topic: str,
        aggregate_type: str,
        aggregate_id: str,
        aggregate_sequence: int,
        payload: dict[str, object],
    ) -> None:
        cursor.execute(
            """
            INSERT INTO fabric_outbox_v2(
                event_id, topic, aggregate_type, aggregate_id,
                aggregate_sequence, payload
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                uuid.uuid4(),
                topic,
                aggregate_type,
                aggregate_id,
                aggregate_sequence,
                self._Jsonb(payload),
            ),
        )

    def register_worker(self, worker: WorkerDescriptor) -> None:
        if worker.protocol_version != FABRIC_PROTOCOL_VERSION:
            raise WorkerRejectedError(
                f"unsupported worker protocol: {worker.protocol_version}"
            )
        descriptor = {
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
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO fabric_workers_v2(
                        worker_id, descriptor, protocol_version, edition,
                        last_heartbeat, revoked
                    )
                    VALUES (%s, %s, %s, %s, clock_timestamp(), FALSE)
                    ON CONFLICT(worker_id) DO UPDATE SET
                        descriptor=excluded.descriptor,
                        protocol_version=excluded.protocol_version,
                        edition=excluded.edition,
                        last_heartbeat=clock_timestamp()
                    """,
                    (
                        worker.worker_id,
                        self._Jsonb(descriptor),
                        worker.protocol_version,
                        worker.edition,
                    ),
                )

    def heartbeat(self, worker_id: str) -> None:
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE fabric_workers_v2
                    SET last_heartbeat=clock_timestamp()
                    WHERE worker_id=%s AND revoked=FALSE
                    """,
                    (worker_id,),
                )
                if cursor.rowcount != 1:
                    raise WorkerRejectedError(
                        f"unknown or revoked worker: {worker_id}"
                    )

    def submit_work(self, work: WorkSpec) -> str:
        task_id = uuid.uuid4()
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO fabric_tasks_v2(
                        task_id, work_key, work_class, producer,
                        algorithm_version, partition_key, input_identity,
                        coverage, required_capabilities, priority, queue_name,
                        max_attempts, provider, min_memory_bytes, network_class,
                        state
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s::text[],
                        %s, %s, %s, %s, %s, %s, 'READY'
                    )
                    ON CONFLICT(work_key) DO NOTHING
                    RETURNING task_id
                    """,
                    (
                        task_id,
                        work.work_key,
                        work.work_class.value,
                        work.producer,
                        work.algorithm_version,
                        work.partition_key,
                        work.input_identity,
                        self._Jsonb(dict(work.coverage)),
                        [item.value for item in work.required_capabilities],
                        float(work.priority),
                        work.queue,
                        work.max_attempts,
                        work.provider,
                        work.min_memory_bytes,
                        work.network_class,
                    ),
                )
                inserted = cursor.fetchone()
                if inserted is None:
                    cursor.execute(
                        "SELECT task_id FROM fabric_tasks_v2 WHERE work_key=%s",
                        (work.work_key,),
                    )
                    existing = cursor.fetchone()
                    if existing is None:
                        raise RuntimeError(
                            "fabric work disappeared after conflict admission"
                        )
                    return str(existing["task_id"])

                durable_id = str(inserted["task_id"])
                capability = (
                    work.required_capabilities[0].value.lower()
                    if work.required_capabilities
                    else "generic"
                )
                self._emit(
                    cursor,
                    topic=f"fabric.task.ready.{capability}",
                    aggregate_type="task",
                    aggregate_id=durable_id,
                    aggregate_sequence=0,
                    payload={"task_id": durable_id, "work_key": work.work_key},
                )
                return durable_id

    def _worker_descriptor(self, cursor, worker_id: str) -> WorkerDescriptor:
        cursor.execute(
            """
            SELECT descriptor, protocol_version, revoked
            FROM fabric_workers_v2
            WHERE worker_id=%s
            FOR SHARE
            """,
            (worker_id,),
        )
        row = cursor.fetchone()
        if (
            row is None
            or bool(row["revoked"])
            or str(row["protocol_version"]) != FABRIC_PROTOCOL_VERSION
        ):
            raise WorkerRejectedError(f"unknown or revoked worker: {worker_id}")
        data = dict(row["descriptor"])
        return WorkerDescriptor(
            worker_id=str(data["worker_id"]),
            region=str(data["region"]),
            runtime_class=str(data["runtime_class"]),
            architecture=str(data["architecture"]),
            network_class=str(data["network_class"]),
            cpu_count=int(data["cpu_count"]),
            memory_bytes=int(data["memory_bytes"]),
            capabilities=tuple(
                FabricCapability(item) for item in data["capabilities"]
            ),
            allowed_providers=tuple(str(item) for item in data["allowed_providers"]),
            max_concurrency=int(data["max_concurrency"]),
            labels={str(k): str(v) for k, v in dict(data.get("labels", {})).items()},
            protocol_version=str(data["protocol_version"]),
            edition=str(data["edition"]),
        )

    def claim(
        self,
        worker_id: str,
        *,
        queue: str = "default",
        lease_seconds: float = 60.0,
    ) -> LeaseToken | None:
        if not queue.strip():
            raise ValueError("queue must be non-empty")
        if not math.isfinite(float(lease_seconds)) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be finite and positive")
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                worker = self._worker_descriptor(cursor, worker_id)
                cursor.execute(
                    """
                    UPDATE fabric_tasks_v2
                    SET state = CASE
                            WHEN attempt >= max_attempts THEN 'FAILED'
                            ELSE 'READY'
                        END,
                        lease_owner=NULL,
                        lease_deadline=NULL,
                        available_at=clock_timestamp(),
                        updated_at=clock_timestamp(),
                        last_error='lease expired'
                    WHERE state='LEASED'
                      AND lease_deadline <= clock_timestamp()
                    """
                )
                cursor.execute(
                    POSTGRES_CLAIM_SQL,
                    {
                        "queue": queue,
                        "capabilities": [
                            item.value for item in worker.capabilities
                        ],
                        "memory_bytes": worker.memory_bytes,
                        "network_class": worker.network_class,
                        "allowed_providers": list(worker.allowed_providers),
                        "worker_id": worker_id,
                        "lease_seconds": float(lease_seconds),
                    },
                )
                row = cursor.fetchone()
                if row is None:
                    cursor.execute(
                        """
                        UPDATE fabric_workers_v2
                        SET last_heartbeat=clock_timestamp()
                        WHERE worker_id=%s
                        """,
                        (worker_id,),
                    )
                    return None
                cursor.execute(
                    """
                    UPDATE fabric_workers_v2
                    SET last_heartbeat=clock_timestamp()
                    WHERE worker_id=%s
                    """,
                    (worker_id,),
                )
                work = _work_from_row(row)
                return LeaseToken(
                    task_id=str(row["task_id"]),
                    work_key=str(row["work_key"]),
                    worker_id=worker_id,
                    lease_epoch=int(row["lease_epoch"]),
                    lease_deadline=_epoch_seconds(row["lease_deadline"]),
                    attempt=int(row["attempt"]),
                    work=work,
                    cursor=(
                        None
                        if row["cursor"] is None
                        else dict(row["cursor"])
                    ),
                    next_sequence_no=int(row["next_sequence_no"]),
                )

    def _lock_current_lease(self, cursor, lease: LeaseToken) -> dict[str, object]:
        cursor.execute(
            """
            SELECT *
            FROM fabric_tasks_v2
            WHERE task_id=%s
            FOR UPDATE
            """,
            (lease.task_id,),
        )
        row = cursor.fetchone()
        if (
            row is None
            or str(row["state"]) != FabricTaskState.LEASED.value
            or str(row["lease_owner"]) != lease.worker_id
            or int(row["lease_epoch"]) != lease.lease_epoch
            or row["lease_deadline"] is None
            or row["lease_deadline"] <= datetime.now(timezone.utc)
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
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                self._lock_current_lease(cursor, lease)
                cursor.execute(
                    """
                    UPDATE fabric_tasks_v2
                    SET lease_deadline =
                            clock_timestamp() + (%s * interval '1 second'),
                        updated_at=clock_timestamp()
                    WHERE task_id=%s
                    RETURNING lease_deadline
                    """,
                    (float(lease_seconds), lease.task_id),
                )
                row = cursor.fetchone()
                assert row is not None
                return LeaseToken(
                    task_id=lease.task_id,
                    work_key=lease.work_key,
                    worker_id=lease.worker_id,
                    lease_epoch=lease.lease_epoch,
                    lease_deadline=_epoch_seconds(row["lease_deadline"]),
                    attempt=lease.attempt,
                    work=lease.work,
                    cursor=lease.cursor,
                    next_sequence_no=lease.next_sequence_no,
                )

    def commit_batch(self, lease: LeaseToken, batch: ResultBatch) -> bool:
        if batch.task_id != lease.task_id or batch.lease_epoch != lease.lease_epoch:
            raise StaleLeaseError("result batch does not match lease identity")
        payload = {
            "results": list(batch.results),
            "cursor_after": batch.cursor_after,
        }
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                row = self._lock_current_lease(cursor, lease)
                cursor.execute(
                    """
                    SELECT payload_digest
                    FROM fabric_result_batches_v2
                    WHERE task_id=%s AND sequence_no=%s
                    """,
                    (batch.task_id, batch.sequence_no),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    if str(existing["payload_digest"]) != batch.payload_digest:
                        raise BatchConflictError(
                            "result batch sequence replayed with different payload"
                        )
                    return False
                expected = int(row["next_sequence_no"])
                if batch.sequence_no != expected:
                    raise BatchSequenceError(
                        f"expected sequence {expected}, got {batch.sequence_no}"
                    )
                cursor.execute(
                    """
                    INSERT INTO fabric_result_batches_v2(
                        task_id, sequence_no, lease_epoch, payload,
                        payload_digest, cursor_after
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        batch.task_id,
                        batch.sequence_no,
                        batch.lease_epoch,
                        self._Jsonb(payload),
                        batch.payload_digest,
                        (
                            None
                            if batch.cursor_after is None
                            else self._Jsonb(dict(batch.cursor_after))
                        ),
                    ),
                )
                cursor.execute(
                    """
                    UPDATE fabric_tasks_v2
                    SET cursor=%s, next_sequence_no=next_sequence_no+1,
                        updated_at=clock_timestamp()
                    WHERE task_id=%s
                    """,
                    (
                        None
                        if batch.cursor_after is None
                        else self._Jsonb(dict(batch.cursor_after)),
                        batch.task_id,
                    ),
                )
                self._emit(
                    cursor,
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
                )
                return True

    def complete(self, lease: LeaseToken) -> None:
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                row = self._lock_current_lease(cursor, lease)
                sequence = int(row["next_sequence_no"])
                cursor.execute(
                    """
                    UPDATE fabric_tasks_v2
                    SET state='SUCCEEDED', lease_owner=NULL,
                        lease_deadline=NULL, updated_at=clock_timestamp()
                    WHERE task_id=%s
                    """,
                    (lease.task_id,),
                )
                self._emit(
                    cursor,
                    topic="fabric.task.completed",
                    aggregate_type="task",
                    aggregate_id=lease.task_id,
                    aggregate_sequence=sequence,
                    payload={
                        "task_id": lease.task_id,
                        "work_key": lease.work_key,
                        "lease_epoch": lease.lease_epoch,
                    },
                )

    def fail(
        self,
        lease: LeaseToken,
        *,
        error: str,
        retryable: bool,
        retry_delay_seconds: float = 0.0,
    ) -> None:
        if not error.strip():
            raise ValueError("error must be non-empty")
        if not math.isfinite(float(retry_delay_seconds)) or retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must be finite and non-negative")
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                row = self._lock_current_lease(cursor, lease)
                retry = bool(
                    retryable and int(row["attempt"]) < int(row["max_attempts"])
                )
                state = "READY" if retry else "FAILED"
                cursor.execute(
                    """
                    UPDATE fabric_tasks_v2
                    SET state=%s, lease_owner=NULL, lease_deadline=NULL,
                        available_at=clock_timestamp()
                            + (%s * interval '1 second'),
                        last_error=%s, updated_at=clock_timestamp()
                    WHERE task_id=%s
                    """,
                    (
                        state,
                        float(retry_delay_seconds) if retry else 0.0,
                        error.strip(),
                        lease.task_id,
                    ),
                )
                self._emit(
                    cursor,
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
                )

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
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO fabric_provider_budgets_v2(
                        provider, requests_per_second, max_global_inflight,
                        require_qualified_region, updated_at
                    ) VALUES (%s, %s, %s, %s, clock_timestamp())
                    ON CONFLICT(provider) DO UPDATE SET
                        requests_per_second=excluded.requests_per_second,
                        max_global_inflight=excluded.max_global_inflight,
                        require_qualified_region=excluded.require_qualified_region,
                        updated_at=clock_timestamp()
                    """,
                    (
                        provider.strip(),
                        float(requests_per_second),
                        max_global_inflight,
                        bool(require_qualified_region),
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
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO fabric_provider_regions_v2(
                        provider, region, qualified, updated_at
                    ) VALUES (%s, %s, %s, clock_timestamp())
                    ON CONFLICT(provider, region) DO UPDATE SET
                        qualified=excluded.qualified,
                        updated_at=clock_timestamp()
                    """,
                    (provider, region, bool(qualified)),
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
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                self._lock_current_lease(cursor, lease)
                worker = self._worker_descriptor(cursor, lease.worker_id)
                if provider not in worker.allowed_providers:
                    raise WorkerRejectedError(
                        f"worker is not allowed to access provider: {provider}"
                    )
                cursor.execute(
                    """
                    SELECT *
                    FROM fabric_provider_budgets_v2
                    WHERE provider=%s
                    FOR UPDATE
                    """,
                    (provider,),
                )
                budget = cursor.fetchone()
                if budget is None:
                    raise KeyError(f"unknown provider budget: {provider}")
                if bool(budget["require_qualified_region"]):
                    cursor.execute(
                        """
                        SELECT qualified
                        FROM fabric_provider_regions_v2
                        WHERE provider=%s AND region=%s
                        """,
                        (provider, worker.region),
                    )
                    region = cursor.fetchone()
                    if region is None or not bool(region["qualified"]):
                        return None
                cursor.execute(
                    """
                    UPDATE fabric_provider_permits_v2
                    SET active=FALSE
                    WHERE provider=%s AND active=TRUE
                      AND expires_at <= clock_timestamp()
                    """,
                    (provider,),
                )
                cursor.execute(
                    """
                    SELECT COUNT(*) AS n
                    FROM fabric_provider_permits_v2
                    WHERE provider=%s AND active=TRUE
                      AND expires_at > clock_timestamp()
                    """,
                    (provider,),
                )
                active = int(cursor.fetchone()["n"])
                cursor.execute(
                    """
                    SELECT
                        clock_timestamp() >= next_request_at AS rps_ready,
                        clock_timestamp() >= cooldown_until AS cooldown_ready
                    FROM fabric_provider_budgets_v2
                    WHERE provider=%s
                    """,
                    (provider,),
                )
                readiness = cursor.fetchone()
                if (
                    active >= int(budget["max_global_inflight"])
                    or not bool(readiness["rps_ready"])
                    or not bool(readiness["cooldown_ready"])
                ):
                    return None
                permit_id = uuid.uuid4()
                cursor.execute(
                    """
                    UPDATE fabric_provider_budgets_v2
                    SET next_request_at = clock_timestamp()
                        + (%s * interval '1 second'),
                        updated_at=clock_timestamp()
                    WHERE provider=%s
                    """,
                    (
                        float(allowed_requests)
                        / float(budget["requests_per_second"]),
                        provider,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO fabric_provider_permits_v2(
                        permit_id, provider, worker_id, task_id, lease_epoch,
                        allowed_requests, expires_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        clock_timestamp() + (%s * interval '1 second')
                    )
                    RETURNING expires_at
                    """,
                    (
                        permit_id,
                        provider,
                        lease.worker_id,
                        lease.task_id,
                        lease.lease_epoch,
                        allowed_requests,
                        float(ttl_seconds),
                    ),
                )
                row = cursor.fetchone()
                assert row is not None
                return ProviderPermit(
                    permit_id=str(permit_id),
                    provider=provider,
                    worker_id=lease.worker_id,
                    task_id=lease.task_id,
                    lease_epoch=lease.lease_epoch,
                    allowed_requests=allowed_requests,
                    expires_at=_epoch_seconds(row["expires_at"]),
                )

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
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT p.*, w.descriptor
                    FROM fabric_provider_permits_v2 AS p
                    JOIN fabric_workers_v2 AS w ON w.worker_id=p.worker_id
                    WHERE p.permit_id=%s
                    FOR UPDATE OF p
                    """,
                    (permit.permit_id,),
                )
                row = cursor.fetchone()
                if row is None:
                    raise KeyError(permit.permit_id)
                if (
                    str(row["worker_id"]) != permit.worker_id
                    or str(row["task_id"]) != permit.task_id
                    or int(row["lease_epoch"]) != permit.lease_epoch
                ):
                    raise StaleLeaseError("provider permit identity mismatch")
                if not bool(row["active"]):
                    return
                worker = dict(row["descriptor"])
                region = str(worker["region"])
                success = status_code is not None and 200 <= status_code < 400
                cursor.execute(
                    """
                    UPDATE fabric_provider_permits_v2
                    SET active=FALSE, status_code=%s, response_bytes=%s
                    WHERE permit_id=%s
                    """,
                    (status_code, response_bytes, permit.permit_id),
                )
                cursor.execute(
                    """
                    INSERT INTO fabric_provider_regions_v2(
                        provider, region, qualified, samples, successes,
                        throttles, failures, updated_at
                    ) VALUES (
                        %s, %s, FALSE, 1, %s, %s, %s, clock_timestamp()
                    )
                    ON CONFLICT(provider, region) DO UPDATE SET
                        samples=fabric_provider_regions_v2.samples+1,
                        successes=fabric_provider_regions_v2.successes
                            + excluded.successes,
                        throttles=fabric_provider_regions_v2.throttles
                            + excluded.throttles,
                        failures=fabric_provider_regions_v2.failures
                            + excluded.failures,
                        updated_at=clock_timestamp()
                    """,
                    (
                        permit.provider,
                        region,
                        int(success),
                        int(bool(throttled)),
                        int(not success and not throttled),
                    ),
                )
                if throttled and cooldown_seconds > 0:
                    cursor.execute(
                        """
                        UPDATE fabric_provider_budgets_v2
                        SET cooldown_until=GREATEST(
                                cooldown_until,
                                clock_timestamp()
                                    + (%s * interval '1 second')
                            ),
                            updated_at=clock_timestamp()
                        WHERE provider=%s
                        """,
                        (float(cooldown_seconds), permit.provider),
                    )

    def unpublished_events(self, *, limit: int = 100) -> tuple[dict[str, object], ...]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("limit must be positive")
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM fabric_outbox_v2
                WHERE published_at IS NULL
                ORDER BY created_at, event_id
                LIMIT %s
                """,
                (limit,),
            )
            return tuple(dict(row) for row in cursor.fetchall())

    def mark_event_published(self, event_id: str) -> None:
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE fabric_outbox_v2
                    SET published_at=clock_timestamp()
                    WHERE event_id=%s AND published_at IS NULL
                    """,
                    (event_id,),
                )

    def consume_event(self, consumer_id: str, event_id: str) -> bool:
        if not consumer_id.strip() or not event_id.strip():
            raise ValueError("consumer_id and event_id are required")
        with self.connection.transaction():
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO fabric_inbox_v2(consumer_id, event_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (consumer_id, event_id),
                )
                return cursor.rowcount == 1
