"""PostgreSQL production authority backend for Creeper Fabric v2."""

from __future__ import annotations

import hashlib
import json
import math
import time
from uuid import uuid4

from creeper.distributed.authority_store import (
    BatchConflictError,
    BatchSequenceError,
    FabricProtocolMismatchError,
    ProviderAccessDeniedError,
    StaleLeaseError,
    WorkerEgressBudgetExceededError,
    WorkerRejectedError,
)
from creeper.distributed.edition import FABRIC_PROTOCOL_VERSION
from creeper.distributed.identity import canonical_json
from creeper.distributed.models import (
    ProviderPermit,
    ResultBatch,
    TaskClass,
    TaskLease,
    WorkDefinition,
    WorkerDescriptor,
)


def _json(value) -> str:
    return canonical_json(value)


class PostgresAuthorityStore:
    """Multi-authority backend using row locks and SKIP LOCKED."""

    def __init__(self, dsn: str, *, clock=time.time, emit_outbox: bool = True) -> None:
        if not isinstance(emit_outbox, bool):
            raise ValueError("emit_outbox must be a boolean")
        if not dsn.startswith(("postgresql://", "postgres://")):
            raise ValueError("PostgreSQL DSN is required")
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "PostgreSQL Fabric requires the optional 'postgres' dependency"
            ) from exc
        self.psycopg = psycopg
        self.clock = clock
        self.emit_outbox = emit_outbox
        self.connection = psycopg.connect(dsn, row_factory=dict_row)
        self._initialize()

    def _initialize(self) -> None:
        schema = """
        CREATE TABLE IF NOT EXISTS fabric_workers (
            worker_id TEXT PRIMARY KEY,
            worker_instance_id TEXT NOT NULL,
            runtime_class TEXT NOT NULL,
            region TEXT NOT NULL,
            architecture TEXT NOT NULL,
            memory_bytes BIGINT NOT NULL CHECK(memory_bytes >= 0),
            cpu_count INTEGER NOT NULL CHECK(cpu_count >= 1),
            network_class TEXT NOT NULL,
            capabilities_json JSONB NOT NULL,
            producers_json JSONB NOT NULL,
            allowed_providers_json JSONB NOT NULL,
            daily_egress_budget_bytes BIGINT NOT NULL DEFAULT 0
                CHECK(daily_egress_budget_bytes >= 0),
            protocol_version TEXT NOT NULL,
            edition_version TEXT NOT NULL,
            last_heartbeat DOUBLE PRECISION NOT NULL,
            revoked BOOLEAN NOT NULL DEFAULT FALSE
        );

        CREATE TABLE IF NOT EXISTS fabric_work (
            task_id TEXT PRIMARY KEY,
            work_key TEXT NOT NULL UNIQUE,
            producer TEXT NOT NULL,
            task_class TEXT NOT NULL,
            input_identity TEXT NOT NULL,
            payload_json JSONB NOT NULL,
            partition_key TEXT NOT NULL,
            algorithm_version TEXT NOT NULL,
            required_capabilities_json JSONB NOT NULL,
            required_providers_json JSONB NOT NULL,
            priority DOUBLE PRECISION NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL CHECK(max_attempts >= 1),
            not_before DOUBLE PRECISION NOT NULL DEFAULT 0,
            state TEXT NOT NULL,
            lease_owner TEXT,
            lease_owner_instance TEXT,
            lease_generation BIGINT NOT NULL DEFAULT 0,
            lease_deadline DOUBLE PRECISION,
            attempt INTEGER NOT NULL DEFAULT 0,
            cursor TEXT,
            next_sequence_no BIGINT NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at DOUBLE PRECISION NOT NULL,
            updated_at DOUBLE PRECISION NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_fabric_work_claim
            ON fabric_work(state, not_before, priority DESC, created_at);

        CREATE TABLE IF NOT EXISTS fabric_result_batches (
            batch_id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL REFERENCES fabric_work(task_id),
            generation BIGINT NOT NULL,
            sequence_no BIGINT NOT NULL,
            payload_json JSONB NOT NULL,
            payload_digest TEXT NOT NULL,
            cursor_after TEXT,
            final BOOLEAN NOT NULL DEFAULT FALSE,
            committed_at DOUBLE PRECISION NOT NULL,
            consumed_at DOUBLE PRECISION,
            consume_attempts INTEGER NOT NULL DEFAULT 0,
            consume_error TEXT,
            quarantined BOOLEAN NOT NULL DEFAULT FALSE,
            UNIQUE(task_id, sequence_no)
        );
        CREATE INDEX IF NOT EXISTS idx_fabric_result_unconsumed
            ON fabric_result_batches(consumed_at, committed_at);

        CREATE TABLE IF NOT EXISTS fabric_artifacts (
            artifact_id TEXT PRIMARY KEY,
            sha256 TEXT NOT NULL UNIQUE,
            uri TEXT NOT NULL,
            size_bytes BIGINT NOT NULL CHECK(size_bytes >= 0),
            content_type TEXT NOT NULL,
            compression TEXT NOT NULL,
            metadata_json JSONB NOT NULL,
            first_task_id TEXT NOT NULL REFERENCES fabric_work(task_id),
            first_seen_at DOUBLE PRECISION NOT NULL
        );

        CREATE TABLE IF NOT EXISTS fabric_batch_artifacts (
            batch_id TEXT NOT NULL REFERENCES fabric_result_batches(batch_id),
            artifact_id TEXT NOT NULL REFERENCES fabric_artifacts(artifact_id),
            PRIMARY KEY(batch_id, artifact_id)
        );

        CREATE TABLE IF NOT EXISTS fabric_outbox (
            event_id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            aggregate_id TEXT NOT NULL,
            payload_json JSONB NOT NULL,
            created_at DOUBLE PRECISION NOT NULL,
            published_at DOUBLE PRECISION
        );
        CREATE INDEX IF NOT EXISTS idx_fabric_outbox_pending
            ON fabric_outbox(published_at, created_at);

        CREATE TABLE IF NOT EXISTS fabric_request_nonces (
            worker_id TEXT NOT NULL,
            nonce TEXT NOT NULL,
            seen_at DOUBLE PRECISION NOT NULL,
            PRIMARY KEY(worker_id, nonce)
        );

        CREATE TABLE IF NOT EXISTS fabric_provider_budgets (
            provider TEXT PRIMARY KEY,
            requests_per_second DOUBLE PRECISION NOT NULL
                CHECK(requests_per_second > 0),
            max_global_inflight INTEGER NOT NULL
                CHECK(max_global_inflight >= 1),
            require_qualified_region BOOLEAN NOT NULL DEFAULT TRUE,
            next_request_at DOUBLE PRECISION NOT NULL DEFAULT 0,
            cooldown_until DOUBLE PRECISION NOT NULL DEFAULT 0,
            updated_at DOUBLE PRECISION NOT NULL
        );

        CREATE TABLE IF NOT EXISTS fabric_provider_regions (
            provider TEXT NOT NULL,
            region TEXT NOT NULL,
            state TEXT NOT NULL,
            samples INTEGER NOT NULL DEFAULT 0,
            successes INTEGER NOT NULL DEFAULT 0,
            timeouts INTEGER NOT NULL DEFAULT 0,
            throttles INTEGER NOT NULL DEFAULT 0,
            policy_blocks INTEGER NOT NULL DEFAULT 0,
            total_latency_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
            response_bytes BIGINT NOT NULL DEFAULT 0,
            updated_at DOUBLE PRECISION NOT NULL,
            PRIMARY KEY(provider, region)
        );

        CREATE TABLE IF NOT EXISTS fabric_provider_permits (
            permit_id TEXT PRIMARY KEY,
            request_id TEXT NOT NULL,
            provider TEXT NOT NULL REFERENCES fabric_provider_budgets(provider),
            worker_id TEXT NOT NULL REFERENCES fabric_workers(worker_id),
            worker_instance_id TEXT NOT NULL,
            task_id TEXT NOT NULL REFERENCES fabric_work(task_id),
            generation BIGINT NOT NULL,
            allowed_requests INTEGER NOT NULL CHECK(allowed_requests >= 1),
            max_inflight INTEGER NOT NULL CHECK(max_inflight >= 1),
            expires_at DOUBLE PRECISION NOT NULL,
            active BOOLEAN NOT NULL DEFAULT TRUE,
            issued_at DOUBLE PRECISION NOT NULL,
            status_code INTEGER,
            UNIQUE(worker_id, request_id)
        );
        CREATE INDEX IF NOT EXISTS idx_fabric_permit_active
            ON fabric_provider_permits(provider, active, expires_at);

        CREATE TABLE IF NOT EXISTS fabric_worker_egress_daily (
            worker_id TEXT NOT NULL REFERENCES fabric_workers(worker_id),
            day_key INTEGER NOT NULL,
            response_bytes BIGINT NOT NULL DEFAULT 0,
            updated_at DOUBLE PRECISION NOT NULL,
            PRIMARY KEY(worker_id, day_key)
        );
        """
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                cur.execute(schema)

    def close(self) -> None:
        self.connection.close()

    def _emit(self, cur, event_type: str, aggregate_id: str, payload: dict) -> None:
        if not self.emit_outbox:
            return
        cur.execute(
            """
            INSERT INTO fabric_outbox(
                event_id,event_type,aggregate_id,payload_json,created_at
            ) VALUES (%s,%s,%s,%s::jsonb,%s)
            """,
            (
                uuid4().hex,
                event_type,
                aggregate_id,
                _json(payload),
                float(self.clock()),
            ),
        )

    def consume_request_nonce(
        self,
        worker_id: str,
        nonce: str,
        *,
        retention_seconds: float = 900.0,
    ) -> bool:
        now=float(self.clock())
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                cur.execute(
                    "DELETE FROM fabric_request_nonces WHERE seen_at < %s",
                    (now-float(retention_seconds),),
                )
                cur.execute(
                    """
                    INSERT INTO fabric_request_nonces(worker_id,nonce,seen_at)
                    VALUES (%s,%s,%s)
                    ON CONFLICT DO NOTHING
                    """,
                    (worker_id,nonce,now),
                )
                return cur.rowcount == 1

    def register_worker(self, descriptor: WorkerDescriptor) -> None:
        if descriptor.protocol_version != FABRIC_PROTOCOL_VERSION:
            raise FabricProtocolMismatchError("worker protocol mismatch")
        now=float(self.clock())
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO fabric_workers(
                        worker_id,worker_instance_id,runtime_class,region,
                        architecture,memory_bytes,cpu_count,network_class,
                        capabilities_json,producers_json,allowed_providers_json,
                        daily_egress_budget_bytes,protocol_version,
                        edition_version,last_heartbeat,revoked
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,
                        %s::jsonb,%s::jsonb,%s::jsonb,%s,%s,%s,%s,FALSE
                    )
                    ON CONFLICT(worker_id) DO UPDATE SET
                        worker_instance_id=EXCLUDED.worker_instance_id,
                        runtime_class=EXCLUDED.runtime_class,
                        region=EXCLUDED.region,
                        architecture=EXCLUDED.architecture,
                        memory_bytes=EXCLUDED.memory_bytes,
                        cpu_count=EXCLUDED.cpu_count,
                        network_class=EXCLUDED.network_class,
                        capabilities_json=EXCLUDED.capabilities_json,
                        producers_json=EXCLUDED.producers_json,
                        allowed_providers_json=EXCLUDED.allowed_providers_json,
                        daily_egress_budget_bytes=EXCLUDED.daily_egress_budget_bytes,
                        protocol_version=EXCLUDED.protocol_version,
                        edition_version=EXCLUDED.edition_version,
                        last_heartbeat=EXCLUDED.last_heartbeat,
                        revoked=FALSE
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
                cur.execute(
                    """
                    UPDATE fabric_work
                    SET state='PENDING',lease_owner=NULL,
                        lease_owner_instance=NULL,lease_deadline=NULL,
                        updated_at=%s
                    WHERE state='LEASED' AND lease_owner=%s
                      AND lease_owner_instance<>%s
                    """,
                    (now,descriptor.worker_id,descriptor.worker_instance_id),
                )
                self._emit(
                    cur,
                    "WORKER_REGISTERED",
                    descriptor.worker_id,
                    {
                        "worker_id":descriptor.worker_id,
                        "worker_instance_id":descriptor.worker_instance_id,
                    },
                )

    def _worker(self, cur, worker_id: str, instance: str, *, lock=False):
        suffix=" FOR UPDATE" if lock else ""
        cur.execute(
            "SELECT * FROM fabric_workers WHERE worker_id=%s"+suffix,
            (worker_id,),
        )
        row=cur.fetchone()
        if (
            row is None
            or bool(row["revoked"])
            or str(row["worker_instance_id"]) != instance
        ):
            raise WorkerRejectedError("unknown, revoked, or stale worker instance")
        return row

    def heartbeat(self, worker_id: str, worker_instance_id: str) -> None:
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                self._worker(cur,worker_id,worker_instance_id)
                cur.execute(
                    "UPDATE fabric_workers SET last_heartbeat=%s WHERE worker_id=%s",
                    (float(self.clock()),worker_id),
                )

    def revoke_worker(self, worker_id: str) -> None:
        now=float(self.clock())
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                cur.execute(
                    """
                    UPDATE fabric_workers
                    SET revoked=TRUE,last_heartbeat=%s
                    WHERE worker_id=%s
                    """,
                    (now,worker_id),
                )
                if cur.rowcount != 1:
                    raise KeyError(worker_id)
                cur.execute(
                    """
                    UPDATE fabric_work
                    SET state='PENDING',lease_owner=NULL,
                        lease_owner_instance=NULL,lease_deadline=NULL,
                        updated_at=%s
                    WHERE state='LEASED' AND lease_owner=%s
                    """,
                    (now,worker_id),
                )
                self._emit(cur,"WORKER_REVOKED",worker_id,{"worker_id":worker_id})

    def admit_work(self, work: WorkDefinition) -> tuple[str,bool]:
        now=float(self.clock())
        task_id=uuid4().hex
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO fabric_work(
                        task_id,work_key,producer,task_class,input_identity,
                        payload_json,partition_key,algorithm_version,
                        required_capabilities_json,required_providers_json,
                        priority,max_attempts,not_before,state,created_at,updated_at
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s::jsonb,%s,%s,
                        %s::jsonb,%s::jsonb,%s,%s,%s,'PENDING',%s,%s
                    )
                    ON CONFLICT(work_key) DO NOTHING
                    RETURNING task_id
                    """,
                    (
                        task_id,work.work_key,work.producer,work.task_class.value,
                        work.input_identity,_json(dict(work.payload)),work.partition,
                        work.algorithm_version,_json(work.required_capabilities),
                        _json(work.required_providers),work.priority,
                        work.max_attempts,work.not_before,now,now,
                    ),
                )
                row=cur.fetchone()
                if row is None:
                    cur.execute(
                        "SELECT task_id FROM fabric_work WHERE work_key=%s",
                        (work.work_key,),
                    )
                    return str(cur.fetchone()["task_id"]),False
                self._emit(
                    cur,"WORK_ADMITTED",task_id,
                    {"task_id":task_id,"work_key":work.work_key},
                )
                return task_id,True

    @staticmethod
    def _work(row) -> WorkDefinition:
        return WorkDefinition(
            producer=str(row["producer"]),
            task_class=TaskClass(str(row["task_class"])),
            input_identity=str(row["input_identity"]),
            payload=dict(row["payload_json"]),
            partition=str(row["partition_key"]),
            algorithm_version=str(row["algorithm_version"]),
            required_capabilities=tuple(row["required_capabilities_json"]),
            required_providers=tuple(row["required_providers_json"]),
            priority=float(row["priority"]),
            max_attempts=int(row["max_attempts"]),
            not_before=float(row["not_before"]),
        )

    def claim_work(
        self,
        worker_id: str,
        worker_instance_id: str,
        *,
        lease_seconds: float = 300.0,
    ) -> TaskLease | None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        now=float(self.clock())
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                worker=self._worker(cur,worker_id,worker_instance_id,lock=True)
                caps=_json(tuple(worker["capabilities_json"]))
                providers=_json(tuple(worker["allowed_providers_json"]))
                producers=_json(tuple(worker["producers_json"]))
                cur.execute(
                    """
                    SELECT *
                    FROM fabric_work
                    WHERE (
                            state IN ('PENDING','RETRY')
                            OR (state='LEASED' AND lease_deadline <= %s)
                          )
                      AND not_before <= %s
                      AND attempt < max_attempts
                      AND required_capabilities_json <@ %s::jsonb
                      AND required_providers_json <@ %s::jsonb
                      AND (
                            %s::jsonb = '[]'::jsonb
                            OR %s::jsonb ? producer
                          )
                    ORDER BY priority DESC,created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """,
                    (now,now,caps,providers,producers,producers),
                )
                row=cur.fetchone()
                if row is None:
                    return None
                generation=int(row["lease_generation"])+1
                attempt=int(row["attempt"])+1
                deadline=now+float(lease_seconds)
                cur.execute(
                    """
                    UPDATE fabric_work
                    SET state='LEASED',lease_owner=%s,lease_owner_instance=%s,
                        lease_generation=%s,lease_deadline=%s,attempt=%s,
                        updated_at=%s
                    WHERE task_id=%s
                    """,
                    (
                        worker_id,worker_instance_id,generation,deadline,
                        attempt,now,str(row["task_id"]),
                    ),
                )
                self._emit(
                    cur,"WORK_LEASED",str(row["task_id"]),
                    {
                        "task_id":str(row["task_id"]),
                        "worker_id":worker_id,
                        "worker_instance_id":worker_instance_id,
                        "generation":generation,
                    },
                )
                return TaskLease(
                    task_id=str(row["task_id"]),
                    work_key=str(row["work_key"]),
                    worker_id=worker_id,
                    worker_instance_id=worker_instance_id,
                    generation=generation,
                    lease_deadline=deadline,
                    attempt=attempt,
                    work=self._work(row),
                    cursor=None if row["cursor"] is None else str(row["cursor"]),
                    next_sequence_no=int(row["next_sequence_no"]),
                )

    def _lease(
        self,
        cur,
        task_id: str,
        worker_id: str,
        instance: str,
        generation: int,
    ):
        cur.execute(
            "SELECT * FROM fabric_work WHERE task_id=%s FOR UPDATE",
            (task_id,),
        )
        row=cur.fetchone()
        if (
            row is None
            or str(row["state"])!="LEASED"
            or str(row["lease_owner"])!=worker_id
            or str(row["lease_owner_instance"])!=instance
            or int(row["lease_generation"])!=generation
            or row["lease_deadline"] is None
            or float(row["lease_deadline"]) <= float(self.clock())
        ):
            raise StaleLeaseError("worker no longer owns active lease")
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
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                row=self._lease(
                    cur,task_id,worker_id,worker_instance_id,generation
                )
                deadline=float(self.clock())+float(lease_seconds)
                cur.execute(
                    "UPDATE fabric_work SET lease_deadline=%s,updated_at=%s WHERE task_id=%s",
                    (deadline,float(self.clock()),task_id),
                )
                return TaskLease(
                    task_id=task_id,work_key=str(row["work_key"]),
                    worker_id=worker_id,worker_instance_id=worker_instance_id,
                    generation=generation,lease_deadline=deadline,
                    attempt=int(row["attempt"]),work=self._work(row),
                    cursor=None if row["cursor"] is None else str(row["cursor"]),
                    next_sequence_no=int(row["next_sequence_no"]),
                )

    def _batch_payload(self,batch:ResultBatch)->str:
        return _json({
            "task_id":batch.task_id,
            "generation":batch.generation,
            "sequence_no":batch.sequence_no,
            "results":[dict(item) for item in batch.results],
            "artifacts":[
                {
                    "uri":a.uri,"sha256":a.sha256,"size_bytes":a.size_bytes,
                    "content_type":a.content_type,"compression":a.compression,
                    "metadata":dict(a.metadata),
                } for a in batch.artifacts
            ],
            "cursor_after":batch.cursor_after,
            "final":batch.final,
        })

    def commit_result_batch(
        self,
        batch:ResultBatch,
        *,
        worker_id:str,
        worker_instance_id:str,
    )->bool:
        payload=self._batch_payload(batch)
        digest=hashlib.sha256(payload.encode()).hexdigest()
        now=float(self.clock())
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                cur.execute(
                    "SELECT payload_digest FROM fabric_result_batches WHERE batch_id=%s",
                    (batch.batch_id,),
                )
                prior=cur.fetchone()
                if prior is not None:
                    if str(prior["payload_digest"])!=digest:
                        raise BatchConflictError("batch replay changed payload")
                    return False
                row=self._lease(
                    cur,batch.task_id,worker_id,worker_instance_id,batch.generation
                )
                if int(row["next_sequence_no"])!=batch.sequence_no:
                    raise BatchSequenceError(
                        f"expected {row['next_sequence_no']}, got {batch.sequence_no}"
                    )
                cur.execute(
                    """
                    INSERT INTO fabric_result_batches(
                        batch_id,task_id,generation,sequence_no,payload_json,
                        payload_digest,cursor_after,final,committed_at
                    ) VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s)
                    """,
                    (
                        batch.batch_id,batch.task_id,batch.generation,
                        batch.sequence_no,payload,digest,batch.cursor_after,
                        batch.final,now,
                    ),
                )
                for artifact in batch.artifacts:
                    cur.execute(
                        """
                        INSERT INTO fabric_artifacts(
                            artifact_id,sha256,uri,size_bytes,content_type,
                            compression,metadata_json,first_task_id,first_seen_at
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s)
                        ON CONFLICT(artifact_id) DO NOTHING
                        """,
                        (
                            artifact.artifact_id,artifact.sha256.lower(),
                            artifact.uri,artifact.size_bytes,
                            artifact.content_type,artifact.compression,
                            _json(dict(artifact.metadata)),batch.task_id,now,
                        ),
                    )
                    cur.execute(
                        """
                        INSERT INTO fabric_batch_artifacts(batch_id,artifact_id)
                        VALUES (%s,%s) ON CONFLICT DO NOTHING
                        """,
                        (batch.batch_id,artifact.artifact_id),
                    )
                cur.execute(
                    """
                    UPDATE fabric_work
                    SET cursor=%s,next_sequence_no=next_sequence_no+1,
                        state=%s,
                        lease_owner=CASE WHEN %s THEN NULL ELSE lease_owner END,
                        lease_owner_instance=CASE WHEN %s THEN NULL ELSE lease_owner_instance END,
                        lease_deadline=CASE WHEN %s THEN NULL ELSE lease_deadline END,
                        updated_at=%s
                    WHERE task_id=%s
                    """,
                    (
                        batch.cursor_after,
                        "COMPLETE" if batch.final else "LEASED",
                        batch.final,batch.final,batch.final,now,batch.task_id,
                    ),
                )
                self._emit(
                    cur,"RESULT_COMMITTED",batch.task_id,
                    {
                        "task_id":batch.task_id,
                        "batch_id":batch.batch_id,
                        "sequence_no":batch.sequence_no,
                        "final":batch.final,
                    },
                )
                return True

    def finish_task(
        self,task_id:str,*,worker_id:str,worker_instance_id:str,generation:int
    )->None:
        now=float(self.clock())
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                self._lease(cur,task_id,worker_id,worker_instance_id,generation)
                cur.execute(
                    """
                    UPDATE fabric_work
                    SET state='COMPLETE',lease_owner=NULL,
                        lease_owner_instance=NULL,lease_deadline=NULL,updated_at=%s
                    WHERE task_id=%s
                    """,
                    (now,task_id),
                )
                self._emit(cur,"WORK_COMPLETED",task_id,{"task_id":task_id})

    def fail_task(
        self,task_id:str,*,worker_id:str,worker_instance_id:str,
        generation:int,error:str,retryable:bool=True,
        retry_after_seconds:float=0.0,
    )->None:
        now=float(self.clock())
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                row=self._lease(
                    cur,task_id,worker_id,worker_instance_id,generation
                )
                can_retry=retryable and int(row["attempt"])<int(row["max_attempts"])
                state="RETRY" if can_retry else "DEAD"
                cur.execute(
                    """
                    UPDATE fabric_work
                    SET state=%s,lease_owner=NULL,lease_owner_instance=NULL,
                        lease_deadline=NULL,not_before=%s,last_error=%s,updated_at=%s
                    WHERE task_id=%s
                    """,
                    (
                        state,
                        now+(float(retry_after_seconds) if can_retry else 0.0),
                        error[:2000],now,task_id,
                    ),
                )
                self._emit(
                    cur,"WORK_FAILED",task_id,
                    {"task_id":task_id,"retryable":can_retry,"state":state},
                )

    def unconsumed_batches(self,*,limit:int=100):
        with self.connection.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM fabric_result_batches
                WHERE consumed_at IS NULL AND quarantined=FALSE
                ORDER BY committed_at,task_id,sequence_no
                LIMIT %s
                """,
                (limit,),
            )
            return tuple(cur.fetchall())

    def mark_batch_consumed(self,batch_id:str)->bool:
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                cur.execute(
                    """
                    UPDATE fabric_result_batches
                    SET consumed_at=COALESCE(consumed_at,%s)
                    WHERE batch_id=%s
                    """,
                    (float(self.clock()),batch_id),
                )
                return cur.rowcount==1

    def mark_batch_consume_failed(
        self,
        batch_id:str,
        error:str,
        *,
        max_attempts:int=20,
    )->bool:
        if max_attempts<1:
            raise ValueError("max_attempts must be positive")
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                cur.execute(
                    """
                    SELECT consume_attempts FROM fabric_result_batches
                    WHERE batch_id=%s AND consumed_at IS NULL
                    FOR UPDATE
                    """,
                    (batch_id,),
                )
                row=cur.fetchone()
                if row is None:
                    return False
                attempts=int(row["consume_attempts"])+1
                quarantined=attempts>=max_attempts
                cur.execute(
                    """
                    UPDATE fabric_result_batches
                    SET consume_attempts=%s,consume_error=%s,quarantined=%s
                    WHERE batch_id=%s
                    """,
                    (attempts,error[:2000],quarantined,batch_id),
                )
                return quarantined

    def pending_outbox(self,*,limit:int=100):
        with self.connection.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM fabric_outbox WHERE published_at IS NULL
                ORDER BY created_at,event_id LIMIT %s
                """,
                (limit,),
            )
            return tuple(cur.fetchall())

    def mark_outbox_published(self,event_id:str)->bool:
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                cur.execute(
                    """
                    UPDATE fabric_outbox SET published_at=COALESCE(published_at,%s)
                    WHERE event_id=%s
                    """,
                    (float(self.clock()),event_id),
                )
                return cur.rowcount==1

    def gc_transient_state(
        self,
        *,
        retention_seconds: float = 86400.0,
        limit: int = 50000,
    ) -> dict[str,int]:
        """Prune replay-safe transient rows while retaining WorkKey tombstones."""
        if (
            isinstance(retention_seconds,bool)
            or not isinstance(retention_seconds,(int,float))
            or not math.isfinite(float(retention_seconds))
            or retention_seconds <= 0
        ):
            raise ValueError("retention_seconds must be finite and positive")
        if isinstance(limit,bool) or not isinstance(limit,int) or limit < 1:
            raise ValueError("limit must be a positive integer")
        cutoff=float(self.clock())-float(retention_seconds)
        report={
            "provider_permits":0,
            "result_batches":0,
            "outbox_events":0,
            "request_nonces":0,
            "egress_days":0,
            "work_payloads_compacted":0,
        }
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                cur.execute(
                    """
                    SELECT permit_id
                    FROM fabric_provider_permits
                    WHERE active=FALSE AND expires_at<=%s
                    ORDER BY expires_at,permit_id
                    LIMIT %s
                    """,
                    (cutoff,limit),
                )
                permits=[str(row["permit_id"]) for row in cur.fetchall()]
                if permits:
                    cur.execute(
                        """
                        DELETE FROM fabric_provider_permits
                        WHERE permit_id=ANY(%s)
                        """,
                        (permits,),
                    )
                    report["provider_permits"]=cur.rowcount

                cur.execute(
                    """
                    SELECT batch_id
                    FROM fabric_result_batches
                    WHERE (
                        consumed_at IS NOT NULL AND consumed_at<=%s
                    ) OR (
                        quarantined=TRUE AND committed_at<=%s
                    )
                    ORDER BY committed_at,batch_id
                    LIMIT %s
                    """,
                    (cutoff,cutoff,limit),
                )
                batches=[str(row["batch_id"]) for row in cur.fetchall()]
                if batches:
                    cur.execute(
                        """
                        DELETE FROM fabric_batch_artifacts
                        WHERE batch_id=ANY(%s)
                        """,
                        (batches,),
                    )
                    cur.execute(
                        """
                        DELETE FROM fabric_result_batches
                        WHERE batch_id=ANY(%s)
                        """,
                        (batches,),
                    )
                    report["result_batches"]=cur.rowcount

                cur.execute(
                    """
                    SELECT event_id
                    FROM fabric_outbox
                    WHERE (
                        published_at IS NOT NULL AND published_at<=%s
                    ) OR (
                        %s AND published_at IS NULL AND created_at<=%s
                    )
                    ORDER BY created_at,event_id
                    LIMIT %s
                    """,
                    (cutoff,not self.emit_outbox,cutoff,limit),
                )
                outbox=[str(row["event_id"]) for row in cur.fetchall()]
                if outbox:
                    cur.execute(
                        """
                        DELETE FROM fabric_outbox
                        WHERE event_id=ANY(%s)
                        """,
                        (outbox,),
                    )
                    report["outbox_events"]=cur.rowcount

                cur.execute(
                    """
                    SELECT worker_id,nonce
                    FROM fabric_request_nonces
                    WHERE seen_at<=%s
                    ORDER BY seen_at,worker_id,nonce
                    LIMIT %s
                    """,
                    (cutoff,limit),
                )
                nonces=[
                    (str(row["worker_id"]),str(row["nonce"]))
                    for row in cur.fetchall()
                ]
                if nonces:
                    cur.executemany(
                        """
                        DELETE FROM fabric_request_nonces
                        WHERE worker_id=%s AND nonce=%s
                        """,
                        nonces,
                    )
                    report["request_nonces"]=len(nonces)

                cur.execute(
                    """
                    SELECT worker_id,day_key
                    FROM fabric_worker_egress_daily
                    WHERE updated_at<=%s
                    ORDER BY updated_at,worker_id,day_key
                    LIMIT %s
                    """,
                    (cutoff,limit),
                )
                egress_days=[
                    (str(row["worker_id"]),int(row["day_key"]))
                    for row in cur.fetchall()
                ]
                if egress_days:
                    cur.executemany(
                        """
                        DELETE FROM fabric_worker_egress_daily
                        WHERE worker_id=%s AND day_key=%s
                        """,
                        egress_days,
                    )
                    report["egress_days"]=len(egress_days)

                cur.execute(
                    """
                    SELECT w.task_id
                    FROM fabric_work AS w
                    WHERE w.state='COMPLETE'
                      AND w.updated_at<=%s
                      AND w.payload_json<>'{}'::jsonb
                      AND NOT EXISTS (
                          SELECT 1 FROM fabric_result_batches AS b
                          WHERE b.task_id=w.task_id
                            AND b.consumed_at IS NULL
                            AND b.quarantined=FALSE
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM fabric_provider_permits AS p
                          WHERE p.task_id=w.task_id AND p.active=TRUE
                      )
                    ORDER BY w.updated_at,w.task_id
                    LIMIT %s
                    """,
                    (cutoff,limit),
                )
                compactable=[str(row["task_id"]) for row in cur.fetchall()]
                if compactable:
                    cur.execute(
                        """
                        UPDATE fabric_work
                        SET payload_json='{}'::jsonb,
                            cursor=NULL,
                            last_error=NULL
                        WHERE task_id=ANY(%s)
                        """,
                        (compactable,),
                    )
                    report["work_payloads_compacted"]=cur.rowcount
        return report

    def configure_provider_budget(
        self,provider:str,*,requests_per_second:float,
        max_global_inflight:int,require_qualified_region:bool=True,
    )->None:
        now=float(self.clock())
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO fabric_provider_budgets(
                        provider,requests_per_second,max_global_inflight,
                        require_qualified_region,updated_at
                    ) VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT(provider) DO UPDATE SET
                        requests_per_second=EXCLUDED.requests_per_second,
                        max_global_inflight=EXCLUDED.max_global_inflight,
                        require_qualified_region=EXCLUDED.require_qualified_region,
                        updated_at=EXCLUDED.updated_at
                    """,
                    (
                        provider,requests_per_second,max_global_inflight,
                        require_qualified_region,now,
                    ),
                )

    def record_provider_region_observation(
        self,provider:str,*,worker_id:str,worker_instance_id:str,
        task_id:str,generation:int,connect_success:bool,
        status_code:int|None,latency_ms:float,response_bytes:int,
        timeout:bool=False,policy_block:bool=False,
    )->str:
        now=float(self.clock())
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                worker=self._worker(cur,worker_id,worker_instance_id)
                self._lease(cur,task_id,worker_id,worker_instance_id,generation)
                region=str(worker["region"])
                success=bool(
                    connect_success and status_code is not None and status_code<500
                )
                throttle=status_code in {429,503}
                cur.execute(
                    """
                    INSERT INTO fabric_provider_regions(
                        provider,region,state,samples,successes,timeouts,
                        throttles,policy_blocks,total_latency_ms,response_bytes,
                        updated_at
                    ) VALUES (%s,%s,'UNKNOWN',1,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(provider,region) DO UPDATE SET
                        samples=fabric_provider_regions.samples+1,
                        successes=fabric_provider_regions.successes+EXCLUDED.successes,
                        timeouts=fabric_provider_regions.timeouts+EXCLUDED.timeouts,
                        throttles=fabric_provider_regions.throttles+EXCLUDED.throttles,
                        policy_blocks=fabric_provider_regions.policy_blocks+EXCLUDED.policy_blocks,
                        total_latency_ms=fabric_provider_regions.total_latency_ms+EXCLUDED.total_latency_ms,
                        response_bytes=fabric_provider_regions.response_bytes+EXCLUDED.response_bytes,
                        updated_at=EXCLUDED.updated_at
                    RETURNING samples,successes,timeouts,throttles,policy_blocks
                    """,
                    (
                        provider,region,int(success),int(timeout),int(throttle),
                        int(policy_block),max(0.0,float(latency_ms)),
                        max(0,int(response_bytes)),now,
                    ),
                )
                row=cur.fetchone()
                samples=int(row["samples"])
                successes=int(row["successes"])
                blocked=int(row["policy_blocks"])
                failures=int(row["timeouts"])+int(row["throttles"])+blocked
                if blocked:
                    state="BLOCKED"
                elif samples>=3 and successes>=2 and failures<=samples//2:
                    state="QUALIFIED"
                elif samples>=3 and successes==0:
                    state="UNQUALIFIED"
                else:
                    state="UNKNOWN"
                cur.execute(
                    """
                    UPDATE fabric_provider_regions SET state=%s,updated_at=%s
                    WHERE provider=%s AND region=%s
                    """,
                    (state,now,provider,region),
                )
                return state

    @staticmethod
    def _day_key(now:float)->int:
        return int(now//86400)

    def issue_provider_permit(
        self,provider:str,*,worker_id:str,worker_instance_id:str,
        task_id:str,generation:int,request_id:str,ttl_seconds:float=30.0,
    )->ProviderPermit|None:
        now=float(self.clock())
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                worker=self._worker(cur,worker_id,worker_instance_id,lock=True)
                self._lease(cur,task_id,worker_id,worker_instance_id,generation)
                if provider not in set(worker["allowed_providers_json"]):
                    raise ProviderAccessDeniedError(provider)
                budget_bytes=int(worker["daily_egress_budget_bytes"])
                if budget_bytes>0:
                    cur.execute(
                        """
                        SELECT response_bytes FROM fabric_worker_egress_daily
                        WHERE worker_id=%s AND day_key=%s
                        """,
                        (worker_id,self._day_key(now)),
                    )
                    usage=cur.fetchone()
                    if usage is not None and int(usage["response_bytes"])>=budget_bytes:
                        raise WorkerEgressBudgetExceededError(worker_id)
                cur.execute(
                    """
                    SELECT * FROM fabric_provider_permits
                    WHERE worker_id=%s AND request_id=%s
                    """,
                    (worker_id,request_id),
                )
                prior=cur.fetchone()
                if prior is not None:
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
                cur.execute(
                    "SELECT * FROM fabric_provider_budgets WHERE provider=%s FOR UPDATE",
                    (provider,),
                )
                budget=cur.fetchone()
                if budget is None:
                    raise ProviderAccessDeniedError(
                        f"provider budget is not configured: {provider}"
                    )
                cur.execute(
                    """
                    UPDATE fabric_provider_permits SET active=FALSE
                    WHERE provider=%s AND active=TRUE AND expires_at<=%s
                    """,
                    (provider,now),
                )
                if bool(budget["require_qualified_region"]):
                    cur.execute(
                        """
                        SELECT state FROM fabric_provider_regions
                        WHERE provider=%s AND region=%s
                        """,
                        (provider,str(worker["region"])),
                    )
                    region=cur.fetchone()
                    if region is None or str(region["state"])!="QUALIFIED":
                        return None
                cur.execute(
                    """
                    SELECT COUNT(*) AS n FROM fabric_provider_permits
                    WHERE provider=%s AND active=TRUE
                    """,
                    (provider,),
                )
                active=int(cur.fetchone()["n"])
                if (
                    active>=int(budget["max_global_inflight"])
                    or now<float(budget["next_request_at"])
                    or now<float(budget["cooldown_until"])
                ):
                    return None
                next_at=now+1.0/float(budget["requests_per_second"])
                cur.execute(
                    """
                    UPDATE fabric_provider_budgets
                    SET next_request_at=%s,updated_at=%s WHERE provider=%s
                    """,
                    (next_at,now,provider),
                )
                permit=ProviderPermit(
                    permit_id=uuid4().hex,request_id=request_id,
                    provider=provider,worker_id=worker_id,
                    worker_instance_id=worker_instance_id,task_id=task_id,
                    generation=generation,allowed_requests=1,
                    max_inflight=int(budget["max_global_inflight"]),
                    expires_at=now+float(ttl_seconds),
                )
                cur.execute(
                    """
                    INSERT INTO fabric_provider_permits(
                        permit_id,request_id,provider,worker_id,
                        worker_instance_id,task_id,generation,allowed_requests,
                        max_inflight,expires_at,issued_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        permit.permit_id,permit.request_id,permit.provider,
                        permit.worker_id,permit.worker_instance_id,permit.task_id,
                        permit.generation,permit.allowed_requests,
                        permit.max_inflight,permit.expires_at,now,
                    ),
                )
                return permit

    def report_provider_permit(
        self,permit_id:str,*,worker_id:str,worker_instance_id:str,
        status_code:int|None,cooldown_seconds:float=0.0,response_bytes:int=0,
    )->None:
        now=float(self.clock())
        with self.connection.transaction():
            with self.connection.cursor() as cur:
                self._worker(cur,worker_id,worker_instance_id)
                cur.execute(
                    """
                    SELECT * FROM fabric_provider_permits
                    WHERE permit_id=%s FOR UPDATE
                    """,
                    (permit_id,),
                )
                row=cur.fetchone()
                if (
                    row is None
                    or str(row["worker_id"])!=worker_id
                    or str(row["worker_instance_id"])!=worker_instance_id
                ):
                    raise ProviderAccessDeniedError("unknown provider permit")
                if not bool(row["active"]):
                    return
                cur.execute(
                    """
                    UPDATE fabric_provider_permits
                    SET active=FALSE,status_code=%s WHERE permit_id=%s
                    """,
                    (status_code,permit_id),
                )
                if cooldown_seconds:
                    cur.execute(
                        """
                        UPDATE fabric_provider_budgets
                        SET cooldown_until=GREATEST(cooldown_until,%s),
                            updated_at=%s WHERE provider=%s
                        """,
                        (
                            now+float(cooldown_seconds),now,str(row["provider"])
                        ),
                    )
                cur.execute(
                    """
                    INSERT INTO fabric_worker_egress_daily(
                        worker_id,day_key,response_bytes,updated_at
                    ) VALUES (%s,%s,%s,%s)
                    ON CONFLICT(worker_id,day_key) DO UPDATE SET
                        response_bytes=fabric_worker_egress_daily.response_bytes
                            +EXCLUDED.response_bytes,
                        updated_at=EXCLUDED.updated_at
                    """,
                    (worker_id,self._day_key(now),int(response_bytes),now),
                )

    def task_row(self,task_id:str):
        with self.connection.cursor() as cur:
            cur.execute("SELECT * FROM fabric_work WHERE task_id=%s",(task_id,))
            row=cur.fetchone()
            if row is None:
                raise KeyError(task_id)
            return row

    def status_snapshot(self)->dict[str,int]:
        with self.connection.cursor() as cur:
            cur.execute("SELECT state,COUNT(*) AS n FROM fabric_work GROUP BY state")
            states={str(row["state"]):int(row["n"]) for row in cur.fetchall()}
            cur.execute("SELECT COUNT(*) AS n FROM fabric_workers WHERE revoked=FALSE")
            workers=int(cur.fetchone()["n"])
            cur.execute(
                "SELECT COUNT(*) AS n FROM fabric_work "
                "WHERE state='COMPLETE' AND payload_json='{}'::jsonb"
            )
            compacted=int(cur.fetchone()["n"])
            cur.execute(
                "SELECT COUNT(*) AS n FROM fabric_result_batches "
                "WHERE consumed_at IS NULL AND quarantined=FALSE"
            )
            unconsumed=int(cur.fetchone()["n"])
            cur.execute(
                "SELECT COUNT(*) AS n FROM fabric_result_batches WHERE quarantined=TRUE"
            )
            quarantined=int(cur.fetchone()["n"])
            cur.execute("SELECT COUNT(*) AS n FROM fabric_result_batches")
            retained=int(cur.fetchone()["n"])
            cur.execute(
                "SELECT COUNT(*) AS n FROM fabric_provider_permits WHERE active=FALSE"
            )
            inactive_permits=int(cur.fetchone()["n"])
            cur.execute(
                "SELECT COUNT(*) AS n FROM fabric_outbox WHERE published_at IS NULL"
            )
            outbox=int(cur.fetchone()["n"])
        return {
            "workers":workers,
            "pending":states.get("PENDING",0)+states.get("RETRY",0),
            "leased":states.get("LEASED",0),
            "complete":states.get("COMPLETE",0),
            "dead":states.get("DEAD",0),
            "compacted_work":compacted,
            "unconsumed_batches":unconsumed,
            "quarantined_batches":quarantined,
            "retained_batches":retained,
            "inactive_provider_permits":inactive_permits,
            "pending_outbox":outbox,
        }
