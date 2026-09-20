from __future__ import annotations

import os
import unittest
from uuid import uuid4

from creeper.distributed.authority_store import (
    BatchConflictError,
    DistributedAuthorityStore,
    StaleLeaseError,
)
from creeper.distributed.models import (
    Capability,
    ResultBatch,
    TaskClass,
    WorkDefinition,
    WorkerDescriptor,
)
from creeper.distributed.postgres_store import PostgresAuthorityStore


_REQUIRED_SURFACE = {
    "register_worker",
    "heartbeat",
    "revoke_worker",
    "admit_work",
    "claim_work",
    "renew_task",
    "commit_result_batch",
    "finish_task",
    "fail_task",
    "unconsumed_batches",
    "unconsumed_batches_for_task",
    "mark_batch_consumed",
    "mark_batch_consume_failed",
    "pending_outbox",
    "mark_outbox_published",
    "gc_transient_state",
    "configure_provider_budget",
    "record_provider_region_observation",
    "issue_provider_permit",
    "report_provider_permit",
    "task_row",
    "status_snapshot",
    "consume_request_nonce",
}


class FabricStoreSurfaceTests(unittest.TestCase):
    def test_sqlite_and_postgres_expose_same_authority_protocol(self) -> None:
        for cls in (DistributedAuthorityStore, PostgresAuthorityStore):
            with self.subTest(store=cls.__name__):
                missing = sorted(
                    name for name in _REQUIRED_SURFACE
                    if not callable(getattr(cls, name, None))
                )
                self.assertEqual(missing, [])


@unittest.skipUnless(
    os.environ.get("CREEPER_POSTGRES_TEST_DSN"),
    "CREEPER_POSTGRES_TEST_DSN is not configured",
)
class FabricPostgresAuthorityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dsn = os.environ["CREEPER_POSTGRES_TEST_DSN"]
        self.store = PostgresAuthorityStore(self.dsn)
        with self.store.connection.transaction():
            with self.store.connection.cursor() as cur:
                cur.execute(
                    """
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname='public'
                      AND tablename LIKE 'fabric_%'
                    ORDER BY tablename
                    """
                )
                tables = [str(row["tablename"]) for row in cur.fetchall()]
                if tables:
                    cur.execute(
                        "TRUNCATE " + ",".join(tables) + " CASCADE"
                    )
        suffix = uuid4().hex[:12]
        self.worker = WorkerDescriptor(
            worker_id=f"worker-{suffix}",
            worker_instance_id=f"instance-{suffix}",
            runtime_class="full",
            region="sg",
            architecture="x86_64",
            memory_bytes=8 * 1024**3,
            cpu_count=4,
            network_class="public",
            capabilities=(Capability.RESIDUAL_QUERY.value,),
            producers=("ResidualQueryProducer",),
            allowed_providers=("datacite",),
        )
        self.store.register_worker(self.worker)
        self.store.configure_provider_budget(
            "datacite",
            requests_per_second=10.0,
            max_global_inflight=4,
            require_qualified_region=False,
        )

    def tearDown(self) -> None:
        self.store.close()

    def work(self, suffix: str = "1") -> WorkDefinition:
        return WorkDefinition(
            producer="ResidualQueryProducer",
            task_class=TaskClass.RESIDUAL_QUERY,
            input_identity=f"input:{suffix}",
            payload={"query": suffix},
            partition="cell",
            algorithm_version="v1",
            required_capabilities=(Capability.RESIDUAL_QUERY.value,),
            required_providers=("datacite",),
        )

    def test_work_key_and_result_batch_are_idempotent(self) -> None:
        task_id, inserted = self.store.admit_work(self.work())
        replay_id, replay_inserted = self.store.admit_work(self.work())
        self.assertTrue(inserted)
        self.assertFalse(replay_inserted)
        self.assertEqual(task_id, replay_id)

        lease = self.store.claim_work(
            self.worker.worker_id,
            self.worker.worker_instance_id,
            lease_seconds=60,
        )
        self.assertIsNotNone(lease)
        assert lease is not None
        batch = ResultBatch(
            task_id=task_id,
            generation=lease.generation,
            sequence_no=0,
            results=({"value": 1},),
            cursor_after="done",
            final=True,
        )
        self.assertTrue(
            self.store.commit_result_batch(
                batch,
                worker_id=self.worker.worker_id,
                worker_instance_id=self.worker.worker_instance_id,
            )
        )
        self.assertFalse(
            self.store.commit_result_batch(
                batch,
                worker_id=self.worker.worker_id,
                worker_instance_id=self.worker.worker_instance_id,
            )
        )
        with self.assertRaises(BatchConflictError):
            self.store.commit_result_batch(
                ResultBatch(
                    task_id=task_id,
                    generation=lease.generation,
                    sequence_no=0,
                    results=({"value": 2},),
                    cursor_after="done",
                    final=True,
                ),
                worker_id=self.worker.worker_id,
                worker_instance_id=self.worker.worker_instance_id,
            )
        row = self.store.task_row(task_id)
        self.assertEqual(row["state"], "COMPLETE")
        self.assertEqual(int(row["next_sequence_no"]), 1)

    def test_brokerless_mode_does_not_append_outbox_rows(self) -> None:
        before=len(self.store.pending_outbox(limit=1000))
        quiet=PostgresAuthorityStore(self.dsn,emit_outbox=False)
        try:
            suffix=uuid4().hex[:12]
            worker=WorkerDescriptor(
                worker_id=f"quiet-worker-{suffix}",
                worker_instance_id=f"quiet-instance-{suffix}",
                runtime_class="full",
                region="sg",
                architecture="x86_64",
                memory_bytes=1024,
                cpu_count=1,
                network_class="public",
                capabilities=(Capability.RESIDUAL_QUERY.value,),
                producers=("ResidualQueryProducer",),
                allowed_providers=("datacite",),
            )
            quiet.register_worker(worker)
            quiet.configure_provider_budget(
                "datacite",
                requests_per_second=10.0,
                max_global_inflight=4,
                require_qualified_region=False,
            )
            task_id,_=quiet.admit_work(self.work("quiet-"+suffix))
            lease=quiet.claim_work(
                worker.worker_id,
                worker.worker_instance_id,
                lease_seconds=60,
            )
            self.assertIsNotNone(lease)
            assert lease is not None
            quiet.commit_result_batch(
                ResultBatch(
                    task_id=task_id,
                    generation=lease.generation,
                    sequence_no=0,
                    results=({"value":1},),
                    final=True,
                ),
                worker_id=worker.worker_id,
                worker_instance_id=worker.worker_instance_id,
            )
        finally:
            quiet.close()
        after=len(self.store.pending_outbox(limit=1000))
        self.assertEqual(after,before)

    def test_gc_prunes_consumed_batch_but_retains_workkey_tombstone(self) -> None:
        now=[1_000_000.0]
        store=PostgresAuthorityStore(
            self.dsn,
            clock=lambda:now[0],
            emit_outbox=False,
        )
        try:
            suffix=uuid4().hex[:12]
            worker=WorkerDescriptor(
                worker_id=f"gc-worker-{suffix}",
                worker_instance_id=f"gc-instance-{suffix}",
                runtime_class="full",
                region="sg",
                architecture="x86_64",
                memory_bytes=1024,
                cpu_count=1,
                network_class="public",
                capabilities=(Capability.RESIDUAL_QUERY.value,),
                producers=("ResidualQueryProducer",),
                allowed_providers=("datacite",),
            )
            store.register_worker(worker)
            store.configure_provider_budget(
                "datacite",
                requests_per_second=10.0,
                max_global_inflight=4,
                require_qualified_region=False,
            )
            work=self.work("gc-"+suffix)
            task_id,inserted=store.admit_work(work)
            self.assertTrue(inserted)
            lease=store.claim_work(
                worker.worker_id,
                worker.worker_instance_id,
                lease_seconds=60,
            )
            self.assertIsNotNone(lease)
            assert lease is not None
            batch=ResultBatch(
                task_id=task_id,
                generation=lease.generation,
                sequence_no=0,
                results=({"value":1},),
                final=True,
            )
            store.commit_result_batch(
                batch,
                worker_id=worker.worker_id,
                worker_instance_id=worker.worker_instance_id,
            )
            self.assertTrue(store.mark_batch_consumed(batch.batch_id))

            now[0]+=2*86400
            report=store.gc_transient_state(
                retention_seconds=86400,
                limit=100,
            )

            self.assertEqual(report["result_batches"],1)
            self.assertEqual(report["work_payloads_compacted"],1)
            with store.connection.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) AS n FROM fabric_result_batches "
                    "WHERE batch_id=%s",
                    (batch.batch_id,),
                )
                self.assertEqual(int(cur.fetchone()["n"]),0)
            task_row=store.task_row(task_id)
            self.assertEqual(task_row["state"],"COMPLETE")
            self.assertEqual(dict(task_row["payload_json"]),{})
            replay_id,replay_inserted=store.admit_work(work)
            self.assertEqual(replay_id,task_id)
            self.assertFalse(replay_inserted)
        finally:
            store.close()

    def test_new_worker_incarnation_fences_old_generation(self) -> None:
        task_id, _ = self.store.admit_work(self.work("fence"))
        old = self.store.claim_work(
            self.worker.worker_id,
            self.worker.worker_instance_id,
            lease_seconds=60,
        )
        self.assertIsNotNone(old)
        assert old is not None

        replacement = WorkerDescriptor(
            worker_id=self.worker.worker_id,
            worker_instance_id="replacement-" + uuid4().hex[:8],
            runtime_class=self.worker.runtime_class,
            region=self.worker.region,
            architecture=self.worker.architecture,
            memory_bytes=self.worker.memory_bytes,
            cpu_count=self.worker.cpu_count,
            network_class=self.worker.network_class,
            capabilities=self.worker.capabilities,
            producers=self.worker.producers,
            allowed_providers=self.worker.allowed_providers,
        )
        self.store.register_worker(replacement)

        with self.assertRaises(StaleLeaseError):
            self.store.commit_result_batch(
                ResultBatch(
                    task_id=task_id,
                    generation=old.generation,
                    sequence_no=0,
                    results=({"stale": True},),
                ),
                worker_id=self.worker.worker_id,
                worker_instance_id=self.worker.worker_instance_id,
            )

        new = self.store.claim_work(
            replacement.worker_id,
            replacement.worker_instance_id,
            lease_seconds=60,
        )
        self.assertIsNotNone(new)
        assert new is not None
        self.assertEqual(new.generation, old.generation + 1)
        self.assertEqual(new.attempt, old.attempt + 1)


if __name__ == "__main__":
    unittest.main()
