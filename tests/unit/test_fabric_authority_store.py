from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

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


class FabricAuthorityStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name)
        self.store=DistributedAuthorityStore(self.root/"fabric.sqlite3")
        self.worker=WorkerDescriptor(
            worker_id="worker-a",
            worker_instance_id="instance-1",
            runtime_class="full",
            region="sg",
            architecture="x86_64",
            memory_bytes=8*1024**3,
            cpu_count=4,
            network_class="public",
            capabilities=(Capability.RESIDUAL_QUERY.value,),
            producers=("ResidualQueryProducer",),
            allowed_providers=("datacite",),
        )
        self.store.register_worker(self.worker)

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def work(self, suffix: str="1") -> WorkDefinition:
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

    def test_work_admission_is_idempotent_and_claim_is_capability_fenced(self) -> None:
        first, inserted=self.store.admit_work(self.work())
        second, inserted_again=self.store.admit_work(self.work())
        self.assertTrue(inserted)
        self.assertFalse(inserted_again)
        self.assertEqual(first,second)

        lease=self.store.claim_work(
            "worker-a","instance-1",lease_seconds=60
        )
        self.assertIsNotNone(lease)
        assert lease is not None
        self.assertEqual(lease.generation,1)
        self.assertEqual(lease.work.producer,"ResidualQueryProducer")

    def test_new_worker_incarnation_releases_and_fences_old_lease(self) -> None:
        task_id,_=self.store.admit_work(self.work())
        old=self.store.claim_work("worker-a","instance-1",lease_seconds=60)
        self.assertIsNotNone(old)
        assert old is not None

        replacement=WorkerDescriptor(
            worker_id="worker-a",
            worker_instance_id="instance-2",
            runtime_class="full",
            region="sg",
            architecture="x86_64",
            memory_bytes=8*1024**3,
            cpu_count=4,
            network_class="public",
            capabilities=(Capability.RESIDUAL_QUERY.value,),
            producers=("ResidualQueryProducer",),
            allowed_providers=("datacite",),
        )
        self.store.register_worker(replacement)

        with self.assertRaises(StaleLeaseError):
            self.store.commit_result_batch(
                ResultBatch(
                    task_id=task_id,
                    generation=old.generation,
                    sequence_no=0,
                    results=({"kind":"stale"},),
                ),
                worker_id="worker-a",
                worker_instance_id="instance-1",
            )

        new=self.store.claim_work("worker-a","instance-2",lease_seconds=60)
        self.assertIsNotNone(new)
        assert new is not None
        self.assertEqual(new.generation,2)
        self.assertEqual(new.attempt,2)

    def test_result_batch_replay_is_idempotent_but_changed_replay_conflicts(self) -> None:
        task_id,_=self.store.admit_work(self.work())
        lease=self.store.claim_work("worker-a","instance-1",lease_seconds=60)
        assert lease is not None
        batch=ResultBatch(
            task_id=task_id,
            generation=lease.generation,
            sequence_no=0,
            results=({"value":1},),
            cursor_after="1",
        )
        self.assertTrue(
            self.store.commit_result_batch(
                batch,
                worker_id="worker-a",
                worker_instance_id="instance-1",
            )
        )
        self.assertFalse(
            self.store.commit_result_batch(
                batch,
                worker_id="worker-a",
                worker_instance_id="instance-1",
            )
        )
        with self.assertRaises(BatchConflictError):
            self.store.commit_result_batch(
                ResultBatch(
                    task_id=task_id,
                    generation=lease.generation,
                    sequence_no=0,
                    results=({"value":2},),
                    cursor_after="1",
                ),
                worker_id="worker-a",
                worker_instance_id="instance-1",
            )
        row=self.store.task_row(task_id)
        self.assertEqual(row["next_sequence_no"],1)

    def test_outbox_can_be_disabled_for_brokerless_http_profile(self) -> None:
        store=DistributedAuthorityStore(
            self.root/"fabric-no-outbox.sqlite3",
            emit_outbox=False,
        )
        try:
            store.register_worker(self.worker)
            task_id,inserted=store.admit_work(self.work("no-outbox"))
            self.assertTrue(inserted)
            lease=store.claim_work(
                "worker-a","instance-1",lease_seconds=60
            )
            self.assertIsNotNone(lease)
            assert lease is not None
            store.commit_result_batch(
                ResultBatch(
                    task_id=task_id,
                    generation=lease.generation,
                    sequence_no=0,
                    results=({"value":1},),
                    final=True,
                ),
                worker_id="worker-a",
                worker_instance_id="instance-1",
            )
            self.assertEqual(store.pending_outbox(),())
            self.assertEqual(store.status_snapshot()["pending_outbox"],0)
        finally:
            store.close()

    def test_gc_prunes_consumed_batch_but_retains_workkey_tombstone(self) -> None:
        now=[1_000_000.0]
        store=DistributedAuthorityStore(
            self.root/"fabric-gc.sqlite3",
            clock=lambda:now[0],
            emit_outbox=False,
        )
        try:
            store.register_worker(self.worker)
            task_id,inserted=store.admit_work(self.work("gc"))
            self.assertTrue(inserted)
            lease=store.claim_work(
                "worker-a","instance-1",lease_seconds=60
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
                worker_id="worker-a",
                worker_instance_id="instance-1",
            )
            self.assertTrue(store.mark_batch_consumed(batch.batch_id))
            self.assertEqual(
                store.connection.execute(
                    "SELECT COUNT(*) FROM fabric_result_batches"
                ).fetchone()[0],
                1,
            )

            now[0]+=2*86400
            report=store.gc_transient_state(
                retention_seconds=86400,
                limit=100,
            )

            self.assertEqual(report["result_batches"],1)
            self.assertEqual(
                store.connection.execute(
                    "SELECT COUNT(*) FROM fabric_result_batches"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(store.task_row(task_id)["state"],"COMPLETE")
            replay_id,replay_inserted=store.admit_work(self.work("gc"))
            self.assertEqual(replay_id,task_id)
            self.assertFalse(replay_inserted)
        finally:
            store.close()

    def test_poison_domain_batch_is_quarantined_without_blocking_inbox(self) -> None:
        task_id,_=self.store.admit_work(self.work())
        lease=self.store.claim_work("worker-a","instance-1",lease_seconds=60)
        assert lease is not None
        batch=ResultBatch(
            task_id=task_id,
            generation=lease.generation,
            sequence_no=0,
            results=({"bad":"shape"},),
            final=True,
        )
        self.store.commit_result_batch(
            batch,
            worker_id="worker-a",
            worker_instance_id="instance-1",
        )
        self.assertEqual(len(self.store.unconsumed_batches()),1)
        self.assertFalse(
            self.store.mark_batch_consume_failed(
                batch.batch_id,"bad",max_attempts=2
            )
        )
        self.assertTrue(
            self.store.mark_batch_consume_failed(
                batch.batch_id,"bad again",max_attempts=2
            )
        )
        self.assertEqual(self.store.unconsumed_batches(),())


if __name__=="__main__":
    unittest.main()
