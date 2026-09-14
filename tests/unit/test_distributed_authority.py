from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.distributed.authority_store import (
    BatchConflictError,
    DistributedAuthorityStore,
    StaleLeaseError,
)
from creeper.distributed.identity import (
    batch_id,
    host_id,
    host_year_id,
    resolution_key,
)
from creeper.distributed.models import (
    Capability,
    ResultBatch,
    TaskClass,
    WorkDefinition,
    WorkerDescriptor,
)


class MutableClock:
    def __init__(self, value: float = 1000.0) -> None:
        self.value = float(value)

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += float(seconds)


class DistributedAuthorityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.clock = MutableClock()
        self.store = DistributedAuthorityStore(
            Path(self.tmp.name) / "distributed.sqlite3",
            clock=self.clock,
        )
        self.worker_a = WorkerDescriptor(
            worker_id="worker-us",
            runtime_class="vm",
            region="us-east",
            architecture="x86_64",
            memory_bytes=2 * 1024**3,
            cpu_count=2,
            network_class="public",
            capabilities=(
                Capability.ONLINE_QUERY.value,
                Capability.RDAP.value,
            ),
        )
        self.worker_b = WorkerDescriptor(
            worker_id="worker-eu",
            runtime_class="vm",
            region="eu-central",
            architecture="x86_64",
            memory_bytes=2 * 1024**3,
            cpu_count=2,
            network_class="public",
            capabilities=(Capability.ONLINE_QUERY.value,),
        )
        self.store.register_worker(self.worker_a)
        self.store.register_worker(self.worker_b)

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    @staticmethod
    def work(hostname: str = "example.com", *, partition: str = "0") -> WorkDefinition:
        return WorkDefinition(
            producer="HistoricalQueryProducer",
            task_class=TaskClass.HOST_BATCH,
            input_identity=hostname,
            coverage={"scope": "HOST", "year_from": 1996, "year_to": 2001},
            partition=partition,
            algorithm_version="resolver-v1",
            required_capabilities=(Capability.ONLINE_QUERY.value,),
            priority=1.0,
        )

    def test_identity_layer_is_deterministic_and_namespace_separated(self) -> None:
        self.assertEqual(host_id("Example.COM"), host_id("example.com"))
        self.assertNotEqual(host_id("example.com"), host_year_id("example.com", 1997))
        self.assertEqual(batch_id("task-a", 3), batch_id("task-a", 3))
        self.assertEqual(
            resolution_key(
                hostname="Example.com",
                provider="internet_archive",
                scope="HOST",
                coverage={"year_from": 1996, "year_to": 2001},
                resolver_version="v1",
            ),
            resolution_key(
                hostname="example.com",
                provider="internet_archive",
                scope="HOST",
                coverage={"year_to": 2001, "year_from": 1996},
                resolver_version="v1",
            ),
        )

    def test_work_key_is_exactly_once_admission_key(self) -> None:
        work = self.work()
        first = self.store.admit_work(work)
        second = self.store.admit_work(work)

        self.assertEqual(first, second)
        count = self.store.connection.execute(
            "SELECT COUNT(*) FROM distributed_work"
        ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_two_workers_cannot_hold_same_active_work(self) -> None:
        task_id = self.store.admit_work(self.work())
        first = self.store.claim_work(self.worker_a.worker_id, lease_seconds=30)
        second = self.store.claim_work(self.worker_b.worker_id, lease_seconds=30)

        self.assertIsNotNone(first)
        assert first is not None
        self.assertEqual(first.task_id, task_id)
        self.assertEqual(first.generation, 1)
        self.assertIsNone(second)

    def test_reclaim_increments_generation_and_fences_old_owner(self) -> None:
        self.store.admit_work(self.work())
        first = self.store.claim_work(self.worker_a.worker_id, lease_seconds=10)
        assert first is not None
        self.clock.advance(11)
        second = self.store.claim_work(self.worker_b.worker_id, lease_seconds=30)
        assert second is not None

        self.assertEqual(second.task_id, first.task_id)
        self.assertEqual(second.generation, first.generation + 1)

        stale_batch = ResultBatch(
            task_id=first.task_id,
            generation=first.generation,
            sequence_no=0,
            results=({"hostname": "example.com", "year": 1997},),
            cursor_after="page:1",
        )
        with self.assertRaises(StaleLeaseError):
            self.store.commit_result_batch(
                stale_batch,
                worker_id=self.worker_a.worker_id,
            )

        fresh_batch = ResultBatch(
            task_id=second.task_id,
            generation=second.generation,
            sequence_no=0,
            results=({"hostname": "example.com", "year": 1997},),
            cursor_after="page:1",
        )
        self.assertTrue(
            self.store.commit_result_batch(
                fresh_batch,
                worker_id=self.worker_b.worker_id,
            )
        )
        self.assertEqual(self.store.batch_count(second.task_id), 1)

    def test_result_batch_replay_has_exactly_once_logical_effect(self) -> None:
        self.store.admit_work(self.work())
        lease = self.store.claim_work(self.worker_a.worker_id, lease_seconds=30)
        assert lease is not None
        batch = ResultBatch(
            task_id=lease.task_id,
            generation=lease.generation,
            sequence_no=7,
            results=(
                {"kind": "HY", "hostname": "example.com", "year": 1996},
                {"kind": "H", "hostname": "other.example"},
            ),
            cursor_after="offset:4096",
        )

        self.assertTrue(
            self.store.commit_result_batch(batch, worker_id=lease.worker_id)
        )
        self.assertFalse(
            self.store.commit_result_batch(batch, worker_id=lease.worker_id)
        )
        self.assertEqual(self.store.batch_count(lease.task_id), 1)
        self.assertEqual(
            self.store.task_row(lease.task_id)["cursor"],
            "offset:4096",
        )

        conflicting = ResultBatch(
            task_id=lease.task_id,
            generation=lease.generation,
            sequence_no=7,
            results=({"kind": "HY", "hostname": "evil.example", "year": 1996},),
            cursor_after="offset:4096",
        )
        with self.assertRaises(BatchConflictError):
            self.store.commit_result_batch(
                conflicting,
                worker_id=lease.worker_id,
            )

    def test_exact_replay_remains_acknowledgeable_after_task_finish(self) -> None:
        self.store.admit_work(self.work())
        lease = self.store.claim_work(self.worker_a.worker_id, lease_seconds=30)
        assert lease is not None
        batch = ResultBatch(
            task_id=lease.task_id,
            generation=lease.generation,
            sequence_no=0,
            results=({"kind": "HY", "hostname": "example.com", "year": 2001},),
        )
        self.assertTrue(
            self.store.commit_result_batch(batch, worker_id=lease.worker_id)
        )
        self.store.finish_task(
            lease.task_id,
            worker_id=lease.worker_id,
            generation=lease.generation,
        )
        self.assertFalse(
            self.store.commit_result_batch(batch, worker_id=lease.worker_id)
        )

    def test_capabilities_gate_claims(self) -> None:
        bulk = WorkDefinition(
            producer="BulkHistoricalIndexProducer",
            task_class=TaskClass.SOURCE_SHARD,
            input_identity="arquivo:shard:1",
            coverage={"year_from": 1996, "year_to": 2001},
            partition="shard-1",
            algorithm_version="bulk-v1",
            required_capabilities=(Capability.STREAMING_BULK.value,),
        )
        self.store.admit_work(bulk)
        self.assertIsNone(self.store.claim_work(self.worker_a.worker_id))

        bulk_worker = WorkerDescriptor(
            worker_id="worker-oci",
            runtime_class="vm",
            region="oci-home",
            architecture="aarch64",
            memory_bytes=12 * 1024**3,
            cpu_count=2,
            network_class="public",
            capabilities=(Capability.STREAMING_BULK.value,),
        )
        self.store.register_worker(bulk_worker)
        claimed = self.store.claim_work(bulk_worker.worker_id)
        self.assertIsNotNone(claimed)
        assert claimed is not None
        self.assertEqual(claimed.work.task_class, TaskClass.SOURCE_SHARD)

    def test_provider_inflight_budget_is_global_across_regions(self) -> None:
        self.store.configure_provider_budget(
            "internet_archive",
            requests_per_second=1_000_000.0,
            max_global_inflight=1,
        )
        self.store.admit_work(self.work("a.example"))
        self.store.admit_work(self.work("b.example"))
        lease_a = self.store.claim_work(self.worker_a.worker_id)
        lease_b = self.store.claim_work(self.worker_b.worker_id)
        assert lease_a is not None and lease_b is not None

        permit_a = self.store.issue_provider_permit(
            "internet_archive",
            worker_id=lease_a.worker_id,
            task_id=lease_a.task_id,
            generation=lease_a.generation,
        )
        self.assertIsNotNone(permit_a)
        self.clock.advance(0.001)
        permit_b = self.store.issue_provider_permit(
            "internet_archive",
            worker_id=lease_b.worker_id,
            task_id=lease_b.task_id,
            generation=lease_b.generation,
        )
        self.assertIsNone(permit_b)

        assert permit_a is not None
        self.store.report_provider_permit(
            permit_a.permit_id,
            worker_id=lease_a.worker_id,
            status_code=200,
        )
        permit_b = self.store.issue_provider_permit(
            "internet_archive",
            worker_id=lease_b.worker_id,
            task_id=lease_b.task_id,
            generation=lease_b.generation,
        )
        self.assertIsNotNone(permit_b)

    def test_429_cooldown_is_global_not_per_region(self) -> None:
        self.store.configure_provider_budget(
            "internet_archive",
            requests_per_second=1_000_000.0,
            max_global_inflight=2,
        )
        self.store.admit_work(self.work("a.example"))
        self.store.admit_work(self.work("b.example"))
        lease_a = self.store.claim_work(self.worker_a.worker_id)
        lease_b = self.store.claim_work(self.worker_b.worker_id)
        assert lease_a is not None and lease_b is not None

        first = self.store.issue_provider_permit(
            "internet_archive",
            worker_id=lease_a.worker_id,
            task_id=lease_a.task_id,
            generation=lease_a.generation,
        )
        assert first is not None
        self.store.report_provider_permit(
            first.permit_id,
            worker_id=lease_a.worker_id,
            status_code=429,
            cooldown_seconds=60,
        )
        self.clock.advance(1)
        blocked = self.store.issue_provider_permit(
            "internet_archive",
            worker_id=lease_b.worker_id,
            task_id=lease_b.task_id,
            generation=lease_b.generation,
        )
        self.assertIsNone(blocked)

        snapshot = self.store.provider_budget_snapshot("internet_archive")
        self.assertGreater(snapshot["cooldown_until"], self.clock())


if __name__ == "__main__":
    unittest.main()
