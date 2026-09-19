from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.fabric.models import (
    FABRIC_PROTOCOL_VERSION,
    FabricCapability,
    FabricTaskState,
    FabricWorkClass,
    ResultBatch,
    WorkerDescriptor,
    WorkSpec,
)
from creeper.fabric.schema import POSTGRES_CLAIM_SQL
from creeper.fabric.store import (
    BatchConflictError,
    BatchSequenceError,
    SQLiteFabricStore,
    StaleLeaseError,
)


class FabricV2StoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.now = 1000.0
        self.store = SQLiteFabricStore(
            Path(self.tmp.name) / "fabric.sqlite3",
            clock=lambda: self.now,
        )
        self.worker = WorkerDescriptor(
            worker_id="worker-search-sg",
            region="sg",
            runtime_class="vm",
            architecture="x86_64",
            network_class="public",
            cpu_count=4,
            memory_bytes=8 * 1024**3,
            capabilities=(
                FabricCapability.SEARCH_STRUCTURED,
                FabricCapability.HTTP_FETCH,
            ),
            allowed_providers=("datacite", "internet_archive"),
            max_concurrency=2,
            protocol_version=FABRIC_PROTOCOL_VERSION,
            edition="test",
        )
        self.store.register_worker(self.worker)

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def work(
        self,
        *,
        provider: str | None = "datacite",
        capability: FabricCapability = FabricCapability.SEARCH_STRUCTURED,
        coverage: dict[str, object] | None = None,
        max_attempts: int = 3,
    ) -> WorkSpec:
        return WorkSpec(
            work_class=FabricWorkClass.RESIDUAL_SEARCH,
            producer="residual-search",
            algorithm_version="v1",
            partition_key="cell:abc",
            input_identity="query:abc",
            coverage=coverage or {"period": "1998", "variant": 0},
            required_capabilities=(capability,),
            priority=5.0,
            queue="default",
            max_attempts=max_attempts,
            provider=provider,
            min_memory_bytes=1024,
            network_class="public",
        )

    def test_work_key_is_stable_across_mapping_order(self) -> None:
        one = self.work(coverage={"period": "1998", "variant": 0})
        two = self.work(coverage={"variant": 0, "period": "1998"})
        self.assertEqual(one.work_key, two.work_key)

    def test_submit_is_idempotent_and_emits_one_ready_event(self) -> None:
        work = self.work()
        first = self.store.submit_work(work)
        second = self.store.submit_work(work)

        self.assertEqual(first, second)
        self.assertEqual(len(self.store.unpublished_events()), 1)

    def test_claim_routes_by_capability_provider_memory_and_network(self) -> None:
        denied_provider = self.store.submit_work(self.work(provider="zenodo"))
        denied_cap = self.store.submit_work(
            self.work(
                provider=None,
                capability=FabricCapability.SOURCE_SCOUT,
                coverage={"period": "1998", "variant": 1},
            )
        )
        allowed = self.store.submit_work(
            self.work(coverage={"period": "1998", "variant": 2})
        )

        lease = self.store.claim(self.worker.worker_id, lease_seconds=30)

        self.assertIsNotNone(lease)
        assert lease is not None
        self.assertEqual(lease.task_id, allowed)
        self.assertEqual(
            self.store.task_row(denied_provider)["state"],
            FabricTaskState.READY.value,
        )
        self.assertEqual(
            self.store.task_row(denied_cap)["state"],
            FabricTaskState.READY.value,
        )

    def test_expired_lease_is_reclaimed_with_higher_epoch_and_old_owner_fenced(self) -> None:
        task_id = self.store.submit_work(self.work())
        first = self.store.claim(self.worker.worker_id, lease_seconds=10)
        self.assertIsNotNone(first)
        assert first is not None
        self.assertEqual(first.lease_epoch, 1)

        self.now += 11
        second = self.store.claim(self.worker.worker_id, lease_seconds=10)
        self.assertIsNotNone(second)
        assert second is not None
        self.assertEqual(second.task_id, task_id)
        self.assertEqual(second.lease_epoch, 2)
        self.assertEqual(second.attempt, 2)

        with self.assertRaises(StaleLeaseError):
            self.store.complete(first)

    def test_result_batch_is_idempotent_by_digest(self) -> None:
        self.store.submit_work(self.work())
        lease = self.store.claim(self.worker.worker_id, lease_seconds=60)
        assert lease is not None
        batch = ResultBatch(
            task_id=lease.task_id,
            lease_epoch=lease.lease_epoch,
            sequence_no=0,
            results=({"hostname": "a.example"},),
            cursor_after={"offset": 10},
        )

        self.assertTrue(self.store.commit_batch(lease, batch))
        self.assertFalse(self.store.commit_batch(lease, batch))

        conflict = ResultBatch(
            task_id=lease.task_id,
            lease_epoch=lease.lease_epoch,
            sequence_no=0,
            results=({"hostname": "different.example"},),
            cursor_after={"offset": 10},
        )
        with self.assertRaises(BatchConflictError):
            self.store.commit_batch(lease, conflict)

    def test_result_sequence_is_strictly_monotonic(self) -> None:
        self.store.submit_work(self.work())
        lease = self.store.claim(self.worker.worker_id, lease_seconds=60)
        assert lease is not None
        batch = ResultBatch(
            task_id=lease.task_id,
            lease_epoch=lease.lease_epoch,
            sequence_no=1,
            results=(),
        )
        with self.assertRaises(BatchSequenceError):
            self.store.commit_batch(lease, batch)

    def test_retryable_failure_requeues_without_changing_work_identity(self) -> None:
        task_id = self.store.submit_work(self.work())
        lease = self.store.claim(self.worker.worker_id, lease_seconds=60)
        assert lease is not None

        self.store.fail(
            lease,
            error="transient provider throttle",
            retryable=True,
            retry_delay_seconds=5,
        )
        row = self.store.task_row(task_id)
        self.assertEqual(row["state"], FabricTaskState.READY.value)
        self.assertEqual(row["work_key"], lease.work_key)

        self.now += 6
        next_lease = self.store.claim(self.worker.worker_id, lease_seconds=60)
        assert next_lease is not None
        self.assertEqual(next_lease.lease_epoch, lease.lease_epoch + 1)

    def test_revocation_returns_owned_work_to_ready_and_fences_worker(self) -> None:
        task_id = self.store.submit_work(self.work())
        lease = self.store.claim(self.worker.worker_id, lease_seconds=60)
        assert lease is not None

        self.store.revoke_worker(self.worker.worker_id)

        self.assertEqual(
            self.store.task_row(task_id)["state"],
            FabricTaskState.READY.value,
        )
        with self.assertRaises(StaleLeaseError):
            self.store.complete(lease)

    def test_outbox_and_inbox_are_idempotent(self) -> None:
        task_id = self.store.submit_work(self.work())
        events = self.store.unpublished_events()
        self.assertEqual(len(events), 1)
        event_id = str(events[0]["event_id"])

        self.assertTrue(self.store.consume_event("consumer-a", event_id))
        self.assertFalse(self.store.consume_event("consumer-a", event_id))

        self.store.mark_event_published(event_id)
        self.assertEqual(self.store.unpublished_events(), ())
        self.assertEqual(self.store.task_row(task_id)["state"], "READY")

    def test_dag_reducer_is_blocked_until_all_parents_succeed(self) -> None:
        first = self.work(coverage={"period": "1998", "variant": 10})
        second = self.work(coverage={"period": "1998", "variant": 11})
        self.store.submit_work(first)
        self.store.submit_work(second)

        reducer = WorkSpec(
            work_class=FabricWorkClass.REDUCE_COMMIT,
            producer="residual-reducer",
            algorithm_version="v1",
            partition_key="cell:abc",
            input_identity="reduce:abc",
            coverage={"cell_key": "abc"},
            required_capabilities=(FabricCapability.AUTHORITY_REDUCE,),
            priority=100.0,
            dependency_work_keys=(first.work_key, second.work_key),
        )
        reducer_task_id = self.store.submit_work(reducer)

        reducer_worker = WorkerDescriptor(
            worker_id="authority-reducer",
            region="authority",
            runtime_class="authority",
            architecture="x86_64",
            network_class="private",
            cpu_count=2,
            memory_bytes=4 * 1024**3,
            capabilities=(FabricCapability.AUTHORITY_REDUCE,),
            allowed_providers=(),
            max_concurrency=1,
            edition="test",
        )
        self.store.register_worker(reducer_worker)
        self.assertIsNone(
            self.store.claim(reducer_worker.worker_id, lease_seconds=60)
        )

        one = self.store.claim(self.worker.worker_id, lease_seconds=60)
        assert one is not None
        self.store.complete(one)
        self.assertIsNone(
            self.store.claim(reducer_worker.worker_id, lease_seconds=60)
        )

        two = self.store.claim(self.worker.worker_id, lease_seconds=60)
        assert two is not None
        self.store.complete(two)

        reduced = self.store.claim(
            reducer_worker.worker_id,
            lease_seconds=60,
        )
        self.assertIsNotNone(reduced)
        assert reduced is not None
        self.assertEqual(reduced.task_id, reducer_task_id)
        self.assertEqual(
            set(reduced.work.dependency_work_keys),
            {first.work_key, second.work_key},
        )

    def test_unknown_dependency_rolls_back_work_admission(self) -> None:
        work = WorkSpec(
            work_class=FabricWorkClass.REDUCE_COMMIT,
            producer="reduce",
            algorithm_version="v1",
            partition_key="x",
            input_identity="x",
            coverage={},
            required_capabilities=(FabricCapability.AUTHORITY_REDUCE,),
            dependency_work_keys=("work:missing",),
        )
        with self.assertRaises(KeyError):
            self.store.submit_work(work)
        row = self.store.connection.execute(
            "SELECT 1 FROM fabric_tasks_v2 WHERE work_key=?",
            (work.work_key,),
        ).fetchone()
        self.assertIsNone(row)

    def test_provider_permits_are_global_rate_and_region_gated(self) -> None:
        self.store.configure_provider(
            "datacite",
            requests_per_second=2.0,
            max_global_inflight=1,
            require_qualified_region=True,
        )
        self.store.set_provider_region(
            "datacite",
            "sg",
            qualified=False,
        )
        self.store.submit_work(self.work())
        lease = self.store.claim(self.worker.worker_id, lease_seconds=60)
        assert lease is not None

        self.assertIsNone(
            self.store.acquire_provider_permit(lease, "datacite")
        )

        self.store.set_provider_region(
            "datacite",
            "sg",
            qualified=True,
        )
        permit = self.store.acquire_provider_permit(lease, "datacite")
        self.assertIsNotNone(permit)
        assert permit is not None

        # Max inflight and the global RPS clock both prevent an immediate
        # second request.
        self.assertIsNone(
            self.store.acquire_provider_permit(lease, "datacite")
        )
        self.store.report_provider_permit(
            permit,
            status_code=200,
            response_bytes=1234,
        )
        self.assertIsNone(
            self.store.acquire_provider_permit(lease, "datacite")
        )

        self.now += 0.51
        second = self.store.acquire_provider_permit(lease, "datacite")
        self.assertIsNotNone(second)

    def test_throttle_report_applies_global_provider_cooldown(self) -> None:
        self.store.configure_provider(
            "datacite",
            requests_per_second=10.0,
            max_global_inflight=2,
            require_qualified_region=False,
        )
        self.store.submit_work(self.work())
        lease = self.store.claim(self.worker.worker_id, lease_seconds=60)
        assert lease is not None
        permit = self.store.acquire_provider_permit(lease, "datacite")
        assert permit is not None
        self.store.report_provider_permit(
            permit,
            status_code=429,
            throttled=True,
            cooldown_seconds=5,
        )

        self.now += 1
        self.assertIsNone(
            self.store.acquire_provider_permit(lease, "datacite")
        )
        self.now += 5
        self.assertIsNotNone(
            self.store.acquire_provider_permit(lease, "datacite")
        )

    def test_postgres_claim_uses_skip_locked_and_capability_filters(self) -> None:
        sql = " ".join(POSTGRES_CLAIM_SQL.split()).upper()
        self.assertIn("FOR UPDATE SKIP LOCKED", sql)
        self.assertIn("REQUIRED_CAPABILITIES <@", sql)
        self.assertIn("LEASE_EPOCH = TASK.LEASE_EPOCH + 1", sql)


if __name__ == "__main__":
    unittest.main()
