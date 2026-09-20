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
        self.store.configure_provider_budget(
            "datacite",
            requests_per_second=10.0,
            max_global_inflight=4,
            require_qualified_region=False,
        )

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
            store.configure_provider_budget(
                "datacite",
                requests_per_second=10.0,
                max_global_inflight=4,
                require_qualified_region=False,
            )
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
            store.configure_provider_budget(
                "datacite",
                requests_per_second=10.0,
                max_global_inflight=4,
                require_qualified_region=False,
            )
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
            self.assertEqual(report["work_payloads_compacted"],1)
            self.assertEqual(
                store.connection.execute(
                    "SELECT COUNT(*) FROM fabric_result_batches"
                ).fetchone()[0],
                0,
            )
            task_row=store.task_row(task_id)
            self.assertEqual(task_row["state"],"COMPLETE")
            self.assertEqual(task_row["payload_json"],"{}")
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
        status=self.store.status_snapshot()
        self.assertEqual(status["unconsumed_batches"],0)
        self.assertEqual(status["quarantined_batches"],1)
        self.assertEqual(status["retained_batches"],1)


    def test_unknown_region_probe_converges_to_blocked_and_stops_claims(self) -> None:
        self.store.configure_provider_budget(
            "datacite",
            requests_per_second=10.0,
            max_global_inflight=4,
            require_qualified_region=True,
            allow_unknown_region_probe=True,
        )
        first_id,_=self.store.admit_work(self.work("region-1"))
        lease=self.store.claim_work("worker-a","instance-1",lease_seconds=60)
        self.assertIsNotNone(lease)
        assert lease is not None
        permit=self.store.issue_provider_permit(
            "datacite",
            worker_id="worker-a",
            worker_instance_id="instance-1",
            task_id=lease.task_id,
            generation=lease.generation,
            request_id="probe-1",
        )
        self.assertIsNotNone(permit)
        second_permit=self.store.issue_provider_permit(
            "datacite",
            worker_id="worker-a",
            worker_instance_id="instance-1",
            task_id=lease.task_id,
            generation=lease.generation,
            request_id="probe-2",
        )
        self.assertIsNone(second_permit)

        state=self.store.record_provider_region_observation(
            "datacite",
            worker_id="worker-a",
            worker_instance_id="instance-1",
            task_id=lease.task_id,
            generation=lease.generation,
            connect_success=True,
            status_code=451,
            latency_ms=5.0,
            response_bytes=10,
            policy_block=True,
        )
        self.assertEqual(state,"BLOCKED")
        assert permit is not None
        self.store.report_provider_permit(
            permit.permit_id,
            worker_id="worker-a",
            worker_instance_id="instance-1",
            status_code=451,
        )
        self.store.fail_task(
            first_id,
            worker_id="worker-a",
            worker_instance_id="instance-1",
            generation=lease.generation,
            error="blocked",
            retryable=True,
        )
        self.store.admit_work(self.work("region-2"))
        self.assertIsNone(
            self.store.claim_work("worker-a","instance-1",lease_seconds=60)
        )

    def test_status_snapshot_exposes_stale_worker_identity(self) -> None:
        now=[1000.0]
        store=DistributedAuthorityStore(
            self.root/"fabric-health.sqlite3",
            clock=lambda:now[0],
        )
        try:
            store.register_worker(self.worker)
            healthy=store.status_snapshot(stale_after_seconds=180)
            self.assertEqual(healthy["stale_workers"],0)
            now[0]+=181
            stale=store.status_snapshot(stale_after_seconds=180)
            self.assertEqual(stale["stale_workers"],1)
            self.assertEqual(
                stale["worker_health"][0]["worker_id"],
                "worker-a",
            )
            self.assertTrue(stale["worker_health"][0]["stale"])
        finally:
            store.close()

    def test_strict_unknown_region_does_not_claim_when_probe_is_disabled(self) -> None:
        self.store.configure_provider_budget(
            "datacite",
            requests_per_second=10.0,
            max_global_inflight=4,
            require_qualified_region=True,
            allow_unknown_region_probe=False,
        )
        self.store.admit_work(self.work("strict-unknown"))
        self.assertIsNone(
            self.store.claim_work(
                "worker-a","instance-1",lease_seconds=60
            )
        )

    def test_blocked_region_is_reprobed_after_configured_ttl(self) -> None:
        now=[1000.0]
        store=DistributedAuthorityStore(
            self.root/"fabric-region-reprobe.sqlite3",
            clock=lambda:now[0],
        )
        try:
            store.register_worker(self.worker)
            store.configure_provider_budget(
                "datacite",
                requests_per_second=10.0,
                max_global_inflight=4,
                require_qualified_region=True,
                allow_unknown_region_probe=True,
                region_reprobe_after_seconds=60.0,
            )
            task_id,_=store.admit_work(self.work("region-reprobe"))
            first=store.claim_work(
                "worker-a","instance-1",lease_seconds=60
            )
            self.assertIsNotNone(first)
            assert first is not None
            permit=store.issue_provider_permit(
                "datacite",
                worker_id="worker-a",
                worker_instance_id="instance-1",
                task_id=task_id,
                generation=first.generation,
                request_id="policy-block",
            )
            self.assertIsNotNone(permit)
            state=store.record_provider_region_observation(
                "datacite",
                worker_id="worker-a",
                worker_instance_id="instance-1",
                task_id=task_id,
                generation=first.generation,
                connect_success=True,
                status_code=451,
                latency_ms=5.0,
                response_bytes=10,
                policy_block=True,
            )
            self.assertEqual(state,"BLOCKED")
            assert permit is not None
            store.report_provider_permit(
                permit.permit_id,
                worker_id="worker-a",
                worker_instance_id="instance-1",
                status_code=451,
            )
            store.fail_task(
                task_id,
                worker_id="worker-a",
                worker_instance_id="instance-1",
                generation=first.generation,
                error="policy block",
                retryable=True,
            )

            self.assertIsNone(
                store.claim_work(
                    "worker-a","instance-1",lease_seconds=60
                )
            )
            now[0]+=61.0
            second=store.claim_work(
                "worker-a","instance-1",lease_seconds=60
            )
            self.assertIsNotNone(second)
            assert second is not None
            self.assertIsNotNone(
                store.issue_provider_permit(
                    "datacite",
                    worker_id="worker-a",
                    worker_instance_id="instance-1",
                    task_id=task_id,
                    generation=second.generation,
                    request_id="reprobe",
                )
            )
        finally:
            store.close()


    def test_repeated_claim_returns_existing_active_lease(self) -> None:
        first_id,_=self.store.admit_work(self.work("claim-replay-1"))
        second_id,_=self.store.admit_work(self.work("claim-replay-2"))
        first=self.store.claim_work(
            "worker-a","instance-1",lease_seconds=60
        )
        self.assertIsNotNone(first)
        assert first is not None
        replay=self.store.claim_work(
            "worker-a","instance-1",lease_seconds=120
        )
        self.assertIsNotNone(replay)
        assert replay is not None
        self.assertEqual(replay.task_id,first.task_id)
        self.assertEqual(replay.generation,first.generation)
        self.assertEqual(replay.attempt,first.attempt)
        self.assertGreater(replay.lease_deadline,first.lease_deadline)
        self.assertIn(replay.task_id,{first_id,second_id})
        other=second_id if replay.task_id==first_id else first_id
        self.assertEqual(self.store.task_row(other)["state"],"PENDING")


    def test_expired_lost_response_permit_is_reissued_not_resurrected(self) -> None:
        now=[1000.0]
        store=DistributedAuthorityStore(
            self.root/"fabric-permit-replay.sqlite3",
            clock=lambda:now[0],
        )
        try:
            store.register_worker(self.worker)
            store.configure_provider_budget(
                "datacite",
                requests_per_second=10.0,
                max_global_inflight=4,
                require_qualified_region=False,
            )
            task_id,_=store.admit_work(self.work("permit-replay"))
            lease=store.claim_work(
                "worker-a","instance-1",lease_seconds=300
            )
            self.assertIsNotNone(lease)
            assert lease is not None
            first=store.issue_provider_permit(
                "datacite",
                worker_id="worker-a",
                worker_instance_id="instance-1",
                task_id=task_id,
                generation=lease.generation,
                request_id="lost-response",
                ttl_seconds=10,
            )
            self.assertIsNotNone(first)
            assert first is not None
            now[0]+=11.0
            second=store.issue_provider_permit(
                "datacite",
                worker_id="worker-a",
                worker_instance_id="instance-1",
                task_id=task_id,
                generation=lease.generation,
                request_id="lost-response",
                ttl_seconds=10,
            )
            self.assertIsNotNone(second)
            assert second is not None
            self.assertNotEqual(second.permit_id,first.permit_id)
            self.assertGreater(second.expires_at,first.expires_at)
            row=store.connection.execute(
                """
                SELECT permit_id,active FROM fabric_provider_permits
                WHERE worker_id=? AND request_id=?
                """,
                ("worker-a","lost-response"),
            ).fetchone()
            self.assertEqual(str(row["permit_id"]),second.permit_id)
            self.assertEqual(int(row["active"]),1)
        finally:
            store.close()



if __name__=="__main__":
    unittest.main()
