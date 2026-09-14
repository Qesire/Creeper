from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.distributed.authority_store import (
    BatchConflictError,
    BatchSequenceError,
    DistributedAuthorityStore,
    ProviderRegionNotQualifiedError,
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

    def test_authority_restart_preserves_generation_and_batch_idempotence(self) -> None:
        task_id = self.store.admit_work(self.work("restart.example"))
        first = self.store.claim_work(self.worker_a.worker_id, lease_seconds=5)
        assert first is not None
        batch = ResultBatch(
            task_id=task_id,
            generation=first.generation,
            sequence_no=0,
            results=({"kind": "H", "hostname": "restart.example"},),
            cursor_after="cursor:1",
        )
        self.assertTrue(
            self.store.commit_result_batch(batch, worker_id=first.worker_id)
        )

        path = Path(self.tmp.name) / "distributed.sqlite3"
        self.store.close()
        self.clock.advance(6)
        self.store = DistributedAuthorityStore(path, clock=self.clock)

        second = self.store.claim_work(self.worker_b.worker_id, lease_seconds=30)
        assert second is not None
        self.assertEqual(second.task_id, task_id)
        self.assertEqual(second.generation, first.generation + 1)
        replay = ResultBatch(
            task_id=task_id,
            generation=second.generation,
            sequence_no=0,
            results=({"kind": "H", "hostname": "restart.example"},),
            cursor_after="cursor:1",
        )
        self.assertFalse(
            self.store.commit_result_batch(replay, worker_id=second.worker_id)
        )
        self.assertEqual(self.store.batch_count(task_id), 1)

    def test_result_batch_replay_has_exactly_once_logical_effect(self) -> None:
        self.store.admit_work(self.work())
        lease = self.store.claim_work(self.worker_a.worker_id, lease_seconds=30)
        assert lease is not None
        batch = ResultBatch(
            task_id=lease.task_id,
            generation=lease.generation,
            sequence_no=0,
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
            sequence_no=0,
            results=({"kind": "HY", "hostname": "evil.example", "year": 1996},),
            cursor_after="offset:4096",
        )
        with self.assertRaises(BatchConflictError):
            self.store.commit_result_batch(
                conflicting,
                worker_id=lease.worker_id,
            )

    def test_new_result_batches_must_follow_authority_sequence(self) -> None:
        self.store.admit_work(self.work("sequence.example"))
        lease = self.store.claim_work(self.worker_a.worker_id, lease_seconds=30)
        assert lease is not None
        self.assertEqual(lease.next_sequence_no, 0)

        with self.assertRaises(BatchSequenceError):
            self.store.commit_result_batch(
                ResultBatch(
                    task_id=lease.task_id,
                    generation=lease.generation,
                    sequence_no=1,
                    results=({"kind": "H", "hostname": "sequence.example"},),
                    cursor_after="cursor:bad",
                ),
                worker_id=lease.worker_id,
            )
        self.assertEqual(self.store.batch_count(lease.task_id), 0)
        self.assertEqual(
            self.store.task_row(lease.task_id)["next_sequence_no"],
            0,
        )

        self.assertTrue(
            self.store.commit_result_batch(
                ResultBatch(
                    task_id=lease.task_id,
                    generation=lease.generation,
                    sequence_no=0,
                    results=({"kind": "H", "hostname": "sequence.example"},),
                    cursor_after="cursor:1",
                ),
                worker_id=lease.worker_id,
            )
        )
        row = self.store.task_row(lease.task_id)
        self.assertEqual(row["next_sequence_no"], 1)
        self.assertEqual(row["cursor"], "cursor:1")

    def test_committed_batch_replay_survives_lease_generation_change(self) -> None:
        self.store.admit_work(self.work())
        first = self.store.claim_work(self.worker_a.worker_id, lease_seconds=5)
        assert first is not None
        batch = ResultBatch(
            task_id=first.task_id,
            generation=first.generation,
            sequence_no=0,
            results=({"kind": "HY", "hostname": "example.com", "year": 1999},),
            cursor_after="page:1",
        )
        self.assertTrue(
            self.store.commit_result_batch(batch, worker_id=first.worker_id)
        )

        self.clock.advance(6)
        second = self.store.claim_work(self.worker_b.worker_id, lease_seconds=30)
        assert second is not None
        replay = ResultBatch(
            task_id=second.task_id,
            generation=second.generation,
            sequence_no=0,
            results=({"kind": "HY", "hostname": "example.com", "year": 1999},),
            cursor_after="page:1",
        )
        self.assertFalse(
            self.store.commit_result_batch(replay, worker_id=second.worker_id)
        )
        self.assertEqual(self.store.batch_count(second.task_id), 1)

    def test_revoked_worker_cannot_mutate_current_lease(self) -> None:
        self.store.admit_work(self.work())
        lease = self.store.claim_work(self.worker_a.worker_id, lease_seconds=30)
        assert lease is not None
        self.store.revoke_worker(lease.worker_id)

        with self.assertRaises(RuntimeError):
            self.store.renew_task(
                lease.task_id,
                worker_id=lease.worker_id,
                generation=lease.generation,
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
            require_qualified_region=False,
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

    def test_claim_gate_respects_worker_producer_registry(self) -> None:
        restricted = WorkerDescriptor(
            worker_id="worker-restricted",
            runtime_class="vm",
            region="test-region",
            architecture="x86_64",
            memory_bytes=1024**3,
            cpu_count=2,
            network_class="public",
            capabilities=(Capability.ONLINE_QUERY.value,),
            producers=("HistoricalQueryProducer",),
        )
        self.store.register_worker(restricted)
        unsupported = WorkDefinition(
            producer="RDAPProducer",
            task_class=TaskClass.HOST_BATCH,
            input_identity="rdap.example",
            coverage={"year_from": 1996, "year_to": 2001},
            partition="0",
            algorithm_version="rdap-v1",
            required_capabilities=(Capability.ONLINE_QUERY.value,),
        )
        supported = WorkDefinition(
            producer="HistoricalQueryProducer",
            task_class=TaskClass.HOST_BATCH,
            input_identity="cdx.example",
            coverage={"year_from": 1996, "year_to": 2001},
            partition="0",
            algorithm_version="cdx-v1",
            required_capabilities=(Capability.ONLINE_QUERY.value,),
            priority=-1.0,
        )
        self.store.admit_work(unsupported)
        supported_id = self.store.admit_work(supported)

        claimed = self.store.claim_work(restricted.worker_id)

        self.assertIsNotNone(claimed)
        assert claimed is not None
        self.assertEqual(claimed.task_id, supported_id)
        self.assertEqual(claimed.work.producer, "HistoricalQueryProducer")

    def test_claim_gate_requires_provider_region_qualification(self) -> None:
        self.store.configure_provider_budget(
            "internet_archive",
            requests_per_second=10.0,
            max_global_inflight=1,
        )
        formal = WorkDefinition(
            producer="HistoricalQueryProducer",
            task_class=TaskClass.HOST_BATCH,
            input_identity="qualified.example",
            coverage={
                "provider": "internet_archive",
                "year_from": 1996,
                "year_to": 2001,
            },
            partition="0",
            algorithm_version="resolver-v1",
            required_capabilities=(Capability.ONLINE_QUERY.value,),
        )
        self.store.admit_work(formal)
        self.assertIsNone(self.store.claim_work(self.worker_a.worker_id))

        probe = WorkDefinition(
            producer="RegionProbeProducer",
            task_class=TaskClass.PROBE,
            input_identity="internet_archive",
            coverage={"provider": "internet_archive"},
            partition="qualification",
            algorithm_version="probe-v1",
            required_capabilities=(Capability.ONLINE_QUERY.value,),
            priority=2.0,
        )
        probe_task = self.store.admit_work(probe)
        probe_lease = self.store.claim_work(self.worker_a.worker_id)
        assert probe_lease is not None
        self.assertEqual(probe_lease.task_id, probe_task)
        for _ in range(3):
            self.store.record_provider_region_observation(
                "internet_archive",
                worker_id=probe_lease.worker_id,
                task_id=probe_lease.task_id,
                generation=probe_lease.generation,
                connect_success=True,
                status_code=200,
                latency_ms=50.0,
                response_bytes=64,
            )
        self.store.finish_task(
            probe_lease.task_id,
            worker_id=probe_lease.worker_id,
            generation=probe_lease.generation,
        )

        formal_lease = self.store.claim_work(self.worker_a.worker_id)
        self.assertIsNotNone(formal_lease)
        assert formal_lease is not None
        self.assertEqual(formal_lease.work.producer, "HistoricalQueryProducer")

    def test_formal_provider_permit_requires_qualified_region(self) -> None:
        self.store.configure_provider_budget(
            "internet_archive",
            requests_per_second=10.0,
            max_global_inflight=1,
        )
        self.store.admit_work(self.work("formal.example"))
        lease = self.store.claim_work(self.worker_a.worker_id)
        assert lease is not None

        with self.assertRaises(ProviderRegionNotQualifiedError):
            self.store.issue_provider_permit(
                "internet_archive",
                worker_id=lease.worker_id,
                task_id=lease.task_id,
                generation=lease.generation,
            )

    def test_probe_task_can_access_provider_before_region_is_qualified(self) -> None:
        self.store.configure_provider_budget(
            "internet_archive",
            requests_per_second=10.0,
            max_global_inflight=1,
        )
        probe = WorkDefinition(
            producer="RegionProbeProducer",
            task_class=TaskClass.PROBE,
            input_identity="internet_archive",
            coverage={"provider": "internet_archive"},
            partition="qualification",
            algorithm_version="probe-v1",
            required_capabilities=(Capability.ONLINE_QUERY.value,),
        )
        self.store.admit_work(probe)
        lease = self.store.claim_work(self.worker_a.worker_id)
        assert lease is not None
        permit = self.store.issue_provider_permit(
            "internet_archive",
            worker_id=lease.worker_id,
            task_id=lease.task_id,
            generation=lease.generation,
        )
        self.assertIsNotNone(permit)

    def test_resolution_coverage_subtracts_overlapping_intervals(self) -> None:
        self.store.admit_work(self.work("coverage.example"))
        lease = self.store.claim_work(self.worker_a.worker_id)
        assert lease is not None

        self.assertEqual(
            self.store.uncovered_resolution_intervals(
                hostname="coverage.example",
                provider="cdx-pool:test",
                scope="HOST",
                resolver_version="resolver-v1",
                year_from=1998,
                year_to=2001,
            ),
            ((1998, 2001),),
        )
        self.store.record_complete_resolution_coverage(
            lease.task_id,
            worker_id=lease.worker_id,
            generation=lease.generation,
            hostname="coverage.example",
            provider="cdx-pool:test",
            scope="HOST",
            resolver_version="resolver-v1",
            year_from=1996,
            year_to=1999,
        )
        self.assertEqual(
            self.store.uncovered_resolution_intervals(
                hostname="coverage.example",
                provider="cdx-pool:test",
                scope="HOST",
                resolver_version="resolver-v1",
                year_from=1998,
                year_to=2001,
            ),
            ((2000, 2001),),
        )
        self.store.record_complete_resolution_coverage(
            lease.task_id,
            worker_id=lease.worker_id,
            generation=lease.generation,
            hostname="coverage.example",
            provider="cdx-pool:test",
            scope="HOST",
            resolver_version="resolver-v1",
            year_from=2000,
            year_to=2001,
        )
        self.assertEqual(
            self.store.uncovered_resolution_intervals(
                hostname="coverage.example",
                provider="cdx-pool:test",
                scope="HOST",
                resolver_version="resolver-v1",
                year_from=1998,
                year_to=2001,
            ),
            (),
        )

    def test_host_work_admission_subtracts_existing_coverage(self) -> None:
        self.store.admit_work(self.work("admission.example"))
        lease = self.store.claim_work(self.worker_a.worker_id)
        assert lease is not None
        self.store.record_complete_resolution_coverage(
            lease.task_id,
            worker_id=lease.worker_id,
            generation=lease.generation,
            hostname="admission.example",
            provider="cdx-pool:set-a",
            scope="HOST",
            resolver_version="resolver-v1",
            year_from=1996,
            year_to=1999,
        )
        self.store.finish_task(
            lease.task_id,
            worker_id=lease.worker_id,
            generation=lease.generation,
        )

        admitted = self.store.admit_host_resolution_work(
            hostname="admission.example",
            physical_providers=("internet_archive", "arquivo_pt"),
            coverage_provider="cdx-pool:set-a",
            resolver_version="resolver-v1",
            year_from=1998,
            year_to=2001,
        )

        self.assertEqual(len(admitted), 1)
        row = self.store.task_row(admitted[0])
        coverage = __import__("json").loads(str(row["coverage_json"]))
        self.assertEqual(
            (coverage["year_from"], coverage["year_to"]),
            (2000, 2001),
        )
        self.assertEqual(
            coverage["providers"],
            ["internet_archive", "arquivo_pt"],
        )

        self.assertEqual(
            self.store.admit_host_resolution_work(
                hostname="admission.example",
                physical_providers=("internet_archive", "arquivo_pt"),
                coverage_provider="cdx-pool:set-a",
                resolver_version="resolver-v1",
                year_from=1996,
                year_to=1999,
            ),
            (),
        )
        changed_provider_set = self.store.admit_host_resolution_work(
            hostname="admission.example",
            physical_providers=("internet_archive",),
            coverage_provider="cdx-pool:set-b",
            resolver_version="resolver-v1:set-b",
            year_from=1998,
            year_to=2001,
        )
        self.assertEqual(len(changed_provider_set), 1)

    def test_provider_region_qualification_is_derived_from_worker_region(self) -> None:
        probe_work = WorkDefinition(
            producer="RegionProbeProducer",
            task_class=TaskClass.PROBE,
            input_identity="internet_archive",
            coverage={"provider": "internet_archive"},
            partition="qualification",
            algorithm_version="probe-v1",
            required_capabilities=(Capability.ONLINE_QUERY.value,),
        )
        self.store.admit_work(probe_work)
        lease = self.store.claim_work(self.worker_a.worker_id)
        assert lease is not None

        states = []
        for _ in range(3):
            states.append(
                self.store.record_provider_region_observation(
                    "internet_archive",
                    worker_id=lease.worker_id,
                    task_id=lease.task_id,
                    generation=lease.generation,
                    connect_success=True,
                    status_code=200,
                    latency_ms=100.0,
                    response_bytes=128,
                )
            )
        self.assertEqual(states, ["UNKNOWN", "UNKNOWN", "QUALIFIED"])
        snapshot = self.store.provider_region_snapshot(
            "internet_archive",
            self.worker_a.region,
        )
        assert snapshot is not None
        self.assertEqual(snapshot["state"], "QUALIFIED")
        self.assertEqual(snapshot["samples"], 3)
        self.assertEqual(snapshot["success_rate"], 1.0)
        self.assertEqual(snapshot["mean_latency_ms"], 100.0)

    def test_429_cooldown_is_global_not_per_region(self) -> None:
        self.store.configure_provider_budget(
            "internet_archive",
            requests_per_second=1_000_000.0,
            max_global_inflight=2,
            require_qualified_region=False,
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
        initial_cooldown = self.store.provider_budget_snapshot(
            "internet_archive"
        )["cooldown_until"]
        self.clock.advance(1)
        self.store.report_provider_permit(
            first.permit_id,
            worker_id=lease_a.worker_id,
            status_code=429,
            cooldown_seconds=60,
        )
        self.assertEqual(
            self.store.provider_budget_snapshot("internet_archive")[
                "cooldown_until"
            ],
            initial_cooldown,
        )
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
