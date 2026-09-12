import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.policies import CDXQueryState, EvidenceQueryKey, TemporalScope
from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import HostObservation, SourceRecord
from creeper.runtime.source_producer import SourceProducer
from creeper.scheduler.credits import CreditLedger
from creeper.scheduler.global_scheduler import GlobalScheduler
from creeper.scheduler.leases import LeaseResult, WorkLease
from creeper.scheduler.priority import LeaseCandidate, ResourceCost
from creeper.sources.domains import DomainState, SourceDomain
from creeper.sources.reservoirs import Reservoir, ReservoirState
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class FakeSource:
    adapter_id = "fixture-source"

    def __init__(self, records):
        self.records = tuple(records)
        self.executions = 0

    def execute(self, lease: WorkLease):
        self.executions += 1
        selected = self.records[: lease.max_records]
        return iter(selected), LeaseResult(
            lease_id=lease.lease_id,
            records=len(selected),
            requests=1,
            bytes_read=sum(len(record.payload) for record in selected),
            elapsed_seconds=0.001,
            next_cursor=None,
        )

    def extract_hosts(self, record: SourceRecord):
        yield HostObservation(
            hostname=record.payload,
            source_id=record.source_id,
            locator=record.locator,
            scope=record.scope,
            source_year=record.source_year,
            source_time=record.source_time,
            record_type=record.record_type,
            artifact_ref=record.artifact_ref,
            direct_year_mask=record.direct_year_mask,
            year_hint_mask=record.year_hint_mask,
        )


class SourceProducerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        task_root = self.root / "task"
        baseline_root = task_root / "merged260909-3"
        baseline_root.mkdir(parents=True)
        for year in range(1996, 2002):
            (baseline_root / f"{year}.txt").write_text("", encoding="utf-8")
        (baseline_root / "candidate_pool.txt").write_text("", encoding="utf-8")
        self.baseline = BaselineIndex.build(task_root, self.root / "baseline.sqlite3")
        self.control = ControlStore(self.root / "control.sqlite3")
        self.evidence = EvidenceStore(self.root / "evidence.sqlite3")

    def tearDown(self):
        self.evidence.close()
        self.control.close()
        self.baseline.close()
        self.tmp.cleanup()

    def build_runtime(
        self,
        *,
        backlog_capacity: int,
        expected_tasks: int = 1,
        reservation_tasks: int | None = None,
        range_first_fraction: float = 0.0,
    ):
        record = SourceRecord(
            source_id="fixture-source",
            locator="fixture://1",
            payload="novel.example",
            scope=CandidateSourceScope.LOCAL_DISCOVERY,
            source_year=1997,
        )
        adapter = FakeSource([record])
        domain = SourceDomain(
            domain_id="fixture-domain",
            family="LOCAL_FIXTURE",
            discovery_mechanism="test",
            temporal_scope=(1996, 2001),
            state=DomainState.EXPLORING,
        )
        reservoir = Reservoir(
            reservoir_id="fixture-reservoir",
            domain_id=domain.domain_id,
            adapter_id=adapter.adapter_id,
            root_locator="fixture://records",
            enumeration_kind="finite_list",
            capacity_lower=1,
            capacity_upper=1,
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )
        self.control.save_domain(domain)
        self.control.save_reservoir(reservoir)
        template = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            cursor_start="0",
            max_records=1,
            max_requests=1,
            max_bytes=1024,
            max_seconds=30,
            expected_evidence_tasks=expected_tasks,
            expected_novel_eed=1.0,
        )
        candidate = LeaseCandidate(
            reservoir_id=reservoir.reservoir_id,
            expected_novel_eed=1.0,
            costs=ResourceCost(0, 1, 1, 1),
            reservoir=reservoir,
            lease=template,
            evidence_provider="wayback",
            expected_evidence_tasks=expected_tasks,
            reservation_evidence_tasks=reservation_tasks,
        )
        runtime = SourceProducer(
            baseline=self.baseline,
            control_store=self.control,
            evidence_store=self.evidence,
            scheduler=GlobalScheduler(CreditLedger({"wayback": 1})),
            candidates=[candidate],
            adapters={adapter.adapter_id: adapter},
            backlog_capacities={"wayback": backlog_capacity},
            queue_capacities={
                "source_records": 2,
                "observations": 2,
                "evidence_tasks": 2,
                "commits": 2,
            },
            range_first_fraction=range_first_fraction,
        )
        return runtime, adapter

    def test_source_producer_only_enqueues_durable_work(self):
        runtime, adapter = self.build_runtime(backlog_capacity=1)

        report = runtime.run_once()

        key = EvidenceQueryKey(
            "novel.example", TemporalScope(1997, 1997), "wayback", "cdx-v1"
        )
        task = self.control.get_evidence_task(key)
        self.assertEqual(report.leases_succeeded, 1)
        self.assertEqual(report.evidence_tasks_enqueued, 1)
        self.assertEqual(adapter.executions, 1)
        self.assertIsNotNone(task)
        self.assertEqual(task.state, "pending")
        self.assertIsNone(task.lease_owner)
        self.assertEqual(runtime.admission.reserved("wayback"), 0)
        self.assertEqual(self.evidence.count(), 0)
        origin = self.control.connection.execute(
            """
            SELECT source_key, reservoir_id
            FROM evidence_task_origins
            WHERE hostname = ? AND year_from = ? AND year_to = ?
            """,
            ("novel.example", 1997, 1997),
        ).fetchone()
        self.assertIsNotNone(origin)
        self.assertEqual(origin["source_key"], "fixture-reservoir")
        self.assertEqual(origin["reservoir_id"], "fixture-reservoir")

    def test_single_year_discovery_hint_enqueues_exact_year_without_direct_capsule(self):
        record = SourceRecord(
            source_id="webbase-fixture",
            locator="fixture://webbase/1",
            payload="novel.example",
            scope=CandidateSourceScope.LOCAL_DISCOVERY,
            source_year=2001,
            year_hint_mask=1 << (2001 - 1996),
        )
        adapter = FakeSource([record])
        domain = SourceDomain(
            domain_id="webbase-domain",
            family="RESEARCH_CRAWL",
            discovery_mechanism="test",
            temporal_scope=(2001, 2001),
            state=DomainState.EXPLORING,
        )
        reservoir = Reservoir(
            reservoir_id="webbase-reservoir",
            domain_id=domain.domain_id,
            adapter_id=adapter.adapter_id,
            root_locator="fixture://webbase",
            enumeration_kind="finite_list",
            capacity_lower=1,
            capacity_upper=1,
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )
        self.control.save_domain(domain)
        self.control.save_reservoir(reservoir)
        template = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            max_records=1,
            max_requests=1,
            max_bytes=1024,
            max_seconds=30,
            expected_evidence_tasks=1,
            expected_novel_eed=1.0,
        )
        candidate = LeaseCandidate(
            reservoir_id=reservoir.reservoir_id,
            expected_novel_eed=1.0,
            costs=ResourceCost(0, 1, 1, 1),
            reservoir=reservoir,
            lease=template,
            evidence_provider="wayback",
            expected_evidence_tasks=1,
        )
        runtime = SourceProducer(
            baseline=self.baseline,
            control_store=self.control,
            evidence_store=self.evidence,
            scheduler=GlobalScheduler(CreditLedger({"wayback": 1})),
            candidates=[candidate],
            adapters={adapter.adapter_id: adapter},
            backlog_capacities={"wayback": 1},
            queue_capacities={
                "source_records": 2,
                "observations": 2,
                "evidence_tasks": 2,
                "commits": 2,
            },
        )

        report = runtime.run_once()

        key = EvidenceQueryKey(
            "novel.example",
            TemporalScope(2001, 2001),
            "wayback",
            "cdx-v1",
        )
        self.assertIsNotNone(self.control.get_evidence_task(key))
        self.assertEqual(report.evidence_tasks_enqueued, 1)
        self.assertEqual(report.direct_capsules_committed, 0)
        self.assertEqual(self.evidence.count(), 0)


    def test_range_first_reservation_covers_parent_and_future_fanout(self):
        runtime, adapter = self.build_runtime(
            backlog_capacity=6,
            expected_tasks=1,
            reservation_tasks=6,
            range_first_fraction=1.0,
        )

        report = runtime.run_once()

        self.assertEqual(report.leases_succeeded, 1)
        self.assertEqual(report.evidence_tasks_enqueued, 1)
        self.assertEqual(adapter.executions, 1)
        tasks = self.control.list_evidence_tasks()
        self.assertEqual(len(tasks), 1)
        self.assertEqual(
            (
                tasks[0].key.temporal_scope.year_from,
                tasks[0].key.temporal_scope.year_to,
            ),
            (1996, 2001),
        )
        # One parent row is durable; five extra slots remain held for a
        # worst-case DECOMPOSED exact-year fanout.
        self.assertEqual(runtime.admission.reserved("wayback"), 5)

    def test_completed_wayback_range_suppresses_redundant_exact_year_work(self):
        range_key = EvidenceQueryKey(
            "novel.example", TemporalScope(1996, 1998), "wayback", "cdx-v1"
        )
        self.control.enqueue_evidence_tasks([range_key])
        self.control.claim_evidence_tasks(owner="evidence-a", limit=1)
        self.control.finish_range_task(
            range_key,
            CDXQueryState.PASS,
            owner="evidence-a",
        )
        runtime, adapter = self.build_runtime(backlog_capacity=2)

        report = runtime.run_once()

        exact_key = EvidenceQueryKey(
            "novel.example", TemporalScope(1997, 1997), "wayback", "cdx-v1"
        )
        self.assertEqual(report.leases_succeeded, 1)
        self.assertEqual(report.evidence_tasks_enqueued, 0)
        self.assertEqual(adapter.executions, 1)
        self.assertIsNone(self.control.get_evidence_task(exact_key))

    def test_duplicate_direct_host_years_commit_one_capsule_per_batch(self):
        records = [
            SourceRecord(
                source_id="direct-fixture",
                locator=f"fixture://{index}",
                payload="repeat.example",
                scope=CandidateSourceScope.LOCAL_DISCOVERY,
                source_year=1997,
                source_time=f"1997010{index}000000",
                record_type="CDX_CAPTURE",
                artifact_ref=f"fixture://{index}",
                direct_year_mask=1 << (1997 - 1996),
            )
            for index in range(1, 4)
        ]
        adapter = FakeSource(records)
        domain = SourceDomain(
            domain_id="direct-domain",
            family="DIRECT_FIXTURE",
            discovery_mechanism="test",
            temporal_scope=(1996, 2001),
            state=DomainState.EXPLORING,
        )
        reservoir = Reservoir(
            reservoir_id="direct-reservoir",
            domain_id=domain.domain_id,
            adapter_id=adapter.adapter_id,
            root_locator="fixture://direct",
            enumeration_kind="finite_list",
            capacity_lower=3,
            capacity_upper=3,
            evidence_mode="direct_year",
            state=ReservoirState.READY,
        )
        self.control.save_domain(domain)
        self.control.save_reservoir(reservoir)
        template = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            max_records=3,
            max_requests=1,
            max_bytes=4096,
            max_seconds=30,
            expected_evidence_tasks=0,
            expected_novel_eed=1.0,
        )
        candidate = LeaseCandidate(
            reservoir_id=reservoir.reservoir_id,
            expected_novel_eed=1.0,
            costs=ResourceCost(0, 0, 1, 1),
            reservoir=reservoir,
            lease=template,
            evidence_mode="direct_year",
            expected_evidence_tasks=0,
        )
        runtime = SourceProducer(
            baseline=self.baseline,
            control_store=self.control,
            evidence_store=self.evidence,
            scheduler=GlobalScheduler(CreditLedger({"wayback": 1})),
            candidates=[candidate],
            adapters={adapter.adapter_id: adapter},
            backlog_capacities={"wayback": 1},
            queue_capacities={
                "source_records": 4,
                "observations": 4,
                "evidence_tasks": 4,
                "commits": 4,
            },
            baseline_batch_size=10,
        )

        report = runtime.run_once()

        self.assertEqual(report.direct_capsules_committed, 1)
        self.assertEqual(self.evidence.count(), 1)
        self.assertEqual(report.evidence_tasks_enqueued, 0)

    def test_full_backlog_blocks_source_before_adapter_execution(self):
        occupied = EvidenceQueryKey(
            "occupied.example", TemporalScope(1997, 1997), "wayback", "cdx-v1"
        )
        self.control.enqueue_evidence_tasks([occupied])
        runtime, adapter = self.build_runtime(backlog_capacity=1)

        report = runtime.run_once()

        self.assertEqual(report.leases_succeeded, 0)
        self.assertTrue(report.admission_blocked)
        self.assertEqual(adapter.executions, 0)
        stored = self.control.get_reservoir("fixture-reservoir")
        self.assertEqual(stored.state, ReservoirState.READY)

    def test_conservative_reservation_rejects_lease_larger_than_remaining_capacity(self):
        runtime, adapter = self.build_runtime(backlog_capacity=1, expected_tasks=2)

        report = runtime.run_once()

        self.assertEqual(report.leases_succeeded, 0)
        self.assertTrue(report.admission_blocked)
        self.assertEqual(adapter.executions, 0)


if __name__ == "__main__":
    unittest.main()
