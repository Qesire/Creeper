import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.policies import EvidenceQueryKey, TemporalScope
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
        )


class SourceProducerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        task_root = self.root / "task"
        baseline_root = task_root / "baseline-test"
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

    def build_runtime(self, *, backlog_capacity: int, expected_tasks: int = 1):
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
