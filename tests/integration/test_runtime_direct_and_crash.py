import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex, YEAR_BITS
from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceCapsule,
    EvidenceQueryKey,
    TemporalScope,
)
from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import HostObservation, SourceRecord
from creeper.runtime.pipeline import SyncRuntime
from creeper.scheduler.credits import CreditLedger
from creeper.scheduler.global_scheduler import GlobalScheduler
from creeper.scheduler.leases import LeaseResult, WorkLease
from creeper.scheduler.priority import LeaseCandidate, ResourceCost
from creeper.sources.domains import DomainState, SourceDomain
from creeper.sources.reservoirs import Reservoir, ReservoirState
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class DirectYearReservoir:
    adapter_id = "direct-year-fixture"

    def __init__(self):
        self.executed_leases = []

    def execute(self, lease: WorkLease):
        self.executed_leases.append(lease)
        record = SourceRecord(
            source_id=self.adapter_id,
            locator="fixture://direct/1",
            payload="direct.example",
            scope=CandidateSourceScope.LOCAL_DISCOVERY,
            direct_year_mask=YEAR_BITS[1997],
        )
        return iter((record,)), LeaseResult(
            lease_id=lease.lease_id,
            records=1,
            requests=1,
            bytes_read=len(record.payload),
            elapsed_seconds=0.001,
            next_cursor=None,
        )

    def extract_hosts(self, record: SourceRecord):
        yield HostObservation(
            hostname=record.payload,
            source_id=record.source_id,
            locator=record.locator,
            scope=record.scope,
            direct_year_mask=record.direct_year_mask,
        )


class RuntimeDirectAndCrashIntegrationTests(unittest.TestCase):
    def _empty_baseline(self, root: Path):
        task_root = root / "task"
        baseline_root = task_root / "merged260909-3"
        baseline_root.mkdir(parents=True)
        for year in range(1996, 2002):
            (baseline_root / f"{year}.txt").write_text("", encoding="utf-8")
        (baseline_root / "candidate_pool.txt").write_text("", encoding="utf-8")
        return BaselineIndex.build(task_root, root / "baseline.sqlite3")

    def test_direct_year_commits_without_external_evidence_capacity_or_transport(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline = self._empty_baseline(root)
            control = ControlStore(root / "control.sqlite3")
            evidence = EvidenceStore(root / "evidence.sqlite3")
            adapter = DirectYearReservoir()
            domain = SourceDomain(
                domain_id="direct-year-domain",
                family="LOCAL_FIXTURE",
                discovery_mechanism="integration fixture",
                temporal_scope=(1996, 2001),
                state=DomainState.EXPLORING,
            )
            reservoir = Reservoir(
                reservoir_id="direct-year-reservoir",
                domain_id=domain.domain_id,
                adapter_id=adapter.adapter_id,
                root_locator="fixture://direct",
                enumeration_kind="finite_list",
                capacity_lower=1,
                capacity_upper=1,
                evidence_mode="direct_year",
                state=ReservoirState.READY,
            )
            lease = WorkLease.create(
                reservoir_id=reservoir.reservoir_id,
                max_records=1,
                max_requests=1,
                max_bytes=1024,
                max_seconds=30,
                expected_evidence_tasks=0,
                expected_novel_eed=1.0,
            )
            candidate = LeaseCandidate(
                reservoir_id=reservoir.reservoir_id,
                reservoir=reservoir,
                lease=lease,
                expected_novel_eed=1.0,
                expected_evidence_tasks=0,
                evidence_mode="direct_year",
                evidence_provider="wayback",
                costs=ResourceCost(
                    general_network=0,
                    evidence_network=0,
                    cpu=1,
                    ssd=1,
                ),
            )
            control.save_domain(domain)
            control.save_reservoir(reservoir)
            transport_calls = []

            def transport(hostname: str, year: int):
                transport_calls.append((hostname, year))
                raise AssertionError("direct-year evidence must not use external transport")

            runtime = SyncRuntime(
                baseline=baseline,
                control_store=control,
                evidence_store=evidence,
                scheduler=GlobalScheduler(CreditLedger({"wayback": 0})),
                candidates=[candidate],
                adapters={adapter.adapter_id: adapter},
                evidence_transport=transport,
                queue_capacities={
                    "source_records": 1,
                    "observations": 1,
                    "evidence_tasks": 1,
                    "commits": 1,
                },
                evidence_provider="wayback",
                evidence_policy_version="cdx-v1",
            )

            try:
                report = runtime.run_once()

                self.assertEqual(report.source_records, 1)
                self.assertEqual(report.observations, 1)
                self.assertEqual(report.evidence_tasks_enqueued, 0)
                self.assertEqual(report.evidence_tasks_completed, 0)
                self.assertEqual(report.evidence_capsules_committed, 1)
                self.assertEqual(transport_calls, [])
                self.assertEqual(control.list_evidence_tasks(), [])
                capsules = evidence.all_capsules()
                self.assertEqual(len(capsules), 1)
                self.assertEqual(capsules[0].hostname, "direct.example")
                self.assertEqual(capsules[0].year, 1997)
                self.assertEqual(capsules[0].provider, "direct:direct-year-fixture")
            finally:
                evidence.close()
                control.close()
                baseline.close()

    def test_persisted_capsule_survives_expired_task_recovery_and_duplicate_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            control_path = root / "control.sqlite3"
            evidence_path = root / "evidence.sqlite3"
            now = [100.0]
            key = EvidenceQueryKey(
                "crash.example",
                TemporalScope(1997, 1997),
                "wayback",
                "cdx-v1",
            )
            capsule = EvidenceCapsule(
                hostname=key.hostname,
                year=1997,
                provider=key.provider,
                temporal_semantics="capture_timestamp_year",
                evidence_timestamp="19970101000000",
                source_locator="http://crash.example/",
                payload_hash="a" * 64,
                policy_version=key.policy_version,
            )

            control = ControlStore(
                control_path,
                default_lease_seconds=5.0,
                clock=lambda: now[0],
            )
            evidence = EvidenceStore(evidence_path)
            try:
                self.assertEqual(control.enqueue_evidence_tasks([key]), 1)
                claimed = control.claim_evidence_tasks(owner="old-owner", limit=1)
                self.assertEqual(len(claimed), 1)
                self.assertEqual(claimed[0].state, CDXQueryState.PENDING.value)
                evidence.put(capsule)
            finally:
                evidence.close()
                control.close()

            now[0] = 106.0
            reopened_control = ControlStore(
                control_path,
                default_lease_seconds=5.0,
                clock=lambda: now[0],
            )
            reopened_evidence = EvidenceStore(evidence_path)
            try:
                self.assertEqual(reopened_evidence.count(), 1)
                self.assertEqual(
                    reopened_evidence.for_hostname("crash.example"),
                    [capsule],
                )
                reclaimed = reopened_control.claim_evidence_tasks(
                    owner="new-owner", limit=1
                )
                self.assertEqual(len(reclaimed), 1)
                self.assertEqual(reclaimed[0].key, key)
                self.assertEqual(reclaimed[0].lease_owner, "new-owner")
                reopened_evidence.put(capsule)
                self.assertEqual(reopened_evidence.count(), 1)
            finally:
                reopened_evidence.close()
                reopened_control.close()


if __name__ == "__main__":
    unittest.main()
