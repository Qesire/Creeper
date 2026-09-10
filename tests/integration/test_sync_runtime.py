import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.policies import CDXQueryState
from creeper.scheduler.credits import CreditLedger
from creeper.scheduler.global_scheduler import GlobalScheduler
from creeper.scheduler.leases import LeaseResult, WorkLease
from creeper.scheduler.priority import LeaseCandidate, ResourceCost
from creeper.sources.domains import DomainState, SourceDomain
from creeper.sources.reservoirs import Reservoir, ReservoirState
from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import HostObservation, SourceRecord
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore
from creeper.runtime.pipeline import SyncRuntime


class FakeLocalReservoir:
    adapter_id = "fixture-local"

    def __init__(self, records):
        self.records = tuple(records)
        self.leases = []

    def estimate(self):
        from creeper.sources.reservoirs import ReservoirEstimate

        return ReservoirEstimate(capacity_lower=len(self.records), sampled_records=len(self.records))

    def execute(self, lease: WorkLease):
        self.leases.append(lease)
        records = iter(self.records[: lease.max_records])
        return records, LeaseResult(
            lease_id=lease.lease_id,
            records=min(len(self.records), lease.max_records),
            requests=1,
            bytes_read=sum(len(record.payload.encode("utf-8")) for record in self.records[: lease.max_records]),
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


class SyncRuntimeIntegrationTests(unittest.TestCase):
    def test_run_once_filters_baseline_and_commits_one_fake_evidence_capsule(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_root = root / "task"
            baseline_root = task_root / "merged260909-3"
            baseline_root.mkdir(parents=True)
            for year in range(1996, 2002):
                (baseline_root / f"{year}.txt").write_text(
                    "baseline.example\n" if year == 1997 else "",
                    encoding="utf-8",
                )
            (baseline_root / "candidate_pool.txt").write_text("", encoding="utf-8")
            baseline = BaselineIndex.build(task_root, root / "baseline.sqlite3")

            records = [
                SourceRecord(
                    source_id="fixture-local",
                    locator="fixture://records/1",
                    payload="baseline.example",
                    scope=CandidateSourceScope.LOCAL_DISCOVERY,
                    source_year=1997,
                ),
                SourceRecord(
                    source_id="fixture-local",
                    locator="fixture://records/2",
                    payload="novel.example",
                    scope=CandidateSourceScope.LOCAL_DISCOVERY,
                    source_year=1997,
                ),
            ]
            adapter = FakeLocalReservoir(records)
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
                capacity_lower=2,
                capacity_upper=2,
                evidence_mode="discovery_only",
                state=ReservoirState.READY,
            )
            lease = WorkLease.create(
                reservoir_id=reservoir.reservoir_id,
                cursor_start="1",
                cursor_end="2",
                max_records=2,
                max_requests=1,
                max_bytes=1024,
                max_seconds=30,
                expected_evidence_tasks=1,
                expected_novel_eed=1.0,
            )
            candidate = LeaseCandidate(
                reservoir_id=reservoir.reservoir_id,
                reservoir=reservoir,
                lease=lease,
                expected_novel_eed=1.0,
                expected_evidence_tasks=1,
                costs=ResourceCost(
                    general_network=0,
                    evidence_network=1,
                    cpu=1,
                    ssd=1,
                ),
            )

            control = ControlStore(root / "control.sqlite3")
            control.save_domain(domain)
            control.save_reservoir(reservoir)
            evidence = EvidenceStore(root / "evidence.sqlite3")
            calls = []

            def transport(hostname: str, year: int):
                calls.append((hostname, year))
                return [
                    ([
                        {
                            "timestamp": "19970101000000",
                            "original": f"http://{hostname}/",
                            "status": "200",
                        }
                    ], True)
                ]

            scheduler = GlobalScheduler(CreditLedger({"wayback": 1}))
            runtime = SyncRuntime(
                baseline=baseline,
                control_store=control,
                evidence_store=evidence,
                scheduler=scheduler,
                candidates=[candidate],
                adapters={adapter.adapter_id: adapter},
                evidence_transport=transport,
                queue_capacities={
                    "source_records": 2,
                    "observations": 2,
                    "evidence_tasks": 2,
                    "commits": 2,
                },
                evidence_provider="wayback",
                evidence_policy_version="cdx-v1",
            )

            report = runtime.run_once()

            self.assertEqual(report.leases_succeeded, 1)
            self.assertEqual(report.evidence_tasks_completed, 1)
            self.assertEqual(report.evidence_capsules_committed, 1)
            self.assertLessEqual(report.max_evidence_queue_depth, 2)
            self.assertEqual(calls, [("novel.example", 1997)])
            self.assertEqual(evidence.count(), 1)
            self.assertEqual(len(adapter.leases), 1)
            self.assertEqual(control.get_lease(lease.lease_id).state.value, "SUCCEEDED")

            evidence.close()
            control.close()
            baseline.close()


if __name__ == "__main__":
    unittest.main()
