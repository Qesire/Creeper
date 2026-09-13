from __future__ import annotations

import tempfile
import tomllib
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.contracts import (
    EvidenceAuthority,
    SourceEvidenceContract,
    bind_contract_to_adapter_id,
)
from creeper.evidence.policies import EvidenceQueryKey, TemporalScope
from creeper.runtime.source_producer import SourceProducer
from creeper.scheduler.credits import CreditLedger
from creeper.scheduler.global_scheduler import GlobalScheduler
from creeper.scheduler.leases import WorkLease
from creeper.scheduler.priority import LeaseCandidate, ResourceCost
from creeper.sources.domains import DomainState, SourceDomain
from creeper.sources.production import ProductionAdapterFactory
from creeper.sources.reservoirs import Reservoir, ReservoirState
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class V4ProductionAuthorityConfigTests(unittest.TestCase):
    def test_parallel_owned_configs_use_current_v4_authority(self) -> None:
        root = Path(__file__).resolve().parents[2]
        autopilot = tomllib.loads(
            (root / "conf" / "autopilot.example.toml").read_text(
                encoding="utf-8"
            )
        )
        activated = tomllib.loads(
            (root / "conf" / "creeper.activated.example.toml").read_text(
                encoding="utf-8"
            )
        )

        self.assertEqual(
            autopilot["readiness"]["baseline_eed"],
            "46483739.2890",
        )
        self.assertIn(
            "v4-merged260912-3",
            activated["runtime_data_root"],
        )
        self.assertIn(
            "v4-merged260912-3",
            activated["baseline_index"],
        )
        self.assertNotIn(
            "merged260909-3",
            activated["runtime_data_root"],
        )
        self.assertNotIn(
            "merged260909-3",
            activated["baseline_index"],
        )

    def test_warc_direct_contract_fails_closed_until_adapter_supports_it(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "warc_arc DIRECT_WEB_YEAR contracts are not supported",
        ):
            SourceEvidenceContract(
                contract_id="reviewed-warc-v1",
                authority=EvidenceAuthority.DIRECT_WEB_YEAR,
                parser_kind="warc_arc",
                temporal_semantics="warc_capture_timestamp",
                evidence_type="reviewed_historical_web_record",
                hostname_field="target_uri",
                timestamp_field="warc_date",
                policy_version="reviewed-warc-policy-v1",
            )


class V4DirectRoutingIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        task_root = self.root / "task"
        baseline_root = task_root / "merged260912-3"
        baseline_root.mkdir(parents=True)
        for year in range(1996, 2002):
            (baseline_root / f"{year}.txt").write_text("", encoding="utf-8")
        (baseline_root / "candidate_pool.txt").write_text("", encoding="utf-8")
        self.baseline = BaselineIndex.build(
            task_root,
            self.root / "baseline.sqlite3",
        )
        self.control = ControlStore(self.root / "control.sqlite3")
        self.evidence = EvidenceStore(self.root / "evidence.sqlite3")

    def tearDown(self) -> None:
        self.evidence.close()
        self.control.close()
        self.baseline.close()
        self.tmp.cleanup()

    def _runtime_for_csv(
        self,
        *,
        trusted: bool,
        owner: str,
    ) -> tuple[SourceProducer, object, Reservoir]:
        source = self.root / ("trusted.csv" if trusted else "untrusted.csv")
        source.write_text(
            "https://v4-direct.example/a,1999\n",
            encoding="utf-8",
        )

        contract = None
        if trusted:
            contract = SourceEvidenceContract(
                contract_id="reviewed-v4-csv-v1",
                authority=EvidenceAuthority.DIRECT_WEB_YEAR,
                parser_kind="delimited",
                temporal_semantics="reviewed_web_observation_timestamp",
                evidence_type="reviewed_historical_web_record",
                hostname_field="column:0",
                timestamp_field="column:1",
                policy_version="reviewed-v4-csv-policy-v1",
            )

        adapter_id = (
            bind_contract_to_adapter_id("structured:v4-csv", contract)
            if contract is not None
            else "structured:v4-csv"
        )
        domain = SourceDomain(
            domain_id=f"domain:v4-csv-{owner}",
            family="REVIEWED_WEB_DATASET" if trusted else "UNREVIEWED_DATASET",
            discovery_mechanism="integration-test",
            temporal_scope=(1996, 2001),
            state=DomainState.EXPLORING,
        )
        reservoir = Reservoir(
            reservoir_id=f"reservoir:v4-csv-{owner}",
            domain_id=domain.domain_id,
            adapter_id=adapter_id,
            root_locator=str(source),
            enumeration_kind="structured_records",
            capacity_lower=1,
            capacity_upper=1,
            evidence_mode="direct_year" if trusted else "discovery_only",
            state=ReservoirState.READY,
        )
        self.control.save_domain(domain)
        self.control.save_reservoir(reservoir)
        adapter = ProductionAdapterFactory.open(
            reservoir,
            temporal_scope=(1996, 2001),
        )
        lease = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            cursor_start="byte:0",
            max_records=2,
            max_requests=1,
            max_bytes=4096,
            max_seconds=30,
            expected_evidence_tasks=100 if trusted else 1,
            expected_novel_eed=1.0,
        )
        candidate = LeaseCandidate(
            reservoir_id=reservoir.reservoir_id,
            expected_novel_eed=1.0,
            costs=ResourceCost(0, 100, 1, 1),
            reservoir=reservoir,
            lease=lease,
            evidence_mode=reservoir.evidence_mode,
            evidence_provider="wayback",
            expected_evidence_tasks=100 if trusted else 1,
            reservation_evidence_tasks=100 if trusted else 1,
        )
        runtime = SourceProducer(
            baseline=self.baseline,
            control_store=self.control,
            evidence_store=self.evidence,
            scheduler=GlobalScheduler(CreditLedger({"wayback": 0})),
            candidates=[candidate],
            adapters={adapter_id: adapter},
            backlog_capacities={"wayback": 1},
            queue_capacities={
                "source_records": 2,
                "observations": 2,
                "evidence_tasks": 2,
                "commits": 2,
            },
            owner=owner,
        )
        return runtime, adapter, reservoir

    def _saturate_wayback(self) -> None:
        self.control.enqueue_evidence_tasks(
            [
                EvidenceQueryKey(
                    "occupied.example",
                    TemporalScope(1997, 1997),
                    "wayback",
                    "cdx-v1",
                )
            ]
        )

    def test_reviewed_direct_contract_progresses_with_wayback_full(self) -> None:
        self._saturate_wayback()
        runtime, adapter, reservoir = self._runtime_for_csv(
            trusted=True,
            owner="trusted",
        )
        try:
            report = runtime.run_once()
        finally:
            close = getattr(adapter, "close", None)
            if callable(close):
                close()

        self.assertEqual(report.leases_succeeded, 1)
        self.assertEqual(report.direct_capsules_committed, 1)
        self.assertEqual(report.evidence_tasks_enqueued, 0)
        self.assertEqual(self.evidence.count(), 1)
        self.assertEqual(
            self.control.get_reservoir(reservoir.reservoir_id).state,
            ReservoirState.EXHAUSTED,
        )

    def test_unreviewed_dated_csv_does_not_bypass_wayback_backpressure(self) -> None:
        self._saturate_wayback()
        runtime, adapter, reservoir = self._runtime_for_csv(
            trusted=False,
            owner="untrusted",
        )
        try:
            report = runtime.run_once()
        finally:
            close = getattr(adapter, "close", None)
            if callable(close):
                close()

        self.assertEqual(report.leases_succeeded, 0)
        self.assertTrue(report.admission_blocked)
        self.assertEqual(report.direct_capsules_committed, 0)
        self.assertEqual(self.evidence.count(), 0)
        self.assertEqual(
            self.control.get_reservoir(reservoir.reservoir_id).state,
            ReservoirState.READY,
        )


if __name__ == "__main__":
    unittest.main()
