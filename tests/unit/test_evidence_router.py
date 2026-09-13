from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.evidence.policies import EvidenceQueryKey, TemporalScope
from creeper.evidence.router import EvidenceRouter
from creeper.scheduler.admission import EvidenceBacklogAdmission
from creeper.scheduler.leases import WorkLease
from creeper.sources.domains import DomainState, SourceDomain
from creeper.sources.reservoirs import Reservoir, ReservoirState
from creeper.storage.control_store import ControlStore


class EvidenceRouterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.control = ControlStore(Path(self.tmp.name) / "control.sqlite3")
        self.admission = EvidenceBacklogAdmission(self.control)
        domain = SourceDomain(
            domain_id="router-domain",
            family="TEST",
            discovery_mechanism="test",
            temporal_scope=(1996, 2001),
            state=DomainState.EXPLORING,
        )
        reservoir = Reservoir(
            reservoir_id="router-reservoir",
            domain_id=domain.domain_id,
            adapter_id="fixture",
            root_locator="fixture://router",
            enumeration_kind="finite_list",
            capacity_lower=1,
            capacity_upper=1,
            evidence_mode="direct_year",
            state=ReservoirState.READY,
        )
        self.control.save_domain(domain)
        self.control.save_reservoir(reservoir)
        self.lease = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            max_records=1,
            max_requests=1,
            max_bytes=1024,
            max_seconds=30,
        )
        self.control.save_lease(self.lease)

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    @staticmethod
    def key(
        hostname: str,
        *,
        provider: str = "wayback",
        year_from: int = 1997,
        year_to: int = 1997,
        policy_version: str = "cdx-v1",
    ) -> EvidenceQueryKey:
        return EvidenceQueryKey(
            hostname,
            TemporalScope(year_from, year_to),
            provider,
            policy_version,
        )

    def router(self, capacities: dict[str, int]) -> EvidenceRouter:
        return EvidenceRouter(
            self.control,
            self.admission,
            backlog_capacities=capacities,
        )

    def enqueue(
        self,
        router: EvidenceRouter,
        keys: list[EvidenceQueryKey],
        *,
        preferred_provider: str | None = None,
    ):
        return router.enqueue_or_stage(
            keys,
            source_key="source-a",
            reservoir_id="router-reservoir",
            lease_id=self.lease.lease_id,
            ttl_seconds=60,
            preferred_provider=preferred_provider,
        )

    def test_provider_admission_failure_is_durable_and_retryable(self) -> None:
        occupied = self.key("occupied.example")
        self.control.enqueue_evidence_tasks([occupied])
        router = self.router({"wayback": 1})
        wanted = self.key("wanted.example")

        result = self.enqueue(router, [wanted])

        self.assertEqual(result.enqueued, 0)
        self.assertEqual(result.staged, 1)
        self.assertEqual(router.pending_count(provider="wayback"), 1)
        self.assertIsNone(self.control.get_evidence_task(wanted))

        with self.control.connection:
            self.control.connection.execute(
                "DELETE FROM evidence_tasks WHERE hostname = ?",
                ("occupied.example",),
            )

        self.assertEqual(router.flush_pending(ttl_seconds=60), 1)
        self.assertEqual(router.pending_count(provider="wayback"), 0)
        self.assertIsNotNone(self.control.get_evidence_task(wanted))

    def test_wayback_saturation_does_not_block_rdap_lane(self) -> None:
        occupied = self.key("occupied.example")
        self.control.enqueue_evidence_tasks([occupied])
        router = self.router({"wayback": 1, "rdap": 1})
        wayback = self.key("fallback.example")
        rdap = self.key(
            "example.com",
            provider="rdap",
            year_from=1996,
            year_to=2001,
            policy_version="rdap-registration-v1",
        )

        result = self.enqueue(
            router,
            [wayback, rdap],
            preferred_provider="wayback",
        )

        self.assertEqual(result.enqueued, 1)
        self.assertEqual(router.pending_count(provider="wayback"), 1)
        self.assertEqual(router.pending_count(provider="rdap"), 0)
        self.assertIsNone(self.control.get_evidence_task(wayback))
        self.assertIsNotNone(self.control.get_evidence_task(rdap))

    def test_route_preserves_specialized_rdap_provider(self) -> None:
        router = self.router({"wayback": 1, "rdap": 1})
        rdap = self.key(
            "example.com",
            provider="rdap",
            year_from=1996,
            year_to=2001,
            policy_version="rdap-registration-v1",
        )

        routed = router.route_external(
            [rdap],
            preferred_provider="wayback",
        )

        self.assertEqual(len(routed), 1)
        self.assertEqual(routed[0].provider, "rdap")

    def test_route_changes_scheduling_provider_not_query_scope_or_policy(self) -> None:
        router = self.router({"wayback": 1, "archive-alt": 1})
        key = self.key(
            "novel.example",
            year_from=1998,
            year_to=2000,
            policy_version="cdx-v1",
        )

        routed = router.route_external(
            [key],
            preferred_provider="archive-alt",
        )[0]

        self.assertEqual(routed.hostname, key.hostname)
        self.assertEqual(routed.temporal_scope, key.temporal_scope)
        self.assertEqual(routed.policy_version, key.policy_version)
        self.assertEqual(routed.provider, "archive-alt")


if __name__ == "__main__":
    unittest.main()
