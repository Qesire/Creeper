import unittest

from creeper.scheduler.credits import CreditLedger
from creeper.scheduler.global_scheduler import GlobalScheduler
from creeper.scheduler.leases import LeaseState, WorkLease
from creeper.scheduler.priority import LeaseCandidate, ResourceCost
from creeper.sources.reservoirs import Reservoir, ReservoirState


def make_reservoir(reservoir_id: str, *, evidence_mode: str = "discovery_only") -> Reservoir:
    return Reservoir(
        reservoir_id=reservoir_id,
        domain_id="archive",
        adapter_id="fixture",
        root_locator=f"fixture://{reservoir_id}",
        enumeration_kind="range",
        capacity_lower=100,
        evidence_mode=evidence_mode,
        state=ReservoirState.READY,
    )


def make_lease(reservoir_id: str) -> WorkLease:
    return WorkLease.create(
        reservoir_id=reservoir_id,
        max_records=10,
        max_requests=2,
        max_bytes=4096,
        max_seconds=30,
        now=100.0,
        expires_at=130.0,
    )


class SchedulerEEDTests(unittest.TestCase):
    def test_rank_uses_expected_novel_eed_per_max_normalized_cost(self):
        scheduler = GlobalScheduler(
            CreditLedger({"wayback": 10}),
            resource_capacities={
                "general_network": 1,
                "evidence_network": 1,
                "cpu": 1,
                "ssd": 1,
            },
        )
        direct = LeaseCandidate(
            reservoir_id="direct",
            reservoir=make_reservoir("direct", evidence_mode="direct_year"),
            lease=make_lease("direct"),
            expected_novel_eed=10,
            costs=ResourceCost(general_network=1, evidence_network=0, cpu=1, ssd=1),
            evidence_mode="direct_year",
        )
        cdx_heavy = LeaseCandidate(
            reservoir_id="cdx",
            reservoir=make_reservoir("cdx"),
            lease=make_lease("cdx"),
            expected_novel_eed=20,
            costs=ResourceCost(general_network=1, evidence_network=100, cpu=1, ssd=1),
        )

        self.assertEqual(scheduler.rank([cdx_heavy, direct])[0].reservoir_id, "direct")

    def test_rank_breaks_equal_scores_by_reservoir_id(self):
        scheduler = GlobalScheduler(CreditLedger({"wayback": 10}))
        candidates = [
            LeaseCandidate("zeta", 10, ResourceCost(1, 0, 1, 1)),
            LeaseCandidate("alpha", 10, ResourceCost(1, 0, 1, 1)),
        ]

        self.assertEqual([c.reservoir_id for c in scheduler.rank(candidates)], ["alpha", "zeta"])

    def test_direct_year_grant_does_not_reserve_evidence_credit(self):
        ledger = CreditLedger({"wayback": 1})
        scheduler = GlobalScheduler(ledger)
        candidate = LeaseCandidate(
            reservoir_id="direct",
            reservoir=make_reservoir("direct", evidence_mode="direct_year"),
            lease=make_lease("direct"),
            expected_novel_eed=10,
            costs=ResourceCost(1, 0, 1, 1),
            evidence_mode="direct_year",
            expected_evidence_tasks=100,
        )

        granted = scheduler.grant_next([candidate], owner="worker-1")

        self.assertIsNotNone(granted)
        self.assertEqual(granted.state, LeaseState.GRANTED)
        self.assertEqual(ledger.balance("wayback").reserved, 0)
        self.assertEqual(scheduler.reservoir_state("direct"), ReservoirState.LEASED)

    def test_discovery_only_grant_is_rejected_when_credit_reservation_fails(self):
        ledger = CreditLedger({"wayback": 1})
        scheduler = GlobalScheduler(ledger)
        candidate = LeaseCandidate(
            reservoir_id="discovery",
            reservoir=make_reservoir("discovery"),
            lease=make_lease("discovery"),
            expected_novel_eed=10,
            costs=ResourceCost(1, 1, 1, 1),
            expected_evidence_tasks=2,
        )

        self.assertIsNone(scheduler.grant_next([candidate], owner="worker-1"))
        self.assertEqual(ledger.balance("wayback").reserved, 0)
        self.assertEqual(scheduler.reservoir_state("discovery"), ReservoirState.READY)


if __name__ == "__main__":
    unittest.main()
