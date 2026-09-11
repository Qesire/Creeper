import hashlib
import unittest

from creeper.authority.baseline_index import ALL_YEAR_MASK, YEAR_BITS
from creeper.evidence.policies import EvidenceQueryKey
from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import HostObservation


class EvidencePlannerTests(unittest.TestCase):
    def observation(self, **changes):
        values = {
            "hostname": "new.example",
            "source_id": "source-a",
            "locator": "source-a:17",
            "scope": CandidateSourceScope.LOCAL_DISCOVERY,
        }
        values.update(changes)
        return HostObservation(**values)

    def test_authorized_direct_year_creates_capsule_without_external_query(self):
        from creeper.evidence.planner import EvidencePlanner

        plan = EvidencePlanner().plan(
            self.observation(direct_year_mask=YEAR_BITS[1997]),
            official_mask=0,
            local_mask=0,
            provider="wayback",
            policy_version="v1",
            allow_direct=True,
        )

        self.assertEqual([item.year for item in plan.direct_capsules], [1997])
        self.assertEqual(plan.external_keys, ())
        capsule = plan.direct_capsules[0]
        self.assertEqual(capsule.hostname, "new.example")
        self.assertEqual(capsule.provider, "direct:source-a")
        self.assertEqual(capsule.temporal_semantics, "source_direct_year")
        self.assertEqual(capsule.source_locator, "source-a:17")
        self.assertEqual(
            capsule.payload_hash,
            hashlib.sha256(
                b"new.example\x001997\x00source-a\x00source-a:17\x00\x00"
            ).hexdigest(),
        )

    def test_unauthorized_direct_claim_is_demoted_to_external_hint(self):
        from creeper.evidence.planner import EvidencePlanner

        plan = EvidencePlanner().plan(
            self.observation(direct_year_mask=YEAR_BITS[1997]),
            official_mask=0,
            local_mask=0,
            provider="wayback",
            policy_version="v1",
        )

        self.assertEqual(plan.direct_capsules, ())
        self.assertEqual(
            [(key.hostname, key.temporal_scope.year_from) for key in plan.external_keys],
            [("new.example", 1997)],
        )

    def test_isc_reference_can_never_become_direct_evidence(self):
        from creeper.evidence.planner import EvidencePlanner

        plan = EvidencePlanner().plan(
            self.observation(
                scope=CandidateSourceScope.ISC_REFERENCE,
                source_id="network_wizards:1997",
                direct_year_mask=YEAR_BITS[1997],
            ),
            official_mask=0,
            local_mask=0,
            provider="wayback",
            policy_version="v1",
            allow_direct=True,
        )

        self.assertEqual(plan.direct_capsules, ())
        self.assertEqual(
            [(key.temporal_scope.year_from, key.temporal_scope.year_to) for key in plan.external_keys],
            [(1997, 1997)],
        )

    def test_official_and_local_masks_suppress_direct_and_external_outputs(self):
        from creeper.evidence.planner import EvidencePlanner

        plan = EvidencePlanner().plan(
            self.observation(
                direct_year_mask=YEAR_BITS[1997],
                year_hint_mask=YEAR_BITS[1998],
                source_year=1999,
            ),
            official_mask=YEAR_BITS[1997],
            local_mask=YEAR_BITS[1998] | YEAR_BITS[1999],
            provider="wayback",
            policy_version="v1",
            allow_direct=True,
        )

        self.assertEqual(plan.direct_capsules, ())
        self.assertEqual(plan.external_keys, ())

    def test_hints_and_legacy_source_year_create_external_keys(self):
        from creeper.evidence.planner import EvidencePlanner

        plan = EvidencePlanner().plan(
            self.observation(year_hint_mask=YEAR_BITS[1998], source_year=1999),
            official_mask=0,
            local_mask=0,
            provider="wayback",
            policy_version="v1",
        )

        self.assertEqual(
            [(key.hostname, key.temporal_scope.year_from) for key in plan.external_keys],
            [("new.example", 1998)],
        )
        self.assertEqual(plan.external_keys[0].temporal_scope.year_to, 1999)
        self.assertEqual(plan.direct_capsules, ())

    def test_undated_hostname_plans_full_competition_year_range(self):
        from creeper.evidence.planner import EvidencePlanner

        plan = EvidencePlanner().plan(
            self.observation(),
            official_mask=YEAR_BITS[1997],
            local_mask=0,
            provider="wayback",
            policy_version="v1",
        )

        self.assertEqual(plan.direct_capsules, ())
        self.assertEqual(
            [
                (key.temporal_scope.year_from, key.temporal_scope.year_to)
                for key in plan.external_keys
            ],
            [(1996, 1996), (1998, 2001)],
        )
        self.assertEqual(
            sum(
                1 << (year - 1996)
                for key in plan.external_keys
                for year in range(
                    key.temporal_scope.year_from,
                    key.temporal_scope.year_to + 1,
                )
            ),
            ALL_YEAR_MASK & ~YEAR_BITS[1997],
        )

    def test_contiguous_missing_years_are_one_range_and_gaps_are_separate(self):
        from creeper.evidence.planner import EvidencePlanner

        plan = EvidencePlanner().plan(
            self.observation(year_hint_mask=YEAR_BITS[1996] | YEAR_BITS[1997] | YEAR_BITS[2000] | YEAR_BITS[2001]),
            official_mask=0,
            local_mask=0,
            provider="wayback",
            policy_version="v1",
        )

        self.assertEqual(
            [
                (key.temporal_scope.year_from, key.temporal_scope.year_to)
                for key in plan.external_keys
            ],
            [(1996, 1997), (2000, 2001)],
        )

    def test_provider_coverage_suppresses_external_query_but_not_direct_evidence(self):
        from creeper.evidence.planner import EvidencePlanner

        covered = YEAR_BITS[1997]
        external = EvidencePlanner().plan(
            self.observation(year_hint_mask=YEAR_BITS[1997]),
            official_mask=0,
            local_mask=0,
            provider="wayback",
            policy_version="v1",
            external_covered_mask=covered,
        )
        direct = EvidencePlanner().plan(
            self.observation(
                direct_year_mask=YEAR_BITS[1997],
                year_hint_mask=YEAR_BITS[1997],
            ),
            official_mask=0,
            local_mask=0,
            provider="wayback",
            policy_version="v1",
            allow_direct=True,
            external_covered_mask=covered,
        )

        self.assertEqual(external.external_keys, ())
        self.assertEqual([capsule.year for capsule in direct.direct_capsules], [1997])
        self.assertEqual(direct.external_keys, ())

    def test_authorized_direct_year_takes_precedence_over_same_year_hint(self):
        from creeper.evidence.planner import EvidencePlanner

        plan = EvidencePlanner().plan(
            self.observation(
                direct_year_mask=YEAR_BITS[1997],
                year_hint_mask=YEAR_BITS[1997],
                source_year=1997,
            ),
            official_mask=0,
            local_mask=0,
            provider="arquivo",
            policy_version="v2",
            allow_direct=True,
        )

        self.assertEqual([item.year for item in plan.direct_capsules], [1997])
        self.assertEqual(plan.external_keys, ())

    def test_plan_is_immutable_and_query_key_carries_provider_policy(self):
        from creeper.evidence.planner import EvidencePlan, EvidencePlanner

        plan = EvidencePlanner().plan(
            self.observation(year_hint_mask=YEAR_BITS[2001]),
            official_mask=0,
            local_mask=0,
            provider="arquivo",
            policy_version="policy-3",
        )

        self.assertIsInstance(plan, EvidencePlan)
        self.assertIsInstance(plan.external_keys[0], EvidenceQueryKey)
        self.assertEqual(plan.external_keys[0].provider, "arquivo")
        self.assertEqual(plan.external_keys[0].policy_version, "policy-3")
        with self.assertRaises(AttributeError):
            plan.external_keys = ()


if __name__ == "__main__":
    unittest.main()
