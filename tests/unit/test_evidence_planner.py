import hashlib
import unittest

from creeper.authority.baseline_index import YEAR_BITS
from creeper.evidence.policies import EvidenceCapsule, EvidenceQueryKey
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

    def test_direct_year_creates_capsule_without_external_query(self):
        from creeper.evidence.planner import EvidencePlanner

        plan = EvidencePlanner().plan(
            self.observation(direct_year_mask=YEAR_BITS[1997]),
            official_mask=0,
            local_mask=0,
            provider="wayback",
            policy_version="v1",
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
            hashlib.sha256(b"new.example\x001997\x00source-a\x00source-a:17").hexdigest(),
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
            [("new.example", 1998), ("new.example", 1999)],
        )
        self.assertEqual(plan.direct_capsules, ())

    def test_direct_year_takes_precedence_over_same_year_hint(self):
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
