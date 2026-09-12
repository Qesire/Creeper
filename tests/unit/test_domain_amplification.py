from __future__ import annotations

import unittest

from creeper.evidence.domain_amplification import (
    DOMAIN_AMPLIFICATION_POLICY_VERSION,
    amplification_parent,
    plan_domain_amplification,
)


class DomainAmplificationPlannerTests(unittest.TestCase):
    def test_parent_skips_country_code_suffixes(self):
        self.assertEqual(amplification_parent("www.example.com"), "example.com")
        self.assertIsNone(amplification_parent("www.example.co.uk"))
        self.assertIsNone(amplification_parent("example.com"))

    def test_requires_observed_root_and_minimum_fanout(self):
        hosts = [
            "example.com",
            "a.example.com",
            "b.example.com",
            "c.example.com",
            "d.example.com",
        ]
        keys = plan_domain_amplification(
            hosts,
            provider="wayback",
            min_distinct_hosts=4,
            eligible_hostnames={"a.example.com"},
        )
        self.assertEqual(len(keys), 1)
        self.assertEqual(keys[0].hostname, "example.com")
        self.assertEqual(keys[0].policy_version, DOMAIN_AMPLIFICATION_POLICY_VERSION)
        self.assertEqual(
            (keys[0].temporal_scope.year_from, keys[0].temporal_scope.year_to),
            (1996, 2001),
        )

        self.assertEqual(
            plan_domain_amplification(
                hosts[1:],
                provider="wayback",
                min_distinct_hosts=4,
                eligible_hostnames={"a.example.com"},
            ),
            (),
        )

    def test_skips_group_without_unresolved_member(self):
        hosts = [
            "example.com",
            "a.example.com",
            "b.example.com",
            "c.example.com",
            "d.example.com",
        ]
        self.assertEqual(
            plan_domain_amplification(
                hosts,
                provider="wayback",
                min_distinct_hosts=4,
                eligible_hostnames={"outside.example"},
            ),
            (),
        )


if __name__ == "__main__":
    unittest.main()
