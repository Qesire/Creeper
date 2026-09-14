import unittest

from creeper.evidence.policies import (
    CDXQueryState,
    DomainEvidenceQueryResult,
    EvidenceCapsule,
    EvidenceQueryKey,
    EvidenceQueryResult,
    RangeEvidenceQueryResult,
    TemporalScope,
)
from creeper.evidence.providers.cdx import query_missing_years, query_year


class EvidencePolicyTests(unittest.TestCase):
    def test_temporal_scope_rejects_fractional_and_boolean_years(self):
        with self.assertRaisesRegex(ValueError, "integers within"):
            TemporalScope(1996.5, 1997)
        with self.assertRaisesRegex(ValueError, "integers within"):
            TemporalScope(True, 1997)

    def test_query_key_requires_provider_and_policy_identity(self):
        scope = TemporalScope(1997, 1997)
        with self.assertRaisesRegex(ValueError, "provider"):
            EvidenceQueryKey("example.com", scope, "", "v1")
        with self.assertRaisesRegex(ValueError, "policy_version"):
            EvidenceQueryKey("example.com", scope, "wayback", "   ")

    def test_capsule_rejects_fractional_year_and_normalizes_hostname(self):
        with self.assertRaisesRegex(ValueError, "integer within"):
            EvidenceCapsule(
                hostname="example.com",
                year=1997.5,
                provider="wayback",
                temporal_semantics="capture_timestamp_year",
                evidence_timestamp="19970101000000",
                source_locator="http://example.com/",
                payload_hash="hash",
                policy_version="v1",
            )
        capsule = EvidenceCapsule(
            hostname=" EXAMPLE.COM ",
            year=1997,
            provider="wayback",
            temporal_semantics="capture_timestamp_year",
            evidence_timestamp="19970101000000",
            source_locator="http://example.com/",
            payload_hash="hash",
            policy_version="v1",
        )
        self.assertEqual(capsule.hostname, "example.com")

    def test_provider_result_accounting_requires_exact_nonnegative_integers(self):
        key = EvidenceQueryKey(
            "example.com",
            TemporalScope(1997, 1997),
            "wayback",
            "v1",
        )
        with self.assertRaisesRegex(ValueError, "non-negative integers"):
            EvidenceQueryResult(
                "example.com",
                1997,
                CDXQueryState.INCOMPLETE,
                provider_requests=1.5,
                key=key,
            )
        with self.assertRaisesRegex(ValueError, "non-negative integers"):
            DomainEvidenceQueryResult(
                "example.com",
                EvidenceQueryKey(
                    "example.com",
                    TemporalScope(1996, 1997),
                    "wayback",
                    "v1",
                ),
                CDXQueryState.DECOMPOSED,
                records_seen=True,
            )

    def test_range_result_rejects_fractional_or_duplicate_years(self):
        key = EvidenceQueryKey(
            "example.com",
            TemporalScope(1996, 2001),
            "wayback",
            "v1",
        )
        with self.assertRaisesRegex(ValueError, "integer years"):
            RangeEvidenceQueryResult(
                "example.com",
                key,
                CDXQueryState.DECOMPOSED,
                candidate_years=(1997.0,),
            )
        with self.assertRaisesRegex(ValueError, "must be unique"):
            RangeEvidenceQueryResult(
                "example.com",
                key,
                CDXQueryState.DECOMPOSED,
                followup_years=(1998, 1998),
            )

    def test_completed_second_empty_page_is_exhaustive(self):
        result = query_year("example.com", 1997, lambda h, y: [([], False), ([], True)])
        self.assertEqual(result.state, CDXQueryState.EMPTY_EXHAUSTIVE)

    def test_no_page_transport_is_incomplete(self):
        result = query_year("example.com", 1997, lambda h, y: [])
        self.assertEqual(result.state, CDXQueryState.INCOMPLETE)

    def test_query_key_normalizes_and_separates_provider_policy(self):
        scope = TemporalScope(1997, 1997)
        key = EvidenceQueryKey(" EXAMPLE.COM ", scope, "wayback", "v1")
        self.assertEqual(key.hostname, "example.com")
        self.assertNotEqual(key, EvidenceQueryKey("example.com", scope, "arquivo", "v1"))

    def test_multi_year_probe_propagates_provider_and_policy_to_result_keys(self):
        results = query_missing_years(
            "example.com",
            [1997],
            lambda h, y: [([], True)],
            provider="wayback",
            policy_version="v2",
        )
        self.assertEqual(
            results[0].key,
            EvidenceQueryKey("example.com", TemporalScope(1997, 1997), "wayback", "v2"),
        )

    def test_exact_host_and_exact_year_are_required(self):
        def transport(hostname, year):
            return [
                (
                    [
                        {"timestamp": "19970101000000", "original": "http://www.example.com/", "status": "200"},
                        {"timestamp": "19960101000000", "original": "http://example.com/", "status": "200"},
                    ],
                    True,
                )
            ]

        result = query_year("example.com", 1997, transport)
        self.assertEqual(result.state, CDXQueryState.EMPTY_EXHAUSTIVE)

    def test_success_produces_a_reproducible_capsule(self):
        row = {"timestamp": "19970101000000", "original": "http://example.com/", "status": "200"}
        transport = lambda hostname, year: [([row], True)]
        result = query_year("EXAMPLE.COM", 1997, transport)
        self.assertEqual(result.state, CDXQueryState.PASS)
        self.assertEqual(result.capsule.hostname, "example.com")
        self.assertEqual(result.capsule.year, 1997)
        self.assertEqual(len(result.capsule.payload_hash), 64)

    def test_incomplete_and_transient_are_not_empty(self):
        incomplete = query_year("example.com", 1997, lambda h, y: [([], False)])
        transient = query_year("example.com", 1997, lambda h, y: (_ for _ in ()).throw(TimeoutError("timeout")))
        self.assertEqual(incomplete.state, CDXQueryState.INCOMPLETE)
        self.assertEqual(transient.state, CDXQueryState.TRANSIENT_ERROR)

    def test_multi_year_probe_returns_independent_year_results(self):
        def transport(hostname, year):
            return [([{"timestamp": f"{year}0101000000", "original": "http://example.com/", "status": "200"}], True)]

        results = query_missing_years("example.com", [1996, 2001], transport)
        self.assertEqual([result.year for result in results], [1996, 2001])
        self.assertTrue(all(result.state is CDXQueryState.PASS for result in results))


if __name__ == "__main__":
    unittest.main()
