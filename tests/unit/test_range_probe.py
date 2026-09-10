import unittest

from creeper.evidence.policies import CDXQueryState
from creeper.evidence.providers.cdx import (
    contiguous_year_ranges,
    probe_range,
    query_missing_years,
)


class RangeProbeTests(unittest.TestCase):
    def test_contiguous_year_ranges_are_sorted_deduplicated_and_validated(self):
        self.assertEqual(
            contiguous_year_ranges([2001, 1997, 1996, 2000, 1997]),
            ((1996, 1997), (2000, 2001)),
        )
        with self.assertRaises(ValueError):
            contiguous_year_ranges([1995])

    def test_probe_range_collects_only_exact_host_success_years(self):
        calls = []

        def transport(hostname, year_from, year_to):
            calls.append((hostname, year_from, year_to))
            return [
                (
                    [
                        {
                            "timestamp": "19970101000000",
                            "original": "http://new.example/ok",
                            "status": "200",
                        },
                        {
                            "timestamp": "19980101000000",
                            "original": "http://other.example/",
                            "status": "200",
                        },
                        {
                            "timestamp": "19990101000000",
                            "original": "http://new.example/error",
                            "status": "404",
                        },
                    ],
                    True,
                )
            ]

        result = probe_range("NEW.EXAMPLE", 1996, 1999, transport)

        self.assertEqual(calls, [("new.example", 1996, 1999)])
        self.assertEqual(result.state, CDXQueryState.PASS)
        self.assertTrue(result.complete)
        self.assertEqual(result.candidate_years, (1997,))
        self.assertEqual((result.pages_seen, result.records_seen), (1, 3))

    def test_complete_range_probe_limits_exact_queries_to_hit_years(self):
        range_calls = []
        exact_calls = []

        def range_transport(hostname, year_from, year_to):
            range_calls.append((hostname, year_from, year_to))
            return [
                (
                    [{
                        "timestamp": "19970101000000",
                        "original": "http://new.example/",
                        "status": "200",
                    }],
                    True,
                )
            ]

        def exact_transport(hostname, year):
            exact_calls.append((hostname, year))
            return [
                (
                    [{
                        "timestamp": f"{year}0101000000",
                        "original": f"http://{hostname}/",
                        "status": "200",
                    }],
                    True,
                )
            ]

        results = query_missing_years(
            "NEW.EXAMPLE",
            [1996, 1997, 1998],
            exact_transport,
            range_transport=range_transport,
        )

        self.assertEqual(range_calls, [("new.example", 1996, 1998)])
        self.assertEqual(exact_calls, [("new.example", 1997)])
        self.assertEqual(
            [result.state for result in results],
            [
                CDXQueryState.EMPTY_EXHAUSTIVE,
                CDXQueryState.PASS,
                CDXQueryState.EMPTY_EXHAUSTIVE,
            ],
        )

    def test_incomplete_range_probe_falls_back_to_every_exact_year(self):
        exact_calls = []

        def range_transport(_hostname, _year_from, _year_to):
            return [([], False)]

        def exact_transport(hostname, year):
            exact_calls.append((hostname, year))
            return [([], True)]

        results = query_missing_years(
            "new.example",
            [1996, 1997],
            exact_transport,
            range_transport=range_transport,
        )

        self.assertEqual(exact_calls, [("new.example", 1996), ("new.example", 1997)])
        self.assertEqual(
            [result.state for result in results],
            [CDXQueryState.EMPTY_EXHAUSTIVE, CDXQueryState.EMPTY_EXHAUSTIVE],
        )

    def test_incomplete_range_with_partial_hit_still_exact_probes_every_year(self):
        exact_calls = []

        def range_transport(_hostname, _year_from, _year_to):
            return [
                (
                    [{
                        "timestamp": "19970101000000",
                        "original": "http://new.example/",
                        "status": "200",
                    }],
                    False,
                )
            ]

        probe = probe_range("new.example", 1996, 1998, range_transport)
        self.assertEqual(probe.state, CDXQueryState.PASS)
        self.assertFalse(probe.complete)
        self.assertEqual(probe.candidate_years, (1997,))

        def exact_transport(hostname, year):
            exact_calls.append((hostname, year))
            return [([], True)]

        results = query_missing_years(
            "new.example",
            [1996, 1997, 1998],
            exact_transport,
            range_transport=range_transport,
        )

        self.assertEqual(
            exact_calls,
            [("new.example", 1996), ("new.example", 1997), ("new.example", 1998)],
        )
        self.assertEqual(
            [result.state for result in results],
            [
                CDXQueryState.EMPTY_EXHAUSTIVE,
                CDXQueryState.EMPTY_EXHAUSTIVE,
                CDXQueryState.EMPTY_EXHAUSTIVE,
            ],
        )


if __name__ == "__main__":
    unittest.main()
