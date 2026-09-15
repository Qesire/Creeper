from __future__ import annotations

import sqlite3
import unittest

from creeper.source_discovery.search_identity import (
    RawSearchResult,
    SearchIdentityLedger,
    canonicalize_result_url,
    canonicalize_search_result,
)


class SearchIdentityTests(unittest.TestCase):
    def test_url_identity_strips_tracking_but_preserves_semantic_query(self) -> None:
        canonical = canonicalize_result_url(
            "HTTPS://Example.COM:443/data/../trace/?b=2&utm_source=x&a=1#frag"
        )
        self.assertEqual(canonical, "https://example.com/trace/?a=1&b=2")

    def test_doi_collapses_mirrors_at_dataset_level(self) -> None:
        first = canonicalize_search_result(
            RawSearchResult(
                provider="datacite",
                provider_result_id="10.1234/ABC",
                url="https://repo.example/a.zip",
                title="1998 Proxy Trace v1",
                publisher="Example Lab",
            ),
            relevance_score=0.9,
            qualified=True,
        )
        second = canonicalize_search_result(
            RawSearchResult(
                provider="datacite",
                provider_result_id="10.1234/abc",
                url="https://mirror.example/b.zip",
                title="1999 Proxy Trace v2",
                publisher="Example Lab",
            ),
            relevance_score=0.9,
            qualified=True,
        )
        self.assertEqual(first.dataset_key, second.dataset_key)
        self.assertEqual(first.family_key, second.family_key)
        self.assertNotEqual(first.url_key, second.url_key)
        self.assertNotEqual(first.artifact_key, second.artifact_key)

    def test_ledger_reports_newness_at_all_four_levels(self) -> None:
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        ledger = SearchIdentityLedger(connection, clock=lambda: 10.0)
        first = canonicalize_search_result(
            RawSearchResult(
                provider="datacite",
                provider_result_id="10.1234/a",
                url="https://repo.example/trace-1998.zip",
                title="1998 Proxy Trace",
                publisher="Example Lab",
            ),
            relevance_score=0.9,
            qualified=True,
        )
        mirror = canonicalize_search_result(
            RawSearchResult(
                provider="datacite",
                provider_result_id="10.1234/a",
                url="https://mirror.example/trace-1998.zip",
                title="1998 Proxy Trace",
                publisher="Example Lab",
            ),
            relevance_score=0.9,
            qualified=True,
        )

        first_reg = ledger.register(cell_key="cell-a", result=first)
        second_reg = ledger.register(cell_key="cell-b", result=mirror)

        self.assertTrue(first_reg.new_url)
        self.assertTrue(first_reg.new_artifact)
        self.assertTrue(first_reg.new_dataset)
        self.assertTrue(first_reg.new_family)
        self.assertTrue(second_reg.new_url)
        self.assertTrue(second_reg.new_artifact)
        self.assertFalse(second_reg.new_dataset)
        self.assertFalse(second_reg.new_family)

        refs = connection.execute(
            "SELECT COUNT(*) AS n FROM residual_search_references"
        ).fetchone()
        self.assertEqual(int(refs["n"]), 2)
        connection.close()


if __name__ == "__main__":
    unittest.main()
