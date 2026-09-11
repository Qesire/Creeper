from __future__ import annotations

import unittest

from creeper.source_discovery.promotion import (
    LinkPromotionAccumulator,
    PromotionPolicy,
)
from creeper.source_discovery.scrapy_sidecar import ScrapyLinkDiscovery
from creeper.source_discovery.models import SourceLevel


class LinkPromotionTests(unittest.TestCase):
    @staticmethod
    def link(
        url: str,
        *,
        page: str = "https://seed.example/root/",
        anchor: str = "",
        same_site: bool = False,
    ) -> ScrapyLinkDiscovery:
        return ScrapyLinkDiscovery(
            source_key="src:" + "a" * 64,
            page_url=page,
            discovered_url=url,
            anchor_text=anchor,
            depth=1,
            same_site=same_site,
        )

    def test_same_site_navigation_stays_in_scrapy_frontier(self) -> None:
        acc = LinkPromotionAccumulator()
        acc.add(
            self.link(
                "https://seed.example/archive/index.html",
                anchor="archive directory",
                same_site=True,
            )
        )
        self.assertEqual(acc.promoted(), [])

    def test_bulk_artifact_can_promote_from_one_link_without_claiming_direct_evidence(self) -> None:
        acc = LinkPromotionAccumulator()
        acc.add(
            self.link(
                "https://seed.example/data/oldweb.cdxj",
                same_site=True,
                anchor="CDX index",
            )
        )
        promoted = acc.promoted()

        self.assertEqual(len(promoted), 1)
        item = promoted[0]
        self.assertTrue(item.evidence.bulk_artifact)
        self.assertEqual(item.candidate.source_family, "BULK_ARTIFACT")
        self.assertEqual(item.candidate.level, SourceLevel.SOURCE)
        self.assertEqual(item.candidate.direct_evidence_prior, 0.0)
        self.assertEqual(item.candidate.baseline_overlap_prior, 0.5)

    def test_url_list_and_compressed_cdxj_are_strong_bulk_artifacts(self) -> None:
        for url in (
            "https://seed.example/data/webbase-2001.urls.gz",
            "https://seed.example/data/index.cdxj.gz",
        ):
            acc = LinkPromotionAccumulator()
            acc.add(self.link(url, same_site=True, anchor="historical URL export"))

            promoted = acc.promoted()

            self.assertEqual(len(promoted), 1)
            self.assertTrue(promoted[0].evidence.bulk_artifact)
            self.assertEqual(promoted[0].candidate.source_family, "BULK_ARTIFACT")
            self.assertEqual(promoted[0].candidate.level, SourceLevel.SOURCE)

    def test_generic_external_page_is_never_promoted_without_resource_signal(self) -> None:
        acc = LinkPromotionAccumulator()
        for page in ("https://seed.example/a", "https://seed.example/b"):
            acc.add(self.link("https://other.example/about.html", page=page, anchor="about us"))
        self.assertEqual(acc.promoted(), [])

    def test_catalog_page_requires_distinct_referrer_corroboration(self) -> None:
        acc = LinkPromotionAccumulator(policy=PromotionPolicy(min_distinct_referrers=2))
        acc.add(
            self.link(
                "https://other.example/datasets/catalog/",
                page="https://seed.example/a",
                anchor="dataset catalog",
            )
        )
        acc.add(
            self.link(
                "https://other.example/datasets/catalog/",
                page="https://seed.example/a",
                anchor="dataset catalog duplicate",
            )
        )
        self.assertEqual(acc.promoted(), [])

        acc.add(
            self.link(
                "https://other.example/datasets/catalog/",
                page="https://seed.example/b",
                anchor="collections catalog",
            )
        )
        promoted = acc.promoted()
        self.assertEqual(len(promoted), 1)
        self.assertEqual(promoted[0].evidence.distinct_referrers, 2)
        self.assertEqual(promoted[0].candidate.level, SourceLevel.METASOURCE)

    def test_promotion_count_and_input_consumption_are_hard_bounded_and_deterministic(self) -> None:
        policy = PromotionPolicy(max_promotions=2, max_input_links=5, min_distinct_referrers=1)
        acc = LinkPromotionAccumulator(policy=policy)
        for index in range(8):
            acc.add(
                self.link(
                    f"https://bulk.example/archive-{index}.zip",
                    page=f"https://seed.example/{index}",
                    anchor="archive dump",
                )
            )

        self.assertEqual(acc.input_links, 5)
        promoted = acc.promoted()
        self.assertEqual(len(promoted), 2)
        self.assertEqual(
            [item.candidate.canonical_entrypoint for item in promoted],
            [
                "https://bulk.example/archive-0.zip",
                "https://bulk.example/archive-1.zip",
            ],
        )

    def test_long_query_candidate_is_dropped_before_aggregation(self) -> None:
        acc = LinkPromotionAccumulator(policy=PromotionPolicy(max_query_length=8))
        acc.add(
            self.link(
                "https://other.example/catalog/?filter=" + "x" * 100,
                anchor="dataset catalog",
            )
        )
        self.assertEqual(acc.promoted(), [])


if __name__ == "__main__":
    unittest.main()
