from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from creeper.source_discovery.expander import expand_scrapy_spool
from creeper.source_discovery.models import (
    SourceCandidate,
    SourceLevel,
    SourceState,
    SuppressionScope,
    source_key,
)
from creeper.source_discovery.promotion import PromotionPolicy
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.storage.control_store import ControlStore


class ScrapySourceGraphExpanderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.control = ControlStore(self.root / "control.sqlite3")
        self.registry = SourceDiscoveryRegistry(self.control, max_graph_hops=3)
        self.parent = SourceCandidate(
            canonical_entrypoint="https://seed.example/archive/",
            source_family="HISTORICAL_DIRECTORY",
            level=SourceLevel.COLLECTION,
            discovered_by="agent:test",
            discovery_strategy="META_SOURCE_SEARCH",
            confidence=0.8,
        )
        self.registry.register_proposal(self.parent, proposal_id="proposal:parent")
        self.spool = self.root / "links.jsonl"

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    def write_rows(self, rows: list[dict[str, object]]) -> None:
        with self.spool.open("w", encoding="utf-8") as stream:
            for row in rows:
                stream.write(json.dumps(row, sort_keys=True) + "\n")

    def row(
        self,
        url: str,
        *,
        page: str,
        anchor: str,
        same_site: bool,
    ) -> dict[str, object]:
        return {
            "record_type": "LINK_DISCOVERY",
            "source_key": self.parent.source_key,
            "page_url": page,
            "discovered_url": url,
            "anchor_text": anchor,
            "depth": 1,
            "same_site": same_site,
        }

    def test_expansion_registers_only_promoted_resources_and_is_idempotent(self) -> None:
        bulk_url = "https://seed.example/data/oldweb.cdxj"
        catalog_url = "https://catalog.example/datasets/catalog/"
        self.write_rows(
            [
                self.row(
                    "https://seed.example/about.html",
                    page="https://seed.example/archive/a.html",
                    anchor="archive navigation",
                    same_site=True,
                ),
                self.row(
                    bulk_url,
                    page="https://seed.example/archive/a.html",
                    anchor="CDX index",
                    same_site=True,
                ),
                self.row(
                    catalog_url,
                    page="https://seed.example/archive/a.html",
                    anchor="dataset catalog",
                    same_site=False,
                ),
                self.row(
                    catalog_url,
                    page="https://seed.example/archive/b.html",
                    anchor="collections catalog",
                    same_site=False,
                ),
                self.row(
                    "https://ordinary.example/about.html",
                    page="https://seed.example/archive/a.html",
                    anchor="about us",
                    same_site=False,
                ),
            ]
        )

        first = expand_scrapy_spool(self.registry, parent_source_key=self.parent.source_key, spool_path=self.spool)
        self.assertEqual(first.promoted, 2)
        self.assertEqual(first.inserted_candidates, 2)
        self.assertEqual(first.added_edges, 2)
        self.assertEqual(
            set(self.registry.children(self.parent.source_key)),
            {source_key(bulk_url), source_key(catalog_url)},
        )

        second = expand_scrapy_spool(self.registry, parent_source_key=self.parent.source_key, spool_path=self.spool)
        self.assertEqual(second.inserted_candidates, 0)
        self.assertEqual(second.added_edges, 0)
        self.assertEqual(self.registry.proposal_count(source_key(bulk_url)), 1)
        self.assertEqual(self.registry.proposal_count(source_key(catalog_url)), 1)

    def test_suppression_is_checked_before_candidate_creation(self) -> None:
        bulk_url = "https://blocked.example/archive.zip"
        self.registry.suppress(
            SuppressionScope.SOURCE,
            source_key(bulk_url),
            reason="known saturated resource",
        )
        self.write_rows(
            [
                self.row(
                    bulk_url,
                    page="https://seed.example/archive/a.html",
                    anchor="archive dump",
                    same_site=False,
                )
            ]
        )

        result = expand_scrapy_spool(self.registry, parent_source_key=self.parent.source_key, spool_path=self.spool)
        self.assertEqual(result.suppressed, 1)
        self.assertIsNone(self.registry.get_candidate(source_key(bulk_url)))

    def test_graph_depth_rejection_cannot_create_a_usable_detached_child(self) -> None:
        self.control.close()
        self.control = ControlStore(self.root / "depth.sqlite3")
        self.registry = SourceDiscoveryRegistry(self.control, max_graph_hops=1)
        root = SourceCandidate(
            canonical_entrypoint="https://root.example/catalog/",
            source_family="META",
            level=SourceLevel.METASOURCE,
            discovered_by="agent:test",
            discovery_strategy="META_SOURCE_SEARCH",
            confidence=0.8,
        )
        parent = SourceCandidate(
            canonical_entrypoint="https://parent.example/catalog/",
            source_family="COLLECTION",
            level=SourceLevel.COLLECTION,
            discovered_by="agent:test",
            discovery_strategy="META_SOURCE_SEARCH",
            confidence=0.8,
        )
        self.parent = parent
        self.registry.register_proposal(root, proposal_id="proposal:root")
        self.registry.register_proposal(parent, proposal_id="proposal:parent-depth")
        self.registry.add_edge(root.source_key, parent.source_key)

        child_url = "https://child.example/archive.zip"
        self.write_rows(
            [
                self.row(
                    child_url,
                    page=parent.canonical_entrypoint,
                    anchor="archive dump",
                    same_site=False,
                )
            ]
        )
        result = expand_scrapy_spool(
            self.registry,
            parent_source_key=parent.source_key,
            spool_path=self.spool,
            policy=PromotionPolicy(max_promotions=4),
        )

        self.assertEqual(result.rejected_lineage, 1)
        child = self.registry.get_candidate(source_key(child_url))
        self.assertIsNotNone(child)
        assert child is not None
        self.assertEqual(child.state, SourceState.REJECTED)
        self.assertNotIn(child.source_key, self.registry.children(parent.source_key))

    def test_corrupt_committed_spool_fails_before_any_child_mutation(self) -> None:
        good = self.row(
            "https://bulk.example/archive.zip",
            page="https://seed.example/a",
            anchor="archive dump",
            same_site=False,
        )
        with self.spool.open("wb") as stream:
            stream.write((json.dumps(good) + "\n").encode("utf-8"))
            stream.write(b"{bad-json}\n")

        before = len(self.registry.list_candidates())
        with self.assertRaises(ValueError):
            expand_scrapy_spool(
                self.registry,
                parent_source_key=self.parent.source_key,
                spool_path=self.spool,
            )
        self.assertEqual(len(self.registry.list_candidates()), before)


if __name__ == "__main__":
    unittest.main()
