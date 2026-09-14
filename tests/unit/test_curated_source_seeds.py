from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.curated_seeds import (
    curated_direct_catalogs,
    curated_research_roots,
    curated_source_seeds,
    ensure_curated_direct_catalogs,
    ensure_curated_source_seeds,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.storage.control_store import ControlStore


class CuratedSourceSeedTests(unittest.TestCase):
    def test_research_roots_are_target_period_discovery_only(self):
        roots = curated_research_roots()
        self.assertEqual(len(roots), 4)
        self.assertTrue(
            any(item.canonical_entrypoint == "https://archive95.net/sources" for item in roots)
        )
        self.assertTrue(
            any(item.canonical_entrypoint == "https://hdl.handle.net/11299/200445" for item in roots)
        )
        self.assertTrue(
            any(
                item.canonical_entrypoint.endswith("webbase-2001.urls.gz")
                and item.expected_volume == 118_142_155
                for item in roots
            )
        )
        self.assertTrue(
            any(
                item.canonical_entrypoint.endswith("cnr-2000.urls.gz")
                and item.expected_volume == 325_557
                for item in roots
            )
        )
        self.assertTrue(all(item.direct_evidence_prior == 0.0 for item in roots))
        self.assertTrue(all(item.expected_year_to >= 1996 for item in roots))
        self.assertTrue(all(item.expected_year_from <= 2001 for item in roots))

    def test_all_curated_seeds_are_idempotent(self):
        seeds = curated_source_seeds()
        self.assertEqual(len(seeds), 6)
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            registry = SourceDiscoveryRegistry(control)
            try:
                self.assertEqual(ensure_curated_source_seeds(registry), 6)
                self.assertEqual(ensure_curated_source_seeds(registry), 0)
                self.assertEqual(len(registry.list_candidates()), 6)
            finally:
                control.close()

    def test_official_catalogs_are_idempotent_and_enumerable(self):
        seeds = curated_direct_catalogs()
        self.assertEqual(len(seeds), 2)
        self.assertTrue(
            any(
                item.canonical_entrypoint
                == "https://arquivo.pt/datasets/cdxj/"
                for item in seeds
            )
        )
        self.assertTrue(
            any(
                item.canonical_entrypoint
                == "https://data.labs.loc.gov/us-elections/"
                for item in seeds
            )
        )
        self.assertTrue(all(item.enumerability_prior == 1.0 for item in seeds))

        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            registry = SourceDiscoveryRegistry(control)
            try:
                self.assertEqual(ensure_curated_direct_catalogs(registry), 2)
                self.assertEqual(ensure_curated_direct_catalogs(registry), 0)
                self.assertEqual(len(registry.list_candidates()), 2)
                self.assertTrue(
                    all(
                        registry.proposal_count(seed.source_key) == 1
                        for seed in seeds
                    )
                )
            finally:
                control.close()


if __name__ == "__main__":
    unittest.main()
