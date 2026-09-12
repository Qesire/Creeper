from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.curated_seeds import (
    curated_direct_catalogs,
    ensure_curated_direct_catalogs,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.storage.control_store import ControlStore


class CuratedSourceSeedTests(unittest.TestCase):
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
                "data.labs.loc.gov/us-elections/by-year/2000/manifest.html"
                in item.canonical_entrypoint
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
