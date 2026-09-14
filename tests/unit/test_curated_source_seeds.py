from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.curated_seeds import (
    curated_direct_catalogs,
    curated_direct_record_sources,
    curated_non_snapshot_roots,
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
                and item.expected_volume is None
                for item in roots
            )
        )
        self.assertTrue(
            any(
                item.canonical_entrypoint.endswith("cnr-2000.urls.gz")
                and item.expected_volume is None
                for item in roots
            )
        )
        self.assertTrue(all(item.direct_evidence_prior == 0.0 for item in roots))
        self.assertTrue(all(item.expected_year_to >= 1996 for item in roots))
        self.assertTrue(all(item.expected_year_from <= 2001 for item in roots))

    def test_non_snapshot_roots_are_audited_mailbox_catalogs(self):
        roots = curated_non_snapshot_roots()

        self.assertEqual(len(roots), 3)
        self.assertEqual(
            {item.source_family for item in roots},
            {"HISTORICAL_MAILBOX_CATALOG"},
        )
        self.assertTrue(
            all(
                item.canonical_entrypoint.startswith(
                    "https://lists.gnu.org/archive/mbox/"
                )
                for item in roots
            )
        )
        self.assertTrue(all(item.expected_volume is None for item in roots))
        self.assertTrue(all(item.direct_evidence_prior == 0.0 for item in roots))
        self.assertTrue(all(item.enumerability_prior == 1.0 for item in roots))
        self.assertTrue(
            any("lynx-dev" in item.canonical_entrypoint for item in roots)
        )
        self.assertTrue(
            any("emacs-devel" in item.canonical_entrypoint for item in roots)
        )
        self.assertTrue(
            any("bug-findutils" in item.canonical_entrypoint for item in roots)
        )

    def test_direct_record_seed_is_live_ftp_sitelist_artifact(self):
        roots = curated_direct_record_sources()

        self.assertEqual(len(roots), 4)
        self.assertEqual(
            {root.source_family for root in roots},
            {
                "HISTORICAL_FTP_SITELIST",
                "HISTORICAL_SBI_BBS_DIRECTORY",
            },
        )
        self.assertEqual(
            {root.canonical_entrypoint for root in roots},
            {
                (
                    "https://ftpmirror1.infania.net/pub/simtelnet/msdos/info/"
                    "ftp-list.zip"
                ),
                (
                    "https://ftp.zx.net.nz/pub/archive/simtel.net/pub/simtelnet/"
                    "msdos/info/ftp-list.zip"
                ),
                "https://files.mpoli.fi/software/TEXTS/MISC/SBI0197.ZIP",
                (
                    "https://ftp.zx.net.nz/pub/mirror/files.mpoli.fi/pub/software/"
                    "TEXTS/MISC/SBI0197.ZIP"
                ),
            },
        )
        self.assertTrue(
            all(
                root.expected_year_from >= 1996
                and root.expected_year_to <= 1997
                for root in roots
            )
        )
        self.assertTrue(all(root.direct_evidence_prior == 1.0 for root in roots))
        self.assertTrue(all(root.enumerability_prior == 1.0 for root in roots))
        self.assertTrue(
            any(
                root.discovery_strategy == "CURATED_DIRECT_RECORD_MIRROR"
                and root.baseline_overlap_prior >= 0.95
                for root in roots
            )
        )

    def test_all_curated_seeds_are_idempotent(self):
        seeds = curated_source_seeds()
        self.assertEqual(len(seeds), 12)
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            registry = SourceDiscoveryRegistry(control)
            try:
                self.assertEqual(ensure_curated_source_seeds(registry), 12)
                self.assertEqual(ensure_curated_source_seeds(registry), 0)
                self.assertEqual(len(registry.list_candidates()), 12)
            finally:
                control.close()

    def test_official_catalogs_are_idempotent_and_enumerable(self):
        seeds = curated_direct_catalogs()
        self.assertEqual(len(seeds), 1)
        self.assertTrue(
            any(
                item.canonical_entrypoint
                == "https://arquivo.pt/datasets/cdxj/"
                for item in seeds
            )
        )
        self.assertFalse(
            any("us-elections" in item.canonical_entrypoint for item in seeds)
        )
        self.assertTrue(all(item.enumerability_prior == 1.0 for item in seeds))

        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            registry = SourceDiscoveryRegistry(control)
            try:
                self.assertEqual(ensure_curated_direct_catalogs(registry), 1)
                self.assertEqual(ensure_curated_direct_catalogs(registry), 0)
                self.assertEqual(len(registry.list_candidates()), 1)
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
