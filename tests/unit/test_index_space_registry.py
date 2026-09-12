from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.index_registry import IndexSpaceRegistry
from creeper.source_discovery.index_space import (
    RegionKind,
    RegionSynopsis,
    child_region,
    compile_candidate_index_space,
)
from creeper.source_discovery.models import SourceCandidate, SourceLevel
from creeper.storage.control_store import ControlStore


class IndexSpaceRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.control = ControlStore(Path(self.tmp.name) / "control.sqlite3")
        self.registry = IndexSpaceRegistry(self.control)

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    @staticmethod
    def candidate() -> SourceCandidate:
        return SourceCandidate(
            canonical_entrypoint="https://archive.example/bulk/index.cdxj",
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            discovered_by="test",
            discovery_strategy="DIRECT_EVIDENCE_BULK",
            expected_year_from=1996,
            expected_year_to=2001,
            expected_volume=10_000,
            direct_evidence_prior=1.0,
            enumerability_prior=1.0,
            confidence=0.9,
        )

    def test_registers_root_and_refined_regions_idempotently(self) -> None:
        compiled = compile_candidate_index_space(
            self.candidate(),
            range_supported=True,
        )
        self.registry.register_index_space(compiled)
        self.registry.register_index_space(compiled)

        root = self.registry.get_region(compiled.root_region.region_key)
        self.assertEqual(root, compiled.root_region)

        child = child_region(
            compiled.root_region,
            kind=RegionKind.KEY_PREFIX,
            key_prefix="com,",
        )
        self.registry.put_region(child)
        regions = self.registry.list_regions(compiled.index.index_key)

        self.assertEqual(regions, (compiled.root_region, child))

    def test_round_trips_region_synopsis(self) -> None:
        compiled = compile_candidate_index_space(self.candidate())
        self.registry.register_index_space(compiled)
        synopsis = RegionSynopsis(
            region_key=compiled.root_region.region_key,
            sampled_records=25,
            unique_hosts=8,
            novel_hosts=4,
            observed_host_year_pairs=12,
            novel_host_year_pairs=5,
            novel_eed=4.25,
            bytes_read=8192,
            requests=2,
            observed_year_histogram=((1997, 7), (1998, 5)),
            novel_year_histogram=((1997, 2), (1998, 3)),
            tld_host_year_histogram=(("com", 9), ("org", 3)),
            minhash_values=(7, 11, 13, 17),
            confidence=0.8,
            complete=False,
        )

        self.registry.record_synopsis(synopsis)
        stored = self.registry.get_synopsis(synopsis.region_key)

        self.assertEqual(stored, synopsis)

    def test_synopsis_requires_registered_region(self) -> None:
        synopsis = RegionSynopsis(
            region_key="region:missing",
            sampled_records=0,
            unique_hosts=0,
            novel_hosts=0,
            observed_host_year_pairs=0,
            novel_host_year_pairs=0,
            novel_eed=0.0,
            bytes_read=0,
            requests=0,
            confidence=0.0,
            complete=False,
        )

        with self.assertRaises(KeyError):
            self.registry.record_synopsis(synopsis)


if __name__ == "__main__":
    unittest.main()
