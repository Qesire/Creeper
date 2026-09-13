from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.index_registry import IndexSpaceRegistry
from creeper.source_discovery.index_space import (
    RegionKind,
    RegionState,
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

    def _registered_ready_region(self) -> str:
        compiled = compile_candidate_index_space(
            self.candidate(),
            range_supported=True,
            content_length=1024,
            direct_evidence_authority=True,
        )
        self.registry.register_index_space(compiled)
        self.registry.mark_region_state(
            compiled.root_region.region_key,
            RegionState.HARVEST_READY,
        )
        return compiled.root_region.region_key

    def test_region_harvest_renew_extends_claim(self) -> None:
        now = [100.0]
        self.registry.clock = lambda: now[0]
        region_key = self._registered_ready_region()
        claimed = self.registry.claim_region_for_harvest(
            region_key,
            owner="worker-a",
            ttl_seconds=10.0,
        )
        self.assertIsNotNone(claimed)

        now[0] = 104.0
        renewed_until = self.registry.renew_region_harvest(
            region_key,
            owner="worker-a",
            ttl_seconds=10.0,
        )

        self.assertEqual(renewed_until, 114.0)
        self.assertEqual(
            self.registry.assert_region_harvest_owned(
                region_key,
                owner="worker-a",
            ),
            114.0,
        )
        self.assertEqual(
            self.registry.recover_expired_harvest_claims(now=111.0),
            0,
        )

    def test_wrong_owner_cannot_renew(self) -> None:
        now = [100.0]
        self.registry.clock = lambda: now[0]
        region_key = self._registered_ready_region()
        self.registry.claim_region_for_harvest(
            region_key,
            owner="worker-a",
            ttl_seconds=10.0,
        )

        with self.assertRaisesRegex(ValueError, "not owned"):
            self.registry.renew_region_harvest(
                region_key,
                owner="worker-b",
                ttl_seconds=10.0,
            )

    def test_expired_claim_cannot_be_renewed(self) -> None:
        now = [100.0]
        self.registry.clock = lambda: now[0]
        region_key = self._registered_ready_region()
        self.registry.claim_region_for_harvest(
            region_key,
            owner="worker-a",
            ttl_seconds=10.0,
        )

        now[0] = 110.0
        with self.assertRaisesRegex(ValueError, "expired"):
            self.registry.renew_region_harvest(
                region_key,
                owner="worker-a",
                ttl_seconds=10.0,
            )

    def test_claim_recovers_after_heartbeat_stops(self) -> None:
        now = [100.0]
        self.registry.clock = lambda: now[0]
        region_key = self._registered_ready_region()
        self.registry.claim_region_for_harvest(
            region_key,
            owner="worker-a",
            ttl_seconds=10.0,
        )
        now[0] = 105.0
        self.registry.renew_region_harvest(
            region_key,
            owner="worker-a",
            ttl_seconds=10.0,
        )

        self.assertEqual(
            self.registry.recover_expired_harvest_claims(now=114.0),
            0,
        )
        self.assertEqual(
            self.registry.recover_expired_harvest_claims(now=116.0),
            1,
        )
        self.assertEqual(
            self.registry.get_region(region_key).state,
            RegionState.HARVEST_READY,
        )

    def test_complete_requires_current_owner(self) -> None:
        now = [100.0]
        self.registry.clock = lambda: now[0]
        region_key = self._registered_ready_region()
        self.registry.claim_region_for_harvest(
            region_key,
            owner="worker-a",
            ttl_seconds=10.0,
        )

        now[0] = 111.0
        with self.assertRaisesRegex(ValueError, "expired"):
            self.registry.complete_region_harvest(
                region_key,
                owner="worker-a",
            )
        self.assertEqual(
            self.registry.get_region(region_key).state,
            RegionState.HARVESTING,
        )

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
