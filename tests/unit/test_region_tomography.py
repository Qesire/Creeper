from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.source_discovery.index_registry import IndexSpaceRegistry
from creeper.source_discovery.index_space import (
    RegionState,
    RegionSynopsis,
    compile_candidate_index_space,
)
from creeper.source_discovery.models import SourceCandidate, SourceLevel
from creeper.source_discovery.region_probe import (
    RegionProbeError,
    RegionProbeExecutor,
    RegionProbePolicy,
)
from creeper.source_discovery.tomography import (
    RegionTomographyPlanner,
    RegionTomographyPolicy,
    TomographyActionKind,
)
from creeper.storage.control_store import ControlStore


class RegionTomographyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        task_root = self.root / "task"
        baseline_root = task_root / "merged260909-3"
        baseline_root.mkdir(parents=True)
        for year in range(1996, 2002):
            value = "known.com\n" if year == 1996 else ""
            (baseline_root / f"{year}.txt").write_text(value, encoding="utf-8")
        (baseline_root / "candidate_pool.txt").write_text("", encoding="utf-8")
        self.baseline = BaselineIndex.build(
            task_root,
            self.root / "baseline.sqlite3",
        )
        self.control = ControlStore(self.root / "control.sqlite3")
        self.registry = IndexSpaceRegistry(self.control)

    def tearDown(self) -> None:
        self.control.close()
        self.baseline.close()
        self.tmp.cleanup()

    @staticmethod
    def _candidate(path: Path, *, volume: int = 10_000) -> SourceCandidate:
        return SourceCandidate(
            canonical_entrypoint=f"https://archive.example/{path.name}",
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            discovered_by="test",
            discovery_strategy="DIRECT_EVIDENCE_BULK",
            expected_year_from=1996,
            expected_year_to=2001,
            expected_volume=volume,
            direct_evidence_prior=1.0,
            enumerability_prior=1.0,
            confidence=0.9,
        )

    def _compiled_local(self, path: Path, *, content_length: int | None = None):
        compiled = compile_candidate_index_space(
            self._candidate(path),
            content_length=(
                path.stat().st_size
                if content_length is None
                else content_length
            ),
            direct_evidence_authority=True,
        )
        index = replace(compiled.index, locator=str(path))
        root_region = replace(compiled.root_region, locator=str(path))
        return replace(compiled, index=index, root_region=root_region)

    async def test_local_cdxj_probe_builds_complete_baseline_aware_synopsis(self) -> None:
        path = self.root / "tiny.cdxj"
        path.write_text(
            "\n".join(
                [
                    'com,known)/a 19960101000000 {"url":"http://known.com/a"}',
                    'com,known)/b 19980101000000 {"url":"http://known.com/b"}',
                    'com,novel)/a 19970101000000 {"url":"http://novel.com/a"}',
                    'com,novel)/b 20010101000000 {"url":"http://novel.com/b"}',
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        compiled = self._compiled_local(path)
        self.registry.register_index_space(compiled)
        executor = RegionProbeExecutor(
            self.baseline,
            {"com": Decimal("1")},
            policy=RegionProbePolicy(
                max_sample_bytes=64 * 1024,
                sample_windows=4,
                minhash_width=8,
            ),
        )

        result = await executor.probe(compiled.index, compiled.root_region)

        self.assertTrue(result.synopsis.complete)
        self.assertEqual(result.synopsis.sampled_records, 4)
        self.assertEqual(result.synopsis.unique_hosts, 2)
        self.assertEqual(result.synopsis.novel_hosts, 2)
        self.assertEqual(result.synopsis.observed_host_year_pairs, 4)
        self.assertEqual(result.synopsis.novel_host_year_pairs, 3)
        self.assertEqual(result.synopsis.novel_eed, 3.0)
        self.assertEqual(result.synopsis.requests, 0)
        self.assertEqual(result.synopsis.confidence, 1.0)
        self.assertEqual(
            (result.region.byte_start, result.region.byte_end),
            (0, path.stat().st_size - 1),
        )

    async def test_large_local_probe_stays_within_sample_budget(self) -> None:
        path = self.root / "large.cdxj"
        rows = [
            (
                f"com,site{index:04d})/ "
                f"1998{(index % 12) + 1:02d}01000000 "
                f'{{"url":"http://site{index:04d}.com/"}}'
            )
            for index in range(2000)
        ]
        path.write_text("\n".join(rows) + "\n", encoding="utf-8")
        compiled = compile_candidate_index_space(
            self._candidate(path),
            content_length=path.stat().st_size,
            direct_evidence_authority=True,
        )
        executor = RegionProbeExecutor(
            self.baseline,
            {"com": Decimal("1")},
            policy=RegionProbePolicy(
                max_sample_bytes=4096,
                sample_windows=4,
                minhash_width=8,
            ),
        )

        result = await executor.probe(compiled.index, compiled.root_region)

        self.assertFalse(result.synopsis.complete)
        self.assertLessEqual(result.synopsis.bytes_read, 4096)
        self.assertEqual(len(result.sampled_ranges), 4)
        self.assertGreater(result.synopsis.novel_host_year_pairs, 0)
        self.assertLess(result.synopsis.confidence, 1.0)

    async def test_compressed_cdx_fails_closed_for_byte_tomography(self) -> None:
        path = self.root / "index.cdxj.gz"
        path.write_bytes(b"not-a-real-gzip")
        compiled = self._compiled_local(path)
        executor = RegionProbeExecutor(
            self.baseline,
            {"com": Decimal("1")},
        )

        with self.assertRaisesRegex(RegionProbeError, "compressed"):
            await executor.probe(compiled.index, compiled.root_region)

    def test_planner_refines_positive_region_then_promotes_terminal_child(self) -> None:
        path = self.root / "planned.cdxj"
        path.write_text("x\n" * 100, encoding="utf-8")
        compiled = self._compiled_local(path, content_length=160)
        self.registry.register_index_space(compiled)
        planner = RegionTomographyPlanner(
            self.registry,
            policy=RegionTomographyPolicy(
                max_depth=1,
                min_child_bytes=40,
                min_observations_to_stop=4,
                zero_yield_stop_confidence=0.1,
            ),
        )

        first = planner.advance(compiled.index.index_key)
        self.assertEqual(len(first), 1)
        self.assertEqual(first[0].kind, TomographyActionKind.PROBE)
        self.assertEqual(first[0].region.region_key, compiled.root_region.region_key)

        self.registry.record_synopsis(
            RegionSynopsis(
                region_key=compiled.root_region.region_key,
                sampled_records=20,
                unique_hosts=10,
                novel_hosts=8,
                observed_host_year_pairs=10,
                novel_host_year_pairs=8,
                novel_eed=8.0,
                bytes_read=40,
                requests=1,
                confidence=0.25,
                complete=False,
            )
        )

        second = planner.advance(compiled.index.index_key)
        self.assertEqual(len(second), 2)
        self.assertTrue(
            all(action.kind is TomographyActionKind.PROBE for action in second)
        )
        children = {
            action.region.region_key: action.region for action in second
        }
        self.assertEqual(
            sorted(
                (child.byte_start, child.byte_end)
                for child in children.values()
            ),
            [(0, 79), (80, 159)],
        )

        ordered = sorted(children.values(), key=lambda item: item.byte_start or 0)
        positive, empty = ordered
        self.registry.record_synopsis(
            RegionSynopsis(
                region_key=positive.region_key,
                sampled_records=15,
                unique_hosts=8,
                novel_hosts=6,
                observed_host_year_pairs=8,
                novel_host_year_pairs=6,
                novel_eed=6.0,
                bytes_read=80,
                requests=1,
                confidence=1.0,
                complete=True,
            )
        )
        self.registry.record_synopsis(
            RegionSynopsis(
                region_key=empty.region_key,
                sampled_records=15,
                unique_hosts=8,
                novel_hosts=0,
                observed_host_year_pairs=8,
                novel_host_year_pairs=0,
                novel_eed=0.0,
                bytes_read=80,
                requests=1,
                confidence=1.0,
                complete=True,
            )
        )

        third = planner.advance(compiled.index.index_key)

        self.assertEqual(len(third), 1)
        self.assertEqual(third[0].kind, TomographyActionKind.HARVEST)
        self.assertEqual(third[0].region.region_key, positive.region_key)
        self.assertEqual(
            self.registry.get_region(positive.region_key).state,
            RegionState.HARVEST_READY,
        )
        self.assertEqual(
            self.registry.get_region(empty.region_key).state,
            RegionState.DROPPED,
        )


if __name__ == "__main__":
    unittest.main()
