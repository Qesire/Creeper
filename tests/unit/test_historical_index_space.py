from __future__ import annotations

import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from creeper.authority.baseline_index import YEAR_BITS, BaselineIndex
from creeper.source_discovery.coordinator import TriageDisposition, TriageResult
from creeper.source_discovery.index_space import (
    QueryCapabilityHints,
    RegionKind,
    SourceAccessMode,
    child_region,
    compile_candidate_index_space,
)
from creeper.source_discovery.models import SourceCandidate, SourceLevel
from creeper.source_discovery.overlap import MinHashAccumulator, build_minhash
from creeper.sources.archive.host_year import (
    iter_baseline_difference,
    iter_cdx_host_year_masks,
    summarize_region,
)


class HistoricalIndexSpaceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        baseline_dir = root / "task" / "merged260909-3"
        baseline_dir.mkdir(parents=True)
        for year in range(1996, 2002):
            values = "known.com\n" if year == 1996 else ""
            (baseline_dir / f"{year}.txt").write_text(values, encoding="utf-8")
        (baseline_dir / "candidate_pool.txt").write_text("", encoding="utf-8")
        self.baseline = BaselineIndex.build(
            root / "task",
            root / "baseline.sqlite3",
        )

    def tearDown(self) -> None:
        self.baseline.close()
        self.tmp.cleanup()

    @staticmethod
    def candidate(url: str, *, level: SourceLevel = SourceLevel.SOURCE) -> SourceCandidate:
        return SourceCandidate(
            canonical_entrypoint=url,
            source_family="BULK_ARTIFACT",
            level=level,
            discovered_by="test",
            discovery_strategy="DIRECT_EVIDENCE_BULK",
            expected_year_from=1996,
            expected_year_to=2001,
            expected_volume=10_000_000,
            temporal_semantics_prior=1.0,
            enumerability_prior=1.0,
            direct_evidence_prior=1.0,
            baseline_overlap_prior=0.5,
            confidence=0.9,
        )

    def test_compiler_recognizes_sorted_cdxj_without_inventing_query_api(self) -> None:
        candidate = self.candidate("https://archive.example/index/all.cdxj")
        triage = TriageResult(
            disposition=TriageDisposition.SCOUT,
            status_code=206,
            method="GET",
            content_type="text/plain",
            content_length=100_000_000,
            range_supported=True,
        )

        compiled = compile_candidate_index_space(candidate, triage=triage)

        self.assertEqual(
            compiled.index.capabilities.access_mode,
            SourceAccessMode.SORTED_INDEX,
        )
        self.assertEqual(compiled.index.capabilities.format, "CDXJ")
        self.assertEqual(compiled.index.capabilities.sorted_keyspace, "SURT_URLKEY")
        self.assertTrue(compiled.index.capabilities.range_supported)
        self.assertTrue(compiled.index.capabilities.timestamp_bearing)
        self.assertTrue(compiled.index.capabilities.direct_evidence_authority)
        self.assertFalse(compiled.index.capabilities.supports_query)
        self.assertEqual(compiled.root_region.kind, RegionKind.FULL)

    def test_verified_query_hints_upgrade_access_mode(self) -> None:
        candidate = self.candidate("https://archive.example/index/all.cdx")
        compiled = compile_candidate_index_space(
            candidate,
            query_hints=QueryCapabilityHints(
                supports_query=True,
                supports_prefix=True,
                supports_domain=True,
                supports_date_filter=True,
            ),
        )

        self.assertEqual(
            compiled.index.capabilities.access_mode,
            SourceAccessMode.QUERY_API,
        )
        self.assertTrue(compiled.index.capabilities.can_push_down)
        self.assertTrue(compiled.index.capabilities.supports_domain)

    def test_child_regions_have_stable_hierarchical_identity(self) -> None:
        compiled = compile_candidate_index_space(
            self.candidate("https://archive.example/index/all.cdxj")
        )
        left = child_region(
            compiled.root_region,
            kind=RegionKind.KEY_PREFIX,
            key_prefix="com,",
        )
        same = child_region(
            compiled.root_region,
            kind=RegionKind.KEY_PREFIX,
            key_prefix="com,",
        )
        other = child_region(
            compiled.root_region,
            kind=RegionKind.KEY_PREFIX,
            key_prefix="org,",
        )

        self.assertEqual(left.region_key, same.region_key)
        self.assertNotEqual(left.region_key, other.region_key)
        self.assertEqual(left.parent_region_key, compiled.root_region.region_key)
        self.assertEqual(left.depth, 1)

    def test_cdxj_stream_reduces_capture_multiplicity_to_year_masks(self) -> None:
        lines = [
            'com,known)/a 19960101000000 {"url":"http://known.com/a"}',
            'com,known)/b 19980101000000 {"url":"http://known.com/b"}',
            'com,novel)/a 19970101000000 {"url":"http://novel.com/a"}',
            'com,novel)/b 19970201000000 {"url":"http://novel.com/b"}',
            'com,novel)/c 20010101000000 {"url":"http://novel.com/c"}',
        ]

        reduced = list(
            iter_cdx_host_year_masks(
                lines,
                index_format="CDXJ",
                source_id="archive:test",
                locator_prefix="index.cdxj",
            )
        )

        self.assertEqual(len(reduced), 2)
        self.assertEqual(reduced[0].hostname, "known.com")
        self.assertEqual(
            reduced[0].year_mask,
            YEAR_BITS[1996] | YEAR_BITS[1998],
        )
        self.assertEqual(reduced[0].capture_count, 2)
        self.assertEqual(reduced[1].hostname, "novel.com")
        self.assertEqual(
            reduced[1].year_mask,
            YEAR_BITS[1997] | YEAR_BITS[2001],
        )
        self.assertEqual(reduced[1].capture_count, 3)

        novelty = list(
            iter_baseline_difference(
                iter(reduced),
                self.baseline,
                batch_size=1,
            )
        )
        self.assertEqual(
            novelty[0].novel_year_mask,
            YEAR_BITS[1998],
        )
        self.assertEqual(
            novelty[1].novel_year_mask,
            YEAR_BITS[1997] | YEAR_BITS[2001],
        )

    def test_region_synopsis_tracks_baseline_external_host_year_coverage(self) -> None:
        lines = [
            'com,known)/a 19960101000000 {"url":"http://known.com/a"}',
            'com,known)/b 19980101000000 {"url":"http://known.com/b"}',
            'com,novel)/a 19970101000000 {"url":"http://novel.com/a"}',
            'com,novel)/b 19970201000000 {"url":"http://novel.com/b"}',
            'com,novel)/c 20010101000000 {"url":"http://novel.com/c"}',
        ]
        reduced = iter_cdx_host_year_masks(
            lines,
            index_format="CDXJ",
            source_id="archive:test",
            locator_prefix="index.cdxj",
        )

        synopsis = summarize_region(
            reduced,
            self.baseline,
            {"com": Decimal("1.0")},
            region_key="region:test",
            bytes_read=4096,
            requests=2,
            confidence=0.75,
            complete=False,
            batch_size=1,
            minhash_width=8,
        )

        self.assertEqual(synopsis.sampled_records, 5)
        self.assertEqual(synopsis.unique_hosts, 2)
        self.assertEqual(synopsis.novel_hosts, 2)
        self.assertEqual(synopsis.observed_host_year_pairs, 4)
        self.assertEqual(synopsis.novel_host_year_pairs, 3)
        self.assertEqual(synopsis.novel_eed, 3.0)
        self.assertEqual(
            dict(synopsis.observed_year_histogram),
            {1996: 1, 1997: 1, 1998: 1, 2001: 1},
        )
        self.assertEqual(
            dict(synopsis.novel_year_histogram),
            {1997: 1, 1998: 1, 2001: 1},
        )
        self.assertEqual(
            dict(synopsis.tld_host_year_histogram),
            {"com": 4},
        )
        self.assertEqual(len(synopsis.minhash_values), 8)
        self.assertAlmostEqual(synopsis.novel_fraction, 0.75)
        self.assertAlmostEqual(synopsis.novel_eed_per_byte, 3 / 4096)
        self.assertAlmostEqual(synopsis.novel_eed_per_request, 1.5)

    def test_streaming_minhash_matches_compatibility_builder(self) -> None:
        values = ["a\t1997", "b\t1998", "c\t2001"]
        accumulator = MinHashAccumulator(width=16)
        accumulator.extend(values)

        self.assertEqual(
            accumulator.sketch(),
            build_minhash(values, width=16),
        )


if __name__ == "__main__":
    unittest.main()
