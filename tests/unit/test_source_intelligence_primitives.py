from __future__ import annotations

import unittest

from creeper.source_discovery.models import SourceCandidate, SourceLevel
from creeper.source_discovery.motifs import (
    MotifExpansionPolicy,
    MotifProtocolError,
    expand_template,
    infer_year_sibling_candidates,
)
from creeper.source_discovery.overlap import build_minhash


class MotifExpansionTests(unittest.TestCase):
    def test_finite_template_expands_deterministically(self) -> None:
        urls = expand_template(
            "https://archive.example/{YEAR}/index-{SHARD}.cdxj",
            {
                "YEAR": [1999, 2000],
                "SHARD": ["00", "01"],
            },
        )

        self.assertEqual(
            urls,
            (
                "https://archive.example/1999/index-00.cdxj",
                "https://archive.example/1999/index-01.cdxj",
                "https://archive.example/2000/index-00.cdxj",
                "https://archive.example/2000/index-01.cdxj",
            ),
        )

    def test_template_expansion_is_hard_bounded(self) -> None:
        with self.assertRaisesRegex(MotifProtocolError, "max_expansions"):
            expand_template(
                "https://archive.example/{YEAR}/{SHARD}",
                {
                    "YEAR": [1996, 1997, 1998],
                    "SHARD": [0, 1, 2],
                },
                policy=MotifExpansionPolicy(max_expansions=8),
            )

    def test_proven_annual_source_yields_siblings_only_once(self) -> None:
        source = SourceCandidate(
            canonical_entrypoint=(
                "https://archive.example/2001/index.cdxj.gz"
            ),
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            discovered_by="agent:test",
            discovery_strategy="DIRECT_EVIDENCE_BULK",
            expected_year_from=2001,
            expected_year_to=2001,
            expected_volume=100000,
            temporal_semantics_prior=1.0,
            enumerability_prior=0.95,
            direct_evidence_prior=1.0,
            confidence=0.9,
        )

        siblings = infer_year_sibling_candidates(source)

        self.assertEqual(len(siblings), 5)
        self.assertEqual(
            {item.expected_year_from for item in siblings},
            {1996, 1997, 1998, 1999, 2000},
        )
        self.assertTrue(
            all(
                item.discovery_strategy == "YEAR_SIBLING_MOTIF"
                for item in siblings
            )
        )
        self.assertEqual(
            infer_year_sibling_candidates(siblings[0]),
            (),
        )


class OverlapSketchTests(unittest.TestCase):
    def test_identical_samples_have_unit_similarity(self) -> None:
        left = build_minhash(["a.example", "b.example", "c.example"])
        right = build_minhash(["c.example", "a.example", "b.example"])
        self.assertEqual(left.similarity(right), 1.0)

    def test_disjoint_samples_are_not_identical(self) -> None:
        left = build_minhash(["a.example", "b.example"])
        right = build_minhash(["x.test", "y.test"])
        self.assertLess(left.similarity(right), 1.0)


if __name__ == "__main__":
    unittest.main()
