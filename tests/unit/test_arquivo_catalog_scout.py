from __future__ import annotations

import unittest

from creeper.source_discovery.arquivo_catalog_scout import (
    AUDITED_ARQUIVO_CATALOG_URL,
    ArquivoCatalogFetch,
    ArquivoCatalogScoutExecutor,
    is_audited_arquivo_catalog,
)
from creeper.source_discovery.coordinator import ScoutDisposition
from creeper.source_discovery.models import SourceCandidate, SourceLevel, SourceState


class ArquivoCatalogScoutTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def parent(
        *,
        discovered_by: str = "curated-official-seed",
        entrypoint: str = AUDITED_ARQUIVO_CATALOG_URL,
        family: str = "PUBLIC_ARCHIVE_INDEX_CATALOG",
        level: SourceLevel = SourceLevel.METASOURCE,
    ) -> SourceCandidate:
        return SourceCandidate(
            canonical_entrypoint=entrypoint,
            source_family=family,
            level=level,
            discovered_by=discovered_by,
            discovery_strategy="CURATED_DIRECT_CATALOG",
            expected_volume=150,
            temporal_semantics_prior=0.95,
            enumerability_prior=1.0,
            direct_evidence_prior=0.0,
            baseline_overlap_prior=0.5,
            access_cost_prior=0.2,
            adapter_cost_prior=0.2,
            confidence=1.0,
            state=SourceState.SCOUTING,
        )

    @staticmethod
    def fetcher_for(html: str, *, calls: list[tuple[str, int, float]] | None = None):
        async def fetcher(url: str, max_bytes: int, timeout_seconds: float):
            if calls is not None:
                calls.append((url, max_bytes, timeout_seconds))
            return ArquivoCatalogFetch(
                body=html.encode("utf-8"),
                status_code=200,
                final_url=AUDITED_ARQUIVO_CATALOG_URL,
            )

        return fetcher

    async def test_audited_catalog_three_cdxj_entries_yield_three_children(self) -> None:
        html = """
        <pre>
        <a href="one.cdxj">one.cdxj</a> 1M
        <a href="two.cdxj">two.cdxj</a> 2M
        <a href="three.cdxj">three.cdxj</a> 3M
        </pre>
        """
        calls: list[tuple[str, int, float]] = []
        result = await ArquivoCatalogScoutExecutor(
            fetcher=self.fetcher_for(html, calls=calls)
        )(self.parent())

        self.assertEqual(result.disposition, ScoutDisposition.HOLD)
        self.assertIsNone(result.measurement)
        self.assertEqual(len(result.discovered_candidates), 3)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], AUDITED_ARQUIVO_CATALOG_URL)

    async def test_non_cdxj_and_cross_origin_links_are_ignored(self) -> None:
        html = """
        <pre>
        <a href="ok.cdxj">ok.cdxj</a> 1M
        <a href="notes.txt">notes.txt</a> 2M
        <a href="https://evil.example/foreign.cdxj">foreign.cdxj</a> 3M
        </pre>
        """
        result = await ArquivoCatalogScoutExecutor(
            fetcher=self.fetcher_for(html)
        )(self.parent())

        self.assertEqual(
            [child.canonical_entrypoint for child in result.discovered_candidates],
            ["https://arquivo.pt/datasets/cdxj/ok.cdxj"],
        )

    async def test_duplicate_links_dedupe_deterministically(self) -> None:
        html = """
        <pre>
        <a href="same.cdxj">same-a.cdxj</a> 1M
        <a href="./same.cdxj">same-b.cdxj</a> 9G
        <a href="other.cdxj">other.cdxj</a> 2M
        </pre>
        """
        executor = ArquivoCatalogScoutExecutor(fetcher=self.fetcher_for(html))

        first = await executor(self.parent())
        second = await executor(self.parent())

        first_keys = [child.source_key for child in first.discovered_candidates]
        second_keys = [child.source_key for child in second.discovered_candidates]
        self.assertEqual(len(first_keys), 2)
        self.assertEqual(first_keys, second_keys)

    async def test_large_cdxj_entry_is_not_discarded(self) -> None:
        html = """
        <pre>
        <a href="huge.cdxj">huge.cdxj</a> 900G
        <a href="small.cdxj">small.cdxj</a> 1K
        </pre>
        """
        result = await ArquivoCatalogScoutExecutor(
            fetcher=self.fetcher_for(html)
        )(self.parent())

        self.assertEqual(
            [child.canonical_entrypoint for child in result.discovered_candidates],
            [
                "https://arquivo.pt/datasets/cdxj/huge.cdxj",
                "https://arquivo.pt/datasets/cdxj/small.cdxj",
            ],
        )

    async def test_child_identity_preserves_canonical_resource_url_and_priors(self) -> None:
        html = """
        <pre>
        <a href="exact.cdxj?download=1">exact.cdxj</a> 18G
        </pre>
        """
        result = await ArquivoCatalogScoutExecutor(
            fetcher=self.fetcher_for(html)
        )(self.parent())
        child = result.discovered_candidates[0]

        self.assertEqual(
            child.canonical_entrypoint,
            "https://arquivo.pt/datasets/cdxj/exact.cdxj?download=1",
        )
        self.assertEqual(child.source_family, "BULK_ARTIFACT")
        self.assertEqual(child.level, SourceLevel.SOURCE)
        self.assertEqual(
            child.discovery_strategy,
            "DETERMINISTIC_AUDITED_CATALOG_EXPANSION",
        )
        self.assertEqual(child.direct_evidence_prior, 1.0)
        self.assertEqual(child.temporal_semantics_prior, 1.0)
        self.assertEqual(child.enumerability_prior, 1.0)
        self.assertEqual(child.state, SourceState.DISCOVERED)
        # Catalog membership is scheduling metadata only. No deterministic
        # annual measurement/evidence is manufactured by this scout.
        self.assertIsNone(result.measurement)

    async def test_parent_stays_hold_and_child_lineage_points_to_parent(self) -> None:
        parent = self.parent()
        html = """
        <pre><a href="one.cdxj">one.cdxj</a> 1M</pre>
        """
        result = await ArquivoCatalogScoutExecutor(
            fetcher=self.fetcher_for(html)
        )(parent)

        self.assertEqual(result.disposition, ScoutDisposition.HOLD)
        self.assertEqual(result.edge_relation, "catalog_enumerates_cdxj")
        self.assertEqual(
            result.discovered_candidates[0].discovered_by,
            f"arquivo-catalog:{parent.source_key}",
        )

    async def test_non_audited_candidate_is_rejected_before_fetch(self) -> None:
        calls: list[tuple[str, int, float]] = []
        executor = ArquivoCatalogScoutExecutor(
            fetcher=self.fetcher_for("", calls=calls)
        )

        with self.assertRaisesRegex(ValueError, "only accepts the audited Arquivo catalog"):
            await executor(self.parent(discovered_by="generic-search"))

        self.assertEqual(calls, [])

    async def test_exact_audited_predicate_requires_url_family_level_and_provenance(self) -> None:
        self.assertTrue(is_audited_arquivo_catalog(self.parent()))
        self.assertFalse(
            is_audited_arquivo_catalog(
                self.parent(entrypoint="https://arquivo.pt/datasets/")
            )
        )
        self.assertFalse(
            is_audited_arquivo_catalog(self.parent(family="RESOURCE_CATALOG"))
        )
        self.assertFalse(
            is_audited_arquivo_catalog(self.parent(level=SourceLevel.COLLECTION))
        )
        self.assertFalse(
            is_audited_arquivo_catalog(self.parent(discovered_by="other"))
        )


if __name__ == "__main__":
    unittest.main()
