from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.residual_report import (
    REPORT_VERSION,
    ResidualSearchReportError,
    build_residual_search_report,
    load_residual_search_report,
)
from creeper.source_discovery.residual_search import (
    ResidualSearchLedger,
    SearchCell,
    SearchCellScheduler,
)
from creeper.source_discovery.search_identity import (
    RawSearchResult,
    SearchIdentityLedger,
    canonicalize_search_result,
)
from creeper.storage.control_store import ControlStore


class ResidualSearchReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "control.sqlite3"
        self.control = ControlStore(self.path)
        self.registry = SourceDiscoveryRegistry(self.control)
        self.ledger = ResidualSearchLedger(self.registry.connection)
        self.identity = SearchIdentityLedger(self.registry.connection)
        self.cell = SearchCell(
            mechanism="proxy_access",
            institution="university",
            period="1998",
            artifact="trace",
        )
        self.ledger.ensure_search_profile("providers=alpha,beta")
        self.ledger.ensure_cell(self.cell)

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    def _episode(
        self,
        *,
        result_count: int,
        duplicate_results: int,
        unique_roots: int,
        new_families: int,
        qualified_roots: int,
        cost: float,
        accepted_proposals: int,
        new_sources: int,
        accepted_eed: float,
    ) -> None:
        plan = SearchCellScheduler(self.ledger).next_plans(limit=1)[0]
        episode = self.registry.begin_search_episode(
            strategy="RESIDUAL_CELL:proxy_access",
            backend="alpha+beta",
            query=plan.query,
            actor="deterministic:alpha+beta",
        )
        self.ledger.bind_search_episode(self.cell, episode.episode_id)
        self.ledger.record_episode(
            self.cell,
            result_count=result_count,
            duplicate_results=duplicate_results,
            unique_roots=unique_roots,
            new_families=new_families,
            qualified_roots=qualified_roots,
            search_cost_seconds=cost,
        )
        self.registry.finish_search_episode(
            episode.episode_id,
            search_cost_seconds=cost,
            accepted_proposals=accepted_proposals,
            new_sources=new_sources,
        )
        self.registry.connection.execute(
            "UPDATE source_search_episodes SET accepted_novel_eed=? "
            "WHERE episode_id=?",
            (accepted_eed, episode.episode_id),
        )
        self.ledger.reconcile_search_rewards()

    def _register_result(
        self,
        *,
        provider: str,
        provider_result_id: str,
        url: str,
        title: str,
        doi: str,
    ) -> None:
        result = canonicalize_search_result(
            RawSearchResult(
                provider=provider,
                provider_result_id=provider_result_id,
                url=url,
                title=title,
                publisher="Example Archive",
                resource_type="dataset",
                identifiers=(doi,),
            ),
            relevance_score=0.9,
            qualified=True,
        )
        self.identity.register(cell_key=self.cell.key, result=result)

    def test_report_separates_shape_economics_and_provider_identity(self) -> None:
        self._episode(
            result_count=4,
            duplicate_results=1,
            unique_roots=3,
            new_families=2,
            qualified_roots=2,
            cost=2.0,
            accepted_proposals=2,
            new_sources=2,
            accepted_eed=4.0,
        )
        self._episode(
            result_count=3,
            duplicate_results=1,
            unique_roots=2,
            new_families=1,
            qualified_roots=1,
            cost=3.0,
            accepted_proposals=1,
            new_sources=1,
            accepted_eed=6.0,
        )

        self._register_result(
            provider="alpha",
            provider_result_id="shared-a",
            url="https://alpha.example/shared.zip",
            title="1998 Proxy Trace Shared Dataset",
            doi="10.1234/shared",
        )
        self._register_result(
            provider="beta",
            provider_result_id="shared-b",
            url="https://beta.example/shared.zip",
            title="1998 Proxy Trace Shared Dataset",
            doi="10.1234/shared",
        )
        self._register_result(
            provider="alpha",
            provider_result_id="unique-a",
            url="https://alpha.example/unique.zip",
            title="1998 Proxy Trace Alpha Dataset",
            doi="10.1234/alpha-only",
        )

        report = build_residual_search_report(self.registry.connection)

        self.assertEqual(report["report_version"], REPORT_VERSION)
        self.assertTrue(
            str(report["search_profile"]).startswith(
                "residual-query-program-v3|providers=alpha,beta"
            )
        )
        summary = report["summary"]
        self.assertEqual(summary["cells"], 1)
        self.assertEqual(summary["attempts"], 2)
        self.assertEqual(summary["result_count"], 7)
        self.assertEqual(summary["duplicate_results"], 2)
        self.assertEqual(summary["qualified_roots"], 3)
        self.assertAlmostEqual(summary["accepted_novel_eed"], 10.0)
        self.assertAlmostEqual(summary["search_cost_seconds"], 5.0)
        self.assertAlmostEqual(
            summary["accepted_novel_eed_per_search_second"],
            2.0,
        )

        shapes = {
            item["query_shape"]: item for item in report["query_shapes"]
        }
        self.assertEqual(set(shapes), {"STRICT_4D", "RELAX_INSTITUTION"})
        self.assertEqual(shapes["STRICT_4D"]["episodes"], 1)
        self.assertEqual(shapes["STRICT_4D"]["accepted_proposals"], 2)
        self.assertAlmostEqual(
            shapes["STRICT_4D"]["accepted_novel_eed"], 4.0
        )
        self.assertEqual(shapes["RELAX_INSTITUTION"]["episodes"], 1)
        self.assertEqual(
            shapes["RELAX_INSTITUTION"]["accepted_proposals"], 1
        )
        self.assertAlmostEqual(
            shapes["RELAX_INSTITUTION"]["accepted_novel_eed"], 6.0
        )

        cell = report["cells"][0]
        self.assertEqual(cell["program_length"], 8)
        self.assertAlmostEqual(cell["program_coverage_fraction"], 0.25)

        providers = {item["provider"]: item for item in report["providers"]}
        self.assertEqual(providers["alpha"]["distinct_datasets"], 2)
        self.assertEqual(providers["alpha"]["exclusive_datasets"], 1)
        self.assertEqual(providers["alpha"]["shared_datasets"], 1)
        self.assertEqual(providers["beta"]["distinct_datasets"], 1)
        self.assertEqual(providers["beta"]["exclusive_datasets"], 0)
        self.assertEqual(providers["beta"]["shared_datasets"], 1)
        self.assertNotIn("accepted_novel_eed", providers["alpha"])

        self.assertEqual(
            report["provider_overlap"],
            [
                {
                    "provider_a": "alpha",
                    "provider_b": "beta",
                    "shared_datasets": 1,
                    "shared_families": 1,
                }
            ],
        )
        self.assertFalse(
            report["limitations"]["provider_final_eed_attribution_available"]
        )

    def test_load_report_uses_initialized_file_read_only(self) -> None:
        self._episode(
            result_count=0,
            duplicate_results=0,
            unique_roots=0,
            new_families=0,
            qualified_roots=0,
            cost=0.5,
            accepted_proposals=0,
            new_sources=0,
            accepted_eed=0.0,
        )
        self.control.connection.commit()

        report = load_residual_search_report(self.path)

        self.assertEqual(report["summary"]["attempts"], 1)
        self.assertEqual(report["query_shapes"][0]["query_shape"], "STRICT_4D")
        self.assertEqual(
            self.registry.connection.execute(
                "SELECT COUNT(*) FROM residual_search_cells"
            ).fetchone()[0],
            1,
        )

    def test_missing_tables_fail_closed(self) -> None:
        connection = sqlite3.connect(":memory:")
        try:
            with self.assertRaisesRegex(
                ResidualSearchReportError,
                "missing residual-search tables",
            ):
                build_residual_search_report(connection)
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
