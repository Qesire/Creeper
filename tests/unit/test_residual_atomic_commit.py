from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.deterministic_search import DeterministicSearchBatch
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.residual_atomic import commit_deterministic_residual_batch
from creeper.source_discovery.residual_search import QueryPlan, ResidualSearchLedger, SearchCell
from creeper.source_discovery.search_identity import (
    RawSearchResult,
    SearchIdentityLedger,
    canonicalize_search_result,
)
from creeper.storage.control_store import ControlStore


class InjectedCrash(RuntimeError):
    pass


class ResidualAtomicCommitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.control = ControlStore(self.root / "control.sqlite3")
        self.registry = SourceDiscoveryRegistry(self.control)
        self.coverage = ResidualSearchLedger(self.registry.connection)
        self.identities = SearchIdentityLedger(self.registry.connection)
        self.cell = SearchCell(
            mechanism="proxy_access",
            institution="university",
            period="1998",
            artifact="trace",
        )
        self.coverage.ensure_cell(self.cell)
        self.plan = QueryPlan(
            cell=self.cell,
            query='"1998" "proxy" "university" "trace"',
            variant=0,
            exclusions=(),
            score=1.0,
            mechanism_phrase="proxy",
            include_institution=True,
            query_shape="STRICT_4D",
        )
        first = canonicalize_search_result(
            RawSearchResult(
                provider="fixture",
                provider_result_id="r1",
                url="https://repo.example/proxy98.zip",
                title="1998 University Proxy Trace Dataset",
                publisher="Example University",
                identifiers=("10.1234/proxy98",),
            ),
            relevance_score=1.0,
            qualified=True,
        )
        mirror = canonicalize_search_result(
            RawSearchResult(
                provider="fixture",
                provider_result_id="r2",
                url="https://mirror.example/proxy98.zip",
                title="1998 University Proxy Trace Dataset",
                publisher="Example University",
                identifiers=("10.1234/proxy98",),
            ),
            relevance_score=1.0,
            qualified=True,
        )
        self.batch = DeterministicSearchBatch(
            backend="fixture",
            query=self.plan.query,
            actor="deterministic:test",
            results=(first, mirror),
            search_cost_seconds=0.1,
        )

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    def _count(self, table: str) -> int:
        row = self.registry.connection.execute(
            f"SELECT COUNT(*) AS n FROM {table}"
        ).fetchone()
        return int(row["n"])

    def _assert_pre_episode_state(self) -> None:
        stats = self.coverage.stats(self.cell)
        self.assertEqual(stats.attempts, 0)
        self.assertEqual(stats.result_count, 0)
        self.assertEqual(stats.duplicate_results, 0)
        self.assertEqual(stats.unique_roots, 0)
        self.assertEqual(stats.new_families, 0)
        self.assertEqual(stats.qualified_roots, 0)
        self.assertEqual(stats.variant_cursor, 0)
        for table in (
            "source_search_episodes",
            "source_candidates",
            "source_proposals",
            "residual_search_episode_cells",
            "residual_search_cell_families",
            "residual_search_urls",
            "residual_search_artifacts",
            "residual_search_datasets",
            "residual_search_families",
            "residual_search_references",
        ):
            self.assertEqual(self._count(table), 0, table)

    def test_every_crash_window_rolls_back_to_exact_pre_episode_state(self) -> None:
        for target in (
            "after_episode",
            "after_identity_and_proposals",
            "after_cell",
            "before_commit",
        ):
            with self.subTest(target=target):
                def inject(stage: str) -> None:
                    if stage == target:
                        raise InjectedCrash(stage)

                with self.assertRaisesRegex(InjectedCrash, target):
                    commit_deterministic_residual_batch(
                        self.registry,
                        self.coverage,
                        self.identities,
                        plan=self.plan,
                        batch=self.batch,
                        search_cost_seconds=0.1,
                        candidate_cap=8,
                        fault_injector=inject,
                    )
                self._assert_pre_episode_state()

    def test_fabric_idempotency_key_replays_without_new_domain_effects(self) -> None:
        first = commit_deterministic_residual_batch(
            self.registry,
            self.coverage,
            self.identities,
            plan=self.plan,
            batch=self.batch,
            search_cost_seconds=0.1,
            candidate_cap=8,
            idempotency_key="fabric-batch:1",
        )
        second = commit_deterministic_residual_batch(
            self.registry,
            self.coverage,
            self.identities,
            plan=self.plan,
            batch=self.batch,
            search_cost_seconds=0.1,
            candidate_cap=8,
            idempotency_key="fabric-batch:1",
        )

        self.assertEqual(second, first)
        stats = self.coverage.stats(self.cell)
        self.assertEqual(stats.attempts, 1)
        self.assertEqual(stats.variant_cursor, 1)
        self.assertEqual(self._count("source_search_episodes"), 1)
        self.assertEqual(self._count("source_proposals"), 1)
        marker = self.registry.connection.execute(
            """
            SELECT commit_kind
            FROM fabric_domain_commits
            WHERE idempotency_key=?
            """,
            ("fabric-batch:1",),
        ).fetchone()
        self.assertIsNotNone(marker)
        self.assertEqual(marker["commit_kind"], "residual-search")

    def test_success_commits_episode_identity_candidate_and_cursor_together(self) -> None:
        result = commit_deterministic_residual_batch(
            self.registry,
            self.coverage,
            self.identities,
            plan=self.plan,
            batch=self.batch,
            search_cost_seconds=0.1,
            candidate_cap=8,
        )

        self.assertEqual(result.registered_count, 1)
        self.assertEqual(result.new_source_count, 1)
        self.assertEqual(result.dropped_count, 1)
        stats = self.coverage.stats(self.cell)
        self.assertEqual(stats.attempts, 1)
        self.assertEqual(stats.result_count, 2)
        self.assertEqual(stats.duplicate_results, 1)
        self.assertEqual(stats.unique_roots, 1)
        self.assertEqual(stats.new_families, 1)
        self.assertEqual(stats.qualified_roots, 1)
        self.assertEqual(stats.variant_cursor, 1)
        self.assertEqual(self._count("source_search_episodes"), 1)
        self.assertEqual(self._count("source_candidates"), 1)
        self.assertEqual(self._count("source_proposals"), 1)
        self.assertEqual(self._count("residual_search_episode_cells"), 1)
        self.assertEqual(self._count("residual_search_references"), 2)
        episode = self.registry.get_search_episode(result.episode_id)
        self.assertIsNotNone(episode)
        assert episode is not None
        self.assertIsNotNone(episode.finished_at)
        self.assertAlmostEqual(episode.search_cost_seconds, 0.1)


if __name__ == "__main__":
    unittest.main()
