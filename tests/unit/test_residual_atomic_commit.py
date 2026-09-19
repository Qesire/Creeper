from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.deterministic_search import DeterministicSearchBatch
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.research_leads import (
    CURATED_RESEARCH_LEADS,
    ResearchLeadLedger,
)
from creeper.source_discovery.residual_atomic import commit_deterministic_residual_batch
from creeper.source_discovery.residual_search import (
    QueryPlan,
    ResidualSearchLedger,
    SearchCell,
    SearchCellScheduler,
    SearchCellState,
    query_program_length,
)
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

    def test_hard_negative_is_identified_but_never_proposed(self) -> None:
        leads = ResearchLeadLedger(self.registry.connection)
        leads.seed_curated(self.coverage)
        negative = canonicalize_search_result(
            RawSearchResult(
                provider="fixture",
                provider_result_id="ucb-hard-negative",
                url="https://repo.example/ucb-home-ip-trace.dat",
                title="UCB Home IP trace 1996",
                description="Berkeley public client trace",
            ),
            relevance_score=1.0,
            qualified=True,
        )
        batch = DeterministicSearchBatch(
            backend="fixture",
            query=self.plan.query,
            actor="deterministic:test",
            results=(negative,),
            search_cost_seconds=0.1,
        )

        result = commit_deterministic_residual_batch(
            self.registry,
            self.coverage,
            self.identities,
            plan=self.plan,
            batch=batch,
            search_cost_seconds=0.1,
            candidate_cap=8,
            research_leads=leads,
        )

        self.assertEqual(result.registered_count, 0)
        self.assertEqual(result.dropped_count, 1)
        self.assertEqual(self._count("source_candidates"), 0)
        self.assertEqual(self._count("residual_search_references"), 1)
        row = self.registry.connection.execute(
            """
            SELECT matched_count, last_source_key
            FROM source_research_leads_v1
            WHERE lead_id='ucb-home-ip-1996-public-trace'
            """
        ).fetchone()
        self.assertEqual(row["matched_count"], 1)
        self.assertIsNone(row["last_source_key"])

    def test_provenance_hold_is_retained_without_year_or_direct_prior(self) -> None:
        leads = ResearchLeadLedger(self.registry.connection)
        leads.seed_curated(self.coverage)
        held = canonicalize_search_result(
            RawSearchResult(
                provider="fixture",
                provider_result_id="nus-hold",
                url="https://www.comp.nus.edu.sg/research/nlanr-sample.zip",
                title="NUS NLANR teaching sample",
                resource_type="file",
            ),
            relevance_score=1.0,
            qualified=True,
        )
        batch = DeterministicSearchBatch(
            backend="fixture",
            query=self.plan.query,
            actor="deterministic:test",
            results=(held,),
            search_cost_seconds=0.1,
        )

        result = commit_deterministic_residual_batch(
            self.registry,
            self.coverage,
            self.identities,
            plan=self.plan,
            batch=batch,
            search_cost_seconds=0.1,
            candidate_cap=8,
            research_leads=leads,
        )

        self.assertEqual(result.registered_count, 1)
        candidate = self.registry.list_candidates()[0]
        self.assertEqual(candidate.state.value, "HOLD")
        self.assertEqual(
            candidate.state_reason,
            "RESEARCH_PROVENANCE_HOLD:nus-nlanr-sample",
        )
        self.assertIsNone(candidate.expected_year_from)
        self.assertIsNone(candidate.expected_year_to)
        self.assertEqual(candidate.temporal_semantics_prior, 0.0)
        self.assertEqual(candidate.direct_evidence_prior, 0.0)

    def test_exact_recovery_candidate_keeps_deterministic_lineage(self) -> None:
        leads = ResearchLeadLedger(self.registry.connection)
        leads.seed_curated(self.coverage)
        lead = next(
            item
            for item in CURATED_RESEARCH_LEADS
            if item.lead_id == "nlanr-uc-20000714"
        )
        assert lead.recovery_cell is not None
        plan = QueryPlan(
            cell=lead.recovery_cell,
            query='"2000" "uc.sanitized-access.20000714" "research lab" "log"',
            variant=0,
            exclusions=(),
            score=5.0,
            mechanism_phrase="uc.sanitized-access.20000714",
            include_institution=True,
            query_shape="STRICT_4D",
        )
        recovered = canonicalize_search_result(
            RawSearchResult(
                provider="fixture",
                provider_result_id="nlanr-recovered",
                url=(
                    "https://mirror.example/nlanr/"
                    "uc.sanitized-access.20000714.gz"
                ),
                title="historical proxy file",
                resource_type="file",
            ),
            relevance_score=1.0,
            qualified=True,
        )
        batch = DeterministicSearchBatch(
            backend="fixture",
            query=plan.query,
            actor="deterministic:test",
            results=(recovered,),
            search_cost_seconds=0.1,
        )

        result = commit_deterministic_residual_batch(
            self.registry,
            self.coverage,
            self.identities,
            plan=plan,
            batch=batch,
            search_cost_seconds=0.1,
            candidate_cap=8,
            research_leads=leads,
        )

        self.assertEqual(result.registered_count, 1)
        candidate = self.registry.list_candidates()[0]
        self.assertEqual(
            candidate.discovery_strategy,
            "RESEARCH_LEAD_RECOVERY",
        )
        self.assertEqual(
            candidate.source_family,
            "RESEARCH_RECOVERY:nlanr-uc-20000714",
        )
        row = self.registry.connection.execute(
            """
            SELECT matched_count, last_source_key
            FROM source_research_leads_v1
            WHERE lead_id='nlanr-uc-20000714'
            """
        ).fetchone()
        self.assertEqual(row["matched_count"], 1)
        self.assertEqual(row["last_source_key"], candidate.source_key)

    def test_atomic_positive_results_exhaust_recovery_program_without_wrap(self) -> None:
        leads = ResearchLeadLedger(self.registry.connection)
        leads.seed_curated(self.coverage)
        lead = next(
            item
            for item in CURATED_RESEARCH_LEADS
            if item.lead_id == "nlanr-uc-20000714"
        )
        assert lead.recovery_cell is not None
        self.assertEqual(query_program_length(lead.recovery_cell), 2)
        scheduler = SearchCellScheduler(
            self.coverage,
            priority_cell_keys=leads.recovery_cell_keys(),
        )

        for index in range(2):
            plan = scheduler.next_plans(limit=1)[0]
            self.assertEqual(plan.cell, lead.recovery_cell)
            result = canonicalize_search_result(
                RawSearchResult(
                    provider="fixture",
                    provider_result_id=f"nlanr-{index}",
                    url=(
                        f"https://mirror-{index}.example/nlanr/"
                        "uc.sanitized-access.20000714.gz"
                    ),
                    title=f"historical proxy file mirror {index}",
                    resource_type="file",
                    identifiers=(f"10.1234/nlanr-{index}",),
                ),
                relevance_score=1.0,
                qualified=True,
            )
            batch = DeterministicSearchBatch(
                backend="fixture",
                query=plan.query,
                actor="deterministic:test",
                results=(result,),
                search_cost_seconds=0.1,
            )
            commit_deterministic_residual_batch(
                self.registry,
                self.coverage,
                self.identities,
                plan=plan,
                batch=batch,
                search_cost_seconds=0.1,
                candidate_cap=8,
                research_leads=leads,
            )

        stats = self.coverage.stats(lead.recovery_cell)
        self.assertEqual(stats.variant_cursor, 2)
        self.assertEqual(stats.state, SearchCellState.EXHAUSTED)
        remaining = scheduler.next_plans(limit=1)
        self.assertTrue(
            not remaining or remaining[0].cell != lead.recovery_cell
        )

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
