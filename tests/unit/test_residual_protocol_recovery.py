from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.deterministic_search import candidate_from_result
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.residual_recovery import (
    RESIDUAL_PROTOCOL_REVISION,
    recover_residual_protocol_state,
)
from creeper.source_discovery.residual_search import QueryPlan, ResidualSearchLedger, SearchCell
from creeper.source_discovery.search_identity import (
    RawSearchResult,
    SearchIdentityLedger,
    canonicalize_search_result,
)
from creeper.storage.control_store import ControlStore


class ResidualProtocolRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.control = ControlStore(self.root / "control.sqlite3")
        self.registry = SourceDiscoveryRegistry(self.control)
        self.coverage = ResidualSearchLedger(self.registry.connection)
        self.identities = SearchIdentityLedger(self.registry.connection)

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    @staticmethod
    def _plan(cell: SearchCell) -> QueryPlan:
        return QueryPlan(
            cell=cell,
            query=f'"{cell.period}" "proxy" "{cell.institution}" "{cell.artifact}"',
            variant=0,
            exclusions=(),
            score=1.0,
            mechanism_phrase="proxy",
            include_institution=True,
            query_shape="STRICT_4D",
        )

    @staticmethod
    def _result(label: str, doi: str):
        return canonicalize_search_result(
            RawSearchResult(
                provider="fixture",
                provider_result_id=label,
                url=f"https://repo.example/{label}.zip",
                title=f"1998 University Proxy Trace {label}",
                publisher="Example University",
                identifiers=(doi,),
            ),
            relevance_score=1.0,
            qualified=True,
        )

    def _legacy_episode(
        self,
        *,
        cell: SearchCell,
        label: str,
        doi: str,
        finished: bool,
    ) -> str:
        self.coverage.ensure_cell(cell)
        plan = self._plan(cell)
        result = self._result(label, doi)
        episode = self.registry.begin_search_episode(
            strategy=f"RESIDUAL_CELL:{cell.mechanism}",
            backend="fixture",
            query=plan.query,
            actor="deterministic:test",
        )
        self.coverage.bind_search_episode(cell, episode.episode_id)
        registration = self.identities.register(cell_key=cell.key, result=result)
        candidate = candidate_from_result(plan, result)
        self.registry.register_proposal(candidate, episode_id=episode.episode_id)
        self.coverage.record_episode(
            cell,
            result_count=1,
            duplicate_results=int(not registration.new_family),
            unique_roots=int(registration.new_dataset),
            new_families=int(registration.new_family),
            qualified_roots=int(result.qualified and registration.new_dataset),
            family_keys=(result.family_label,),
            search_cost_seconds=0.1,
        )
        if finished:
            self.registry.finish_search_episode(
                episode.episode_id,
                search_cost_seconds=0.1,
                accepted_proposals=1,
                new_sources=1,
            )
        return episode.episode_id

    def _count(self, table: str) -> int:
        row = self.registry.connection.execute(
            f"SELECT COUNT(*) AS n FROM {table}"
        ).fetchone()
        return int(row["n"])

    def test_revision_upgrade_resets_residual_memory_and_removes_dirty_episode(self) -> None:
        completed_cell = SearchCell("proxy_access", "university", "1998", "trace")
        dirty_cell = SearchCell("proxy_access", "research_lab", "1998", "trace")
        completed = self._legacy_episode(
            cell=completed_cell,
            label="completed",
            doi="10.1234/completed",
            finished=True,
        )
        dirty = self._legacy_episode(
            cell=dirty_cell,
            label="dirty",
            doi="10.1234/dirty",
            finished=False,
        )

        report = recover_residual_protocol_state(
            self.registry,
            self.coverage,
            self.identities,
        )

        self.assertTrue(report.protocol_reset)
        self.assertEqual(report.dirty_episodes, 1)
        self.assertEqual(report.reset_cells, 2)
        self.assertEqual(self._count("residual_search_references"), 0)
        self.assertEqual(self._count("residual_search_urls"), 0)
        self.assertEqual(self._count("residual_search_datasets"), 0)
        self.assertEqual(self._count("residual_search_episode_cells"), 0)
        self.assertIsNotNone(self.registry.get_search_episode(completed))
        self.assertIsNone(self.registry.get_search_episode(dirty))
        self.assertEqual(self.registry.proposal_count(candidate_from_result(
            self._plan(dirty_cell), self._result("dirty", "10.1234/dirty")
        ).source_key), 0)
        for cell in (completed_cell, dirty_cell):
            stats = self.coverage.stats(cell)
            self.assertEqual(stats.attempts, 0)
            self.assertEqual(stats.result_count, 0)
            self.assertEqual(stats.variant_cursor, 0)

        revision = self.registry.connection.execute(
            "SELECT value FROM residual_search_meta WHERE key='residual_protocol_revision'"
        ).fetchone()
        self.assertEqual(str(revision["value"]), RESIDUAL_PROTOCOL_REVISION)
        audit = self.registry.connection.execute(
            "SELECT value FROM residual_search_meta WHERE key='residual_protocol_recovery_last'"
        ).fetchone()
        payload = json.loads(str(audit["value"]))
        self.assertTrue(payload["protocol_reset"])
        self.assertEqual(payload["dirty_episodes"], 1)

        second = recover_residual_protocol_state(
            self.registry,
            self.coverage,
            self.identities,
        )
        self.assertFalse(second.changed)

    def test_current_revision_recovers_only_cell_bound_to_unfinished_episode(self) -> None:
        initial = recover_residual_protocol_state(
            self.registry,
            self.coverage,
            self.identities,
        )
        self.assertTrue(initial.protocol_reset)

        clean_cell = SearchCell("proxy_access", "university", "1998", "trace")
        dirty_cell = SearchCell("proxy_access", "research_lab", "1998", "trace")
        clean_episode = self._legacy_episode(
            cell=clean_cell,
            label="clean-current",
            doi="10.1234/clean-current",
            finished=True,
        )
        dirty_episode = self._legacy_episode(
            cell=dirty_cell,
            label="dirty-current",
            doi="10.1234/dirty-current",
            finished=False,
        )
        clean_before = self.coverage.stats(clean_cell)
        self.assertEqual(clean_before.attempts, 1)

        report = recover_residual_protocol_state(
            self.registry,
            self.coverage,
            self.identities,
        )

        self.assertFalse(report.protocol_reset)
        self.assertEqual(report.dirty_episodes, 1)
        self.assertEqual(report.reset_cells, 1)
        self.assertIsNotNone(self.registry.get_search_episode(clean_episode))
        self.assertIsNone(self.registry.get_search_episode(dirty_episode))
        clean_after = self.coverage.stats(clean_cell)
        dirty_after = self.coverage.stats(dirty_cell)
        self.assertEqual(clean_after.attempts, 1)
        self.assertEqual(clean_after.result_count, 1)
        self.assertEqual(clean_after.variant_cursor, 1)
        self.assertEqual(dirty_after.attempts, 0)
        self.assertEqual(dirty_after.result_count, 0)
        self.assertEqual(dirty_after.variant_cursor, 0)

        refs = self.registry.connection.execute(
            "SELECT cell_key FROM residual_search_references ORDER BY cell_key"
        ).fetchall()
        self.assertEqual([str(row["cell_key"]) for row in refs], [clean_cell.key])


if __name__ == "__main__":
    unittest.main()
