from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.scheduler.leases import StateTransitionError
from creeper.source_discovery import (
    ScoutMeasurement,
    SourceCandidate,
    SourceDiscoveryRegistry,
    SourceLevel,
    SourceState,
    SuppressionScope,
    canonicalize_source_entrypoint,
)
from creeper.storage.control_store import ControlStore


class SourceDiscoveryRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.now = [1_800_000_000.0]
        self.control = ControlStore(Path(self.tmp.name) / "control.sqlite3")
        self.registry = SourceDiscoveryRegistry(
            self.control,
            max_graph_hops=2,
            clock=lambda: self.now[0],
        )

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    @staticmethod
    def candidate(
        name: str,
        *,
        family: str = "HISTORICAL_DIRECTORY",
        level: SourceLevel = SourceLevel.SOURCE,
        strategy: str = "EXPLORE_NEW_FAMILY",
        confidence: float = 0.6,
    ) -> SourceCandidate:
        return SourceCandidate(
            canonical_entrypoint=f"https://Example.COM:443/root/../{name}#section",
            source_family=family,
            level=level,
            discovered_by="agent:test",
            discovery_strategy=strategy,
            expected_year_from=1996,
            expected_year_to=2001,
            expected_volume=1_000,
            temporal_semantics_prior=0.7,
            enumerability_prior=0.8,
            direct_evidence_prior=0.4,
            baseline_overlap_prior=0.3,
            access_cost_prior=0.2,
            adapter_cost_prior=0.3,
            confidence=confidence,
        )

    def _to_scout_ready(self, candidate: SourceCandidate) -> SourceCandidate:
        self.registry.register_proposal(candidate)
        self.registry.transition(candidate.source_key, SourceState.TRIAGED)
        return self.registry.transition(candidate.source_key, SourceState.SCOUT_READY)

    def test_canonical_identity_is_conservative_and_stable(self) -> None:
        canonical = canonicalize_source_entrypoint(
            "HTTPS://ExAmPle.COM:443/a/../catalog/?x=2&x=1#client-fragment"
        )
        self.assertEqual(canonical, "https://example.com/catalog/?x=2&x=1")
        self.assertEqual(
            canonicalize_source_entrypoint("http://example.com:80"),
            "http://example.com/",
        )
        with self.assertRaises(ValueError):
            canonicalize_source_entrypoint("ftp://example.com/archive")
        with self.assertRaises(ValueError):
            canonicalize_source_entrypoint("https://user:secret@example.com/archive")

    def test_duplicate_proposals_share_candidate_but_preserve_episode_attribution(self) -> None:
        episode = self.registry.begin_search_episode(
            strategy="META_SOURCE_SEARCH",
            backend="web-search",
            query="1990s web archive collection index",
            actor="agent:meta",
            episode_id="search:1",
        )
        first = self.candidate(
            "catalog/",
            family="ARCHIVE_CATALOG",
            level=SourceLevel.METASOURCE,
            strategy="META_SOURCE_SEARCH",
        )
        stored, inserted = self.registry.register_proposal(
            first,
            episode_id=episode.episode_id,
            proposal_id="proposal:1",
        )
        self.assertTrue(inserted)
        self.assertEqual(stored.source_key, first.source_key)

        duplicate = SourceCandidate(
            canonical_entrypoint="https://example.com/catalog/",
            source_family="ALTERNATE_AGENT_LABEL",
            level=SourceLevel.COLLECTION,
            discovered_by="agent:second",
            discovery_strategy="EXPLOIT_SUCCESS",
            confidence=0.9,
        )
        stored_again, inserted_again = self.registry.register_proposal(
            duplicate,
            episode_id=episode.episode_id,
            proposal_id="proposal:2",
        )
        self.assertFalse(inserted_again)
        self.assertEqual(stored_again.source_family, "ARCHIVE_CATALOG")
        self.assertEqual(self.registry.proposal_count(first.source_key), 2)

        finished = self.registry.finish_search_episode(
            episode.episode_id,
            search_cost_seconds=2.0,
        )
        self.assertEqual(finished.search_cost_seconds, 2.0)
        credited = self.registry.credit_search_episode(
            episode.episode_id,
            accepted_novel_eed=6.0,
        )
        self.assertEqual(credited.reward_per_cost, 3.0)
        rewards = self.registry.strategy_rewards()
        self.assertEqual(len(rewards), 1)
        self.assertEqual(rewards[0].strategy, "META_SOURCE_SEARCH")
        self.assertEqual(rewards[0].reward_per_cost, 3.0)

    def test_agent_prior_cannot_promote_source_without_measured_scout(self) -> None:
        candidate = self.candidate("measured/")
        self.registry.register_proposal(candidate)
        self.registry.transition(candidate.source_key, SourceState.TRIAGED)
        self.registry.transition(candidate.source_key, SourceState.SCOUT_READY)
        self.registry.transition(candidate.source_key, SourceState.SCOUTING)

        with self.assertRaises(StateTransitionError):
            self.registry.transition(candidate.source_key, SourceState.WARM)

        measurement = ScoutMeasurement(
            sampled_records=100,
            unique_hosts=80,
            novel_hosts=20,
            direct_host_years=7,
            requests=4,
            bytes_read=50_000,
            elapsed_seconds=2.0,
            novel_eed=12.0,
        )
        self.registry.record_scout_measurement(candidate.source_key, measurement)
        warm = self.registry.transition(candidate.source_key, SourceState.WARM)
        active = self.registry.transition(candidate.source_key, SourceState.ACTIVE)

        self.assertEqual(warm.state, SourceState.WARM)
        self.assertEqual(active.state, SourceState.ACTIVE)
        stored_measurement = self.registry.get_scout_measurement(candidate.source_key)
        self.assertIsNotNone(stored_measurement)
        assert stored_measurement is not None
        self.assertAlmostEqual(stored_measurement.measured_baseline_overlap, 0.75)
        self.assertEqual(stored_measurement.novel_eed_per_second, 6.0)

    def test_source_family_negative_knowledge_removes_candidate_from_scout_ranking(self) -> None:
        meta = self.candidate(
            "meta/",
            family="SATURATED_FAMILY",
            level=SourceLevel.METASOURCE,
            confidence=0.95,
        )
        ordinary = self.candidate(
            "ordinary/",
            family="NEW_FAMILY",
            level=SourceLevel.SOURCE,
            confidence=0.4,
        )
        self._to_scout_ready(meta)
        self._to_scout_ready(ordinary)
        self.registry.suppress_candidate(
            meta,
            scope=SuppressionScope.FAMILY,
            reason="measured baseline overlap above saturation threshold",
        )

        ranked = self.registry.rank_scout_candidates(limit=10)

        self.assertEqual([item.source_key for item in ranked], [ordinary.source_key])
        self.assertIn("baseline overlap", self.registry.suppression_reason(meta) or "")

    def test_temporary_negative_knowledge_expires_and_is_pruned(self) -> None:
        candidate = self.candidate("temporary/")
        self._to_scout_ready(candidate)
        self.registry.suppress_candidate(
            candidate,
            reason="transiently unreachable",
            ttl_seconds=10.0,
        )
        self.assertIsNotNone(self.registry.suppression_reason(candidate))
        self.now[0] += 11.0
        self.assertIsNone(self.registry.suppression_reason(candidate))
        self.assertEqual(self.registry.prune_expired_suppressions(), 1)

    def test_recursive_cte_rejects_source_graph_cycle(self) -> None:
        meta = self.candidate("meta/", level=SourceLevel.METASOURCE)
        collection = self.candidate("collection/", level=SourceLevel.COLLECTION)
        source = self.candidate("source/", level=SourceLevel.SOURCE)
        for item in (meta, collection, source):
            self.registry.register_proposal(item)

        self.assertTrue(self.registry.add_edge(meta.source_key, collection.source_key))
        self.assertTrue(self.registry.add_edge(collection.source_key, source.source_key))
        self.assertFalse(self.registry.add_edge(collection.source_key, source.source_key))
        with self.assertRaisesRegex(ValueError, "cycle"):
            self.registry.add_edge(source.source_key, meta.source_key)
        self.assertEqual(self.registry.children(meta.source_key), [collection.source_key])

    def test_graph_depth_budget_blocks_recursive_metasource_explosion(self) -> None:
        root = self.candidate("root/", level=SourceLevel.METASOURCE)
        middle = self.candidate("middle/", level=SourceLevel.METASOURCE)
        leaf = self.candidate("leaf/", level=SourceLevel.COLLECTION)
        too_deep = self.candidate("too-deep/", level=SourceLevel.SOURCE)
        for item in (root, middle, leaf, too_deep):
            self.registry.register_proposal(item)

        self.registry.add_edge(root.source_key, middle.source_key)
        self.registry.add_edge(middle.source_key, leaf.source_key)
        with self.assertRaisesRegex(ValueError, "max_graph_hops"):
            self.registry.add_edge(leaf.source_key, too_deep.source_key)

    def test_inventory_tracks_reservoir_tiers_without_url_frontier_state(self) -> None:
        cold = self.candidate("cold/")
        ready = self.candidate("ready/")
        self.registry.register_proposal(cold)
        self._to_scout_ready(ready)

        inventory = self.registry.inventory()

        self.assertEqual(inventory[SourceState.DISCOVERED], 1)
        self.assertEqual(inventory[SourceState.SCOUT_READY], 1)
        self.assertEqual(inventory[SourceState.ACTIVE], 0)


if __name__ == "__main__":
    unittest.main()
