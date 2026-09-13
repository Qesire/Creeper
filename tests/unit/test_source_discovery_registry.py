from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.scheduler.leases import StateTransitionError
from creeper.source_discovery import (
    MeasurementMode,
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

    def test_common_crawl_corpus_cannot_be_registered_from_any_discovery_path(self) -> None:
        candidate = self.candidate(
            "CC-MAIN-2001-01-index",
            family="BULK_ARTIFACT",
        )
        candidate = SourceCandidate(
            canonical_entrypoint="https://index.commoncrawl.org/CC-MAIN-2001-01-index",
            source_family=candidate.source_family,
            level=candidate.level,
            discovered_by=candidate.discovered_by,
            discovery_strategy=candidate.discovery_strategy,
            expected_year_from=candidate.expected_year_from,
            expected_year_to=candidate.expected_year_to,
            expected_volume=candidate.expected_volume,
            temporal_semantics_prior=candidate.temporal_semantics_prior,
            enumerability_prior=candidate.enumerability_prior,
            direct_evidence_prior=candidate.direct_evidence_prior,
            baseline_overlap_prior=candidate.baseline_overlap_prior,
            access_cost_prior=candidate.access_cost_prior,
            adapter_cost_prior=candidate.adapter_cost_prior,
            confidence=candidate.confidence,
        )
        with self.assertRaisesRegex(ValueError, "Common Crawl"):
            self.registry.register_proposal(candidate)

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

    def test_scout_measurement_credits_originating_search_once_and_updates_delta(self) -> None:
        episode = self.registry.begin_search_episode(
            strategy="META_SOURCE_SEARCH",
            backend="web-search",
            query="historical web crawl",
            actor="agent:test",
            episode_id="search:reward",
        )
        candidate = self.candidate("rewarded/")
        self.registry.register_proposal(
            candidate,
            episode_id=episode.episode_id,
            proposal_id="proposal:reward",
        )
        self.registry.finish_search_episode(
            episode.episode_id,
            search_cost_seconds=2.0,
        )
        first = ScoutMeasurement(
            sampled_records=100,
            unique_hosts=80,
            novel_hosts=20,
            direct_host_years=0,
            requests=1,
            bytes_read=1024,
            elapsed_seconds=1.0,
            novel_eed=8.0,
        )
        self.registry.record_scout_measurement(candidate.source_key, first)
        self.assertEqual(
            self.registry.get_search_episode(episode.episode_id).accepted_novel_eed,
            8.0,
        )

        second = ScoutMeasurement(
            sampled_records=100,
            unique_hosts=80,
            novel_hosts=30,
            direct_host_years=0,
            requests=1,
            bytes_read=1024,
            elapsed_seconds=1.0,
            novel_eed=12.0,
        )
        self.registry.record_scout_measurement(candidate.source_key, second)

        rewarded = self.registry.get_search_episode(episode.episode_id)
        self.assertEqual(rewarded.accepted_novel_eed, 12.0)
        self.assertEqual(self.registry.strategy_rewards()[0].reward_per_cost, 6.0)

    def test_final_reward_supersedes_scout_proxy_and_credits_llm_hypothesis(self) -> None:
        episode_id = "llm:test-final"
        self.registry.begin_search_episode(
            strategy="EXPLOIT_SUCCESS",
            backend="codex-cli-subagent",
            query="expand a proven source",
            actor="codex:source-intelligence",
            episode_id=episode_id,
        )
        self.registry.begin_llm_episode(
            episode_id=episode_id,
            task_type="EXPLOIT_SUCCESS_PATTERN",
            backend="codex-cli-subagent",
            actor="codex:source-intelligence",
            context_hash="abc123",
            prompt_version="source-intelligence-v2",
        )
        hypothesis = {
            "hypothesis_id": f"{episode_id}:h1",
            "action": "PROBE_URL",
            "confidence": 0.8,
        }
        self.registry.register_llm_hypothesis(episode_id, hypothesis)
        candidate = self.candidate(
            "final-reward/",
            strategy="EXPLOIT_SUCCESS",
        )
        self.registry.register_proposal(
            candidate,
            episode_id=episode_id,
        )
        self.registry.link_llm_source(
            candidate.source_key,
            hypothesis_id=hypothesis["hypothesis_id"],
        )
        self.registry.finish_search_episode(
            episode_id,
            search_cost_seconds=2.0,
        )
        self.registry.finish_llm_episode(
            episode_id,
            cost_seconds=2.0,
        )

        self.registry.record_scout_measurement(
            candidate.source_key,
            ScoutMeasurement(
                sampled_records=100,
                unique_hosts=80,
                novel_hosts=20,
                direct_host_years=0,
                requests=1,
                bytes_read=1024,
                elapsed_seconds=1.0,
                novel_eed=8.0,
            ),
        )
        self.assertEqual(
            self.registry.get_search_episode(episode_id).accepted_novel_eed,
            8.0,
        )

        self.registry.record_final_reward(
            candidate.source_key,
            final_accepted_eed=3.5,
        )

        self.assertEqual(
            self.registry.get_search_episode(episode_id).accepted_novel_eed,
            3.5,
        )
        task_rewards = self.registry.llm_task_rewards()
        self.assertEqual(len(task_rewards), 1)
        self.assertEqual(
            task_rewards[0]["task_type"],
            "EXPLOIT_SUCCESS_PATTERN",
        )
        self.assertEqual(task_rewards[0]["credited_eed"], 3.5)

        # Later proxy remeasurement cannot overwrite formal final authority.
        self.registry.record_scout_measurement(
            candidate.source_key,
            ScoutMeasurement(
                sampled_records=100,
                unique_hosts=90,
                novel_hosts=50,
                direct_host_years=0,
                requests=2,
                bytes_read=2048,
                elapsed_seconds=1.0,
                novel_eed=25.0,
            ),
        )
        self.assertEqual(
            self.registry.get_search_episode(episode_id).accepted_novel_eed,
            3.5,
        )

    def test_residual_and_overlap_scout_signals_round_trip(self) -> None:
        candidate = self._to_scout_ready(self.candidate("residual/"))
        self.registry.transition(candidate.source_key, SourceState.SCOUTING)
        measurement = ScoutMeasurement(
            sampled_records=10,
            unique_hosts=5,
            novel_hosts=2,
            direct_host_years=0,
            requests=1,
            bytes_read=512,
            elapsed_seconds=0.5,
            novel_eed=2.0,
            singleton_observations=3,
            doubleton_observations=1,
            estimated_unseen_fraction=0.3,
            minhash_values=(1, 2, 3, 4),
        )
        self.registry.record_scout_measurement(
            candidate.source_key,
            measurement,
        )

        restored = self.registry.get_scout_measurement(candidate.source_key)
        self.assertIsNotNone(restored)
        assert restored is not None
        self.assertEqual(restored.singleton_observations, 3)
        self.assertEqual(restored.doubleton_observations, 1)
        self.assertAlmostEqual(restored.estimated_unseen_fraction, 0.3)
        self.assertEqual(restored.minhash_values, (1, 2, 3, 4))
        self.assertAlmostEqual(restored.residual_opportunity, 1.5)

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

    def test_year_aware_scout_measurement_round_trips_through_registry(self) -> None:
        candidate = self._to_scout_ready(self.candidate("dated/"))
        self.registry.transition(candidate.source_key, SourceState.SCOUTING)
        measurement = ScoutMeasurement(
            sampled_records=100,
            unique_hosts=40,
            novel_hosts=0,
            direct_host_years=0,
            requests=1,
            bytes_read=1_024,
            elapsed_seconds=2.0,
            novel_eed=0.0,
            measurement_mode=MeasurementMode.HOST_YEAR,
            observed_host_year_pairs=4,
            novel_host_year_pairs=3,
            novel_pair_eed=2.5,
        )
        self.registry.record_scout_measurement(candidate.source_key, measurement)

        restored = self.registry.get_scout_measurement(candidate.source_key)
        self.assertIsNotNone(restored)
        assert restored is not None
        self.assertEqual(restored.measurement_mode, MeasurementMode.HOST_YEAR)
        self.assertEqual(restored.observed_host_year_pairs, 4)
        self.assertEqual(restored.novel_host_year_pairs, 3)
        self.assertEqual(restored.novel_eed_for_ranking, 2.5)
        self.assertAlmostEqual(restored.measured_baseline_overlap, 0.25)

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


    def test_scout_measurement_is_authority_bound(self) -> None:
        candidate = self._to_scout_ready(self.candidate("authority-bound/"))
        self.registry.transition(candidate.source_key, SourceState.SCOUTING)
        self.registry.set_scout_authority(
            baseline_signature="baseline-a",
            model_signature="model-a",
        )
        measurement = ScoutMeasurement(
            sampled_records=20,
            unique_hosts=10,
            novel_hosts=5,
            direct_host_years=0,
            requests=1,
            bytes_read=512,
            elapsed_seconds=1.0,
            novel_eed=5.0,
        )

        self.registry.record_scout_measurement(
            candidate.source_key,
            measurement,
        )

        row = self.registry.connection.execute(
            """
            SELECT baseline_signature, model_signature
            FROM source_scout_metrics
            WHERE source_key = ?
            """,
            (candidate.source_key,),
        ).fetchone()
        self.assertEqual(
            (row["baseline_signature"], row["model_signature"]),
            ("baseline-a", "model-a"),
        )
        self.assertEqual(
            self.registry.get_scout_measurement(candidate.source_key),
            measurement,
        )

    def test_legacy_scout_measurement_is_stale(self) -> None:
        candidate = self._to_scout_ready(self.candidate("legacy-stale/"))
        self.registry.transition(candidate.source_key, SourceState.SCOUTING)
        measurement = ScoutMeasurement(
            sampled_records=20,
            unique_hosts=10,
            novel_hosts=4,
            direct_host_years=0,
            requests=1,
            bytes_read=512,
            elapsed_seconds=1.0,
            novel_eed=4.0,
        )
        self.registry.record_scout_measurement(
            candidate.source_key,
            measurement,
        )
        self.assertIsNotNone(
            self.registry.get_scout_measurement(candidate.source_key)
        )

        self.registry.set_scout_authority(
            baseline_signature="baseline-current",
            model_signature="model-current",
        )

        self.assertIsNone(
            self.registry.get_scout_measurement(candidate.source_key)
        )
        row = self.registry.connection.execute(
            """
            SELECT baseline_signature, model_signature
            FROM source_scout_metrics
            WHERE source_key = ?
            """,
            (candidate.source_key,),
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertIsNone(row["baseline_signature"])
        self.assertIsNone(row["model_signature"])

    def test_model_change_marks_scout_measurement_stale(self) -> None:
        candidate = self._to_scout_ready(self.candidate("model-stale/"))
        self.registry.transition(candidate.source_key, SourceState.SCOUTING)
        self.registry.set_scout_authority(
            baseline_signature="baseline-a",
            model_signature="model-a",
        )
        self.registry.record_scout_measurement(
            candidate.source_key,
            ScoutMeasurement(
                sampled_records=20,
                unique_hosts=10,
                novel_hosts=4,
                direct_host_years=0,
                requests=1,
                bytes_read=512,
                elapsed_seconds=1.0,
                novel_eed=4.0,
            ),
        )

        self.registry.set_scout_authority(
            baseline_signature="baseline-a",
            model_signature="model-b",
        )

        self.assertIsNone(
            self.registry.get_scout_measurement(candidate.source_key)
        )

    def test_stale_scout_proxy_does_not_credit_current_search_strategy(self) -> None:
        self.registry.set_scout_authority(
            baseline_signature="baseline-a",
            model_signature="model-a",
        )
        episode = self.registry.begin_search_episode(
            strategy="AUTHORITY_TEST",
            backend="test",
            query="find source",
            actor="agent:test",
            episode_id="search:authority-stale",
        )
        candidate = self.candidate("authority-reward/")
        self.registry.register_proposal(
            candidate,
            episode_id=episode.episode_id,
            proposal_id="proposal:authority-reward",
        )
        self.registry.finish_search_episode(
            episode.episode_id,
            search_cost_seconds=2.0,
        )
        self.registry.record_scout_measurement(
            candidate.source_key,
            ScoutMeasurement(
                sampled_records=20,
                unique_hosts=10,
                novel_hosts=8,
                direct_host_years=0,
                requests=1,
                bytes_read=512,
                elapsed_seconds=1.0,
                novel_eed=8.0,
            ),
        )
        self.assertEqual(
            self.registry.get_search_episode(
                episode.episode_id
            ).accepted_novel_eed,
            8.0,
        )

        self.registry.set_scout_authority(
            baseline_signature="baseline-b",
            model_signature="model-a",
        )

        self.assertEqual(
            self.registry.get_search_episode(
                episode.episode_id
            ).accepted_novel_eed,
            0.0,
        )
        rewards = self.registry.strategy_rewards()
        reward = next(
            item for item in rewards if item.strategy == "AUTHORITY_TEST"
        )
        self.assertEqual(reward.accepted_novel_eed, 0.0)
        self.assertIsNone(
            self.registry.get_scout_measurement(candidate.source_key)
        )

    def test_remeasurement_under_new_authority_restores_eligibility(self) -> None:
        self.registry.set_scout_authority(
            baseline_signature="baseline-a",
            model_signature="model-a",
        )
        episode = self.registry.begin_search_episode(
            strategy="AUTHORITY_REMEASURE",
            backend="test",
            query="find source",
            actor="agent:test",
            episode_id="search:authority-remeasure",
        )
        candidate = self.candidate("authority-remeasure/")
        self.registry.register_proposal(
            candidate,
            episode_id=episode.episode_id,
            proposal_id="proposal:authority-remeasure",
        )
        self.registry.finish_search_episode(
            episode.episode_id,
            search_cost_seconds=2.0,
        )
        self.registry.record_scout_measurement(
            candidate.source_key,
            ScoutMeasurement(
                sampled_records=20,
                unique_hosts=10,
                novel_hosts=8,
                direct_host_years=0,
                requests=1,
                bytes_read=512,
                elapsed_seconds=1.0,
                novel_eed=8.0,
            ),
        )
        self.registry.set_scout_authority(
            baseline_signature="baseline-b",
            model_signature="model-a",
        )
        replacement = ScoutMeasurement(
            sampled_records=20,
            unique_hosts=10,
            novel_hosts=5,
            direct_host_years=0,
            requests=1,
            bytes_read=512,
            elapsed_seconds=1.0,
            novel_eed=5.0,
        )

        self.registry.record_scout_measurement(
            candidate.source_key,
            replacement,
        )

        self.assertEqual(
            self.registry.get_scout_measurement(candidate.source_key),
            replacement,
        )
        self.assertEqual(
            self.registry.get_search_episode(
                episode.episode_id
            ).accepted_novel_eed,
            5.0,
        )
        row = self.registry.connection.execute(
            """
            SELECT reward_kind, baseline_signature, model_signature, credited_eed
            FROM source_search_reward_attribution
            WHERE source_key = ?
            """,
            (candidate.source_key,),
        ).fetchone()
        self.assertEqual(row["reward_kind"], "scout_proxy")
        self.assertEqual(row["baseline_signature"], "baseline-b")
        self.assertEqual(row["model_signature"], "model-a")
        self.assertEqual(float(row["credited_eed"]), 5.0)

    def test_authority_migration_retains_legacy_scout_row_as_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                control.connection.execute(
                    """
                    CREATE TABLE source_scout_metrics (
                        source_key TEXT PRIMARY KEY,
                        sampled_records INTEGER NOT NULL,
                        unique_hosts INTEGER NOT NULL,
                        novel_hosts INTEGER NOT NULL,
                        direct_host_years INTEGER NOT NULL,
                        requests INTEGER NOT NULL,
                        bytes_read INTEGER NOT NULL,
                        elapsed_seconds REAL NOT NULL,
                        novel_eed REAL NOT NULL,
                        measurement_mode TEXT NOT NULL DEFAULT 'HOST_ONLY',
                        observed_host_year_pairs INTEGER NOT NULL DEFAULT 0,
                        novel_host_year_pairs INTEGER NOT NULL DEFAULT 0,
                        novel_pair_eed REAL NOT NULL DEFAULT 0,
                        singleton_observations INTEGER NOT NULL DEFAULT 0,
                        doubleton_observations INTEGER NOT NULL DEFAULT 0,
                        estimated_unseen_fraction REAL NOT NULL DEFAULT 0,
                        measured_at REAL NOT NULL
                    ) WITHOUT ROWID
                    """
                )
                control.connection.execute(
                    """
                    INSERT INTO source_scout_metrics(
                        source_key, sampled_records, unique_hosts, novel_hosts,
                        direct_host_years, requests, bytes_read,
                        elapsed_seconds, novel_eed, measured_at
                    ) VALUES ('legacy-source', 1, 1, 1, 0, 1, 1, 1, 1, 1)
                    """
                )
                control.connection.commit()

                registry = SourceDiscoveryRegistry(control)
                columns = {
                    str(row[1])
                    for row in control.connection.execute(
                        "PRAGMA table_info(source_scout_metrics)"
                    )
                }
                self.assertIn("baseline_signature", columns)
                self.assertIn("model_signature", columns)
                registry.set_scout_authority(
                    baseline_signature="baseline-new",
                    model_signature="model-new",
                )

                self.assertIsNone(
                    registry.get_scout_measurement("legacy-source")
                )
                retained = control.connection.execute(
                    """
                    SELECT COUNT(*) AS n
                    FROM source_scout_metrics
                    WHERE source_key = 'legacy-source'
                    """
                ).fetchone()
                self.assertEqual(int(retained["n"]), 1)
            finally:
                control.close()



    def test_open_registry_observes_external_authority_replacement(self) -> None:
        self.registry.set_scout_authority(
            baseline_signature="baseline-a",
            model_signature="model-a",
        )
        candidate = self._to_scout_ready(self.candidate("external-authority/"))
        self.registry.transition(candidate.source_key, SourceState.SCOUTING)
        self.registry.record_scout_measurement(
            candidate.source_key,
            ScoutMeasurement(
                sampled_records=10,
                unique_hosts=8,
                novel_hosts=4,
                direct_host_years=0,
                requests=1,
                bytes_read=256,
                elapsed_seconds=1.0,
                novel_eed=4.0,
            ),
        )
        self.assertIsNotNone(
            self.registry.get_scout_measurement(candidate.source_key)
        )

        other_control = ControlStore(Path(self.tmp.name) / "control.sqlite3")
        try:
            other_registry = SourceDiscoveryRegistry(other_control)
            other_registry.set_scout_authority(
                baseline_signature="baseline-b",
                model_signature="model-a",
            )
        finally:
            other_control.close()

        self.assertEqual(
            self.registry.current_scout_authority,
            ("baseline-b", "model-a"),
        )
        self.assertIsNone(
            self.registry.get_scout_measurement(candidate.source_key)
        )

    def test_explicit_stale_measurement_is_retained_but_not_credited(self) -> None:
        self.registry.set_scout_authority(
            baseline_signature="baseline-b",
            model_signature="model-a",
        )
        episode = self.registry.begin_search_episode(
            strategy="STALE_EXECUTOR",
            backend="test",
            query="stale executor",
            actor="agent:test",
            episode_id="search:stale-executor",
        )
        candidate = self.candidate("stale-executor/")
        self.registry.register_proposal(
            candidate,
            episode_id=episode.episode_id,
            proposal_id="proposal:stale-executor",
        )
        self.registry.finish_search_episode(
            episode.episode_id,
            search_cost_seconds=1.0,
        )

        eligible = self.registry.record_scout_measurement(
            candidate.source_key,
            ScoutMeasurement(
                sampled_records=10,
                unique_hosts=8,
                novel_hosts=4,
                direct_host_years=0,
                requests=1,
                bytes_read=256,
                elapsed_seconds=1.0,
                novel_eed=4.0,
            ),
            baseline_signature="baseline-a",
            model_signature="model-a",
        )

        self.assertFalse(eligible)
        self.assertIsNone(
            self.registry.get_scout_measurement(candidate.source_key)
        )
        self.assertEqual(
            self.registry.get_search_episode(
                episode.episode_id
            ).accepted_novel_eed,
            0.0,
        )
        row = self.registry.connection.execute(
            """
            SELECT baseline_signature, model_signature
            FROM source_scout_metrics
            WHERE source_key = ?
            """,
            (candidate.source_key,),
        ).fetchone()
        self.assertEqual(
            (row["baseline_signature"], row["model_signature"]),
            ("baseline-a", "model-a"),
        )


    def test_late_stale_measurement_cannot_overwrite_current_remeasurement(self) -> None:
        self.registry.set_scout_authority(
            baseline_signature="baseline-b",
            model_signature="model-a",
        )
        candidate = self._to_scout_ready(self.candidate("late-stale/"))
        self.registry.transition(candidate.source_key, SourceState.SCOUTING)
        current = ScoutMeasurement(
            sampled_records=10,
            unique_hosts=8,
            novel_hosts=5,
            direct_host_years=0,
            requests=1,
            bytes_read=256,
            elapsed_seconds=1.0,
            novel_eed=5.0,
        )
        self.assertTrue(
            self.registry.record_scout_measurement(
                candidate.source_key,
                current,
            )
        )

        stale = ScoutMeasurement(
            sampled_records=10,
            unique_hosts=8,
            novel_hosts=9,
            direct_host_years=0,
            requests=1,
            bytes_read=256,
            elapsed_seconds=1.0,
            novel_eed=99.0,
        )
        self.assertFalse(
            self.registry.record_scout_measurement(
                candidate.source_key,
                stale,
                baseline_signature="baseline-a",
                model_signature="model-a",
            )
        )

        self.assertEqual(
            self.registry.get_scout_measurement(candidate.source_key),
            current,
        )
        row = self.registry.connection.execute(
            """
            SELECT baseline_signature, model_signature, novel_eed
            FROM source_scout_metrics
            WHERE source_key = ?
            """,
            (candidate.source_key,),
        ).fetchone()
        self.assertEqual(row["baseline_signature"], "baseline-b")
        self.assertEqual(row["model_signature"], "model-a")
        self.assertEqual(float(row["novel_eed"]), 5.0)


if __name__ == "__main__":
    unittest.main()
