from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.source_research.models import (
    ArtifactLead, DecisionRecord, FrontierState, FrontierTask, QueryProgram,
    RewardKind, RewardRecord, RewardScope, RootKind, RootQuery, RootSurface,
    SearchCheckpoint, SearchHit,
)
from creeper.source_research.negative_knowledge import make_negative_knowledge
from creeper.source_research.ranking import ResearchYield
from creeper.source_research.registry import ResearchRegistry
from creeper.source_research.resolver import resolve_node
from creeper.source_research.resume import ResearchResumeManager
from creeper.storage.control_store import ControlStore


class ResearchStateKernelTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db = Path(self.tempdir.name) / "control.sqlite"
        self.store = ControlStore(self.db)
        self.reg = ResearchRegistry(self.store, clock=lambda: 100.0)
        self.reg.upsert_root(
            RootSurface(
                root_id="datacite",
                kind=RootKind.STRUCTURED_REPOSITORY,
                canonical_locator="https://api.datacite.org/dois",
            )
        )
        self.query = RootQuery(
            root_id="datacite",
            query_text="web archive collection",
            native_filters={"type": "dataset"},
        )
        self.program = QueryProgram(
            root_id="datacite",
            strategy="seed",
            queries=(self.query,),
            hard_max_requests=10,
            stop_conditions=("exhausted",),
        )
        self.reg.register_program(self.program)

    def tearDown(self):
        try:
            self.store.connection.close()
        except Exception:
            pass
        self.tempdir.cleanup()

    def test_cross_root_doi_dedup(self):
        a = self.reg.upsert_hit(
            SearchHit(
                "datacite", self.query.query_id, "10.1234/ABC",
                "https://doi.org/10.1234/ABC", "DOI",
            )
        )
        b = self.reg.upsert_hit(
            SearchHit(
                "zenodo", self.query.query_id, "998",
                "https://zenodo.org/records/998", "RECORD",
                metadata={"doi": "https://doi.org/10.1234/abc"},
            )
        )
        self.assertEqual(a.node_id, b.node_id)
        self.assertEqual(a.canonical_key, "doi:10.1234/abc")

    def test_artifact_mirror_dedup(self):
        a = ArtifactLead(
            "datacite", "a", "https://a.example/file.gz",
            checksum="sha256:DEAD", query_id=self.query.query_id,
            program_id=self.program.program_id,
        )
        b = ArtifactLead(
            "zenodo", "b", "https://b.example/mirror.gz",
            checksum="sha256:dead", query_id=self.query.query_id,
            program_id=self.program.program_id,
        )
        aid, _, _ = self.reg.register_artifact_lead(a)
        bid, _, _ = self.reg.register_artifact_lead(b)
        self.assertEqual(aid, bid)

    def test_seed_replay_suppression(self):
        replay = RootQuery(
            root_id="datacite",
            query_text=" WEB   archive COLLECTION ",
            native_filters={"type": "dataset"},
        )
        query_id, created = self.reg.register_query(self.program.program_id, replay)
        self.assertFalse(created)
        self.assertEqual(query_id, self.query.query_id)

    def test_restart_cursor_resume(self):
        self.reg.update_query_checkpoint(
            self.query.query_id,
            checkpoint=SearchCheckpoint(cursor="abc", page=4),
            state="RETRYABLE",
            pages_delta=3,
        )
        self.store.connection.close()
        self.store = ControlStore(self.db)
        reg2 = ResearchRegistry(self.store, clock=lambda: 200.0)
        checkpoint = reg2.query_checkpoint(self.query.query_id)
        self.assertIsNotNone(checkpoint)
        self.assertEqual(checkpoint.cursor, "abc")
        self.assertEqual(checkpoint.page, 4)

    def test_stale_lease_reclaim(self):
        task_id = self.reg.enqueue_frontier(
            FrontierTask("t1", "QUERY", "q1", priority=1.0)
        )
        claimed = self.reg.claim_frontier(
            owner="worker", lease_seconds=10, now=100
        )
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed.task_id, task_id)
        self.assertEqual(claimed.state, FrontierState.CLAIMED)
        self.assertEqual(self.reg.reclaim_stale_leases(now=111), 1)
        self.assertEqual(
            self.reg.get_frontier(task_id).state, FrontierState.READY
        )

    def test_final_reward_idempotent_and_lineage_closure(self):
        node = self.reg.upsert_hit(
            SearchHit(
                "datacite", self.query.query_id, "10.555/x",
                "https://doi.org/10.555/x", "DOI",
            )
        )
        lead = ArtifactLead(
            "datacite", "f1", "https://repo.example/f1.cdx.gz",
            checksum="sha256:123", query_id=self.query.query_id,
            program_id=self.program.program_id,
            source_node_id=node.node_id, pivot_id="pivot:1",
        )
        artifact_id, _, _ = self.reg.register_artifact_lead(lead)
        self.assertGreaterEqual(
            self.reg.bind_artifact_source(
                artifact_id,
                source_key="src:1",
                source_exposure_id="exp:1",
            ),
            1,
        )

        first = self.reg.close_final_reward(
            source_key="src:1", exposure_id="exp:1", final_eed=7.5,
            validation_closed=True, idempotency_token="close:1",
        )
        second = self.reg.close_final_reward(
            source_key="src:1", exposure_id="exp:1", final_eed=7.5,
            validation_closed=True, idempotency_token="close:1",
        )
        self.assertGreaterEqual(first, 5)
        self.assertEqual(second, 0)
        scopes = {
            reward.scope for reward in self.reg.rewards(kind=RewardKind.FINAL)
        }
        self.assertTrue(
            {
                RewardScope.SOURCE, RewardScope.ARTIFACT, RewardScope.QUERY,
                RewardScope.PROGRAM, RewardScope.ROOT, RewardScope.PIVOT,
            }.issubset(scopes)
        )

    def test_final_reward_requires_validation_closed(self):
        with self.assertRaises(ValueError):
            self.reg.close_final_reward(
                source_key="src:1", exposure_id="e", final_eed=1.0,
                validation_closed=False,
            )

    def test_policy_schema_rebuild_preserves_immutable_rewards(self):
        self.reg.enqueue_frontier(
            FrontierTask("t1", "QUERY", "q1", policy_version="p1")
        )
        self.reg.record_decision(
            DecisionRecord(
                "d1", "t1", "arm:a", "p1", "snap:1", 0.5, "ctx", 100.0
            )
        )
        self.reg.record_reward(
            RewardRecord(
                "r1", "PROXY", "QUERY", "q1", 2.0,
                decision_id="d1", policy_version="p1",
                idempotency_key="r1",
            )
        )
        self.reg.record_reward(
            RewardRecord(
                "r2", "FINAL", "QUERY", "q1", 3.0,
                decision_id="d1", policy_version="p1",
                validation_closed=True, idempotency_key="r2",
            )
        )
        stats = self.reg.rebuild_arm_stats(
            policy_version="p1", schema_version=2
        )
        self.assertEqual(stats[0].pulls, 1)
        self.assertEqual(stats[0].proxy_reward, 2.0)
        self.assertEqual(stats[0].final_reward, 3.0)
        self.assertEqual(stats[0].schema_version, 2)
        self.assertEqual(len(self.reg.rewards(policy_version="p1")), 2)

    def test_resume_rebuilds_missing_derived_stats(self):
        self.reg.enqueue_frontier(
            FrontierTask("t1", "QUERY", "q1", policy_version="p1")
        )
        self.reg.record_decision(
            DecisionRecord(
                "d1", "t1", "arm:a", "p1", "snap:1", 1.0, "ctx", 100.0
            )
        )
        report = ResearchResumeManager(self.reg).recover(
            policy_version="p1",
            expected_schema_version=3,
            now=100.0,
        )
        self.assertEqual(report.rebuilt_policy_version, "p1")
        self.assertEqual(
            self.reg.arm_stats(policy_version="p1")[0].schema_version, 3
        )

    def test_hit_heavy_no_artifact_loses_ranking(self):
        noisy = ResearchYield(hits=5000, resolved_artifacts=0, requests=10)
        useful = ResearchYield(
            hits=20, resolved_artifacts=4, productive_sources=1, requests=10
        )
        self.assertGreater(useful.score(), noisy.score())

    def test_incomplete_pagination_is_not_negative_knowledge(self):
        with self.assertRaises(ValueError):
            make_negative_knowledge(
                root_id="datacite",
                scope_kind="QUERY",
                scope_key="q1",
                reason="empty page",
                exhaustive=False,
                pagination_complete=False,
            )

    def test_resolution_produces_lead_not_evidence(self):
        from creeper.source_research.models import ResearchNode

        node = ResearchNode.from_hit(
            SearchHit(
                "zenodo", "q", "123",
                "https://zenodo.org/records/123", "RECORD",
                metadata={
                    "files": [{
                        "key": "x.cdx.gz",
                        "url": "https://x.test/x.cdx.gz",
                    }]
                },
            )
        )
        result = resolve_node(node, query_id="q", program_id="p")
        self.assertEqual(len(result.artifact_leads), 1)
        self.assertIsNone(result.artifact_leads[0].evidence_year)


if __name__ == "__main__":
    unittest.main()
