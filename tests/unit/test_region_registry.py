from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.models import SourceCandidate
from creeper.source_discovery.negative_knowledge import NegativeKnowledge
from creeper.source_discovery.research_models import ExplorationRegion, RegionState
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.storage.control_store import ControlStore


def region(region_id="r1", **kw):
    values = dict(
        region_id=region_id,
        surface_kind="STATIC_LIST",
        root="https://example.test/catalog",
        purpose="historical catalog",
        query_family_json=json.dumps({"template": "x", "dimensions": {"year": [1996, 1997]}}),
        enumerator_spec_json=json.dumps({"kind": "STATIC_LIST"}),
        artifact_predicate_json=json.dumps({"suffix": [".cdx"]}),
        hard_bounds_json=json.dumps({"max_requests": 10}),
        stop_conditions_json=json.dumps({"on": "EOF"}),
        expected_source_family="catalog",
        expected_contract_family="cdx",
        context_hash="ctx",
    )
    values.update(kw)
    return ExplorationRegion(**values)


class RegionRegistryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.now = [1000.0]
        self.control = ControlStore(Path(self.tmp.name) / "control.sqlite3")
        self.registry = SourceDiscoveryRegistry(self.control, clock=lambda: self.now[0])

    def tearDown(self):
        self.control.close()
        self.tmp.cleanup()

    def ready(self, item):
        self.registry.register_region(item)
        self.registry.transition_region(item.region_id, RegionState.VALIDATED)
        self.registry.transition_region(item.region_id, RegionState.READY)

    def test_registration_and_policy_identity(self):
        first, inserted = self.registry.register_region(region())
        again, inserted_again = self.registry.register_region(region("other"))
        changed, changed_inserted = self.registry.register_region(region("third", compiler_version="v7.1"))
        self.assertTrue(inserted)
        self.assertFalse(inserted_again)
        self.assertEqual(first.region_id, again.region_id)
        self.assertTrue(changed_inserted)
        self.assertNotEqual(first.region_key, changed.region_key)

    def test_legal_transition_and_stale_checkpoint_fencing(self):
        self.ready(region())
        generation = self.registry.claim_region("r1")
        self.assertEqual(generation, 1)
        self.registry.checkpoint_region("r1", generation, {"query_index": 2, "requests": 3})
        reclaimed = self.registry.claim_region("r1")
        self.assertEqual(reclaimed, 2)
        with self.assertRaisesRegex(RuntimeError, "stale"):
            self.registry.checkpoint_region("r1", generation, {"query_index": 3})
        self.registry.checkpoint_region("r1", reclaimed, {"query_index": 4})
        self.registry.finish_region("r1", reclaimed, RegionState.EXHAUSTED)
        self.assertEqual(self.registry.get_region("r1").state, RegionState.EXHAUSTED)

    def test_region_source_edge_is_idempotent(self):
        self.ready(region())
        candidate = SourceCandidate(
            canonical_entrypoint="https://example.test/data.cdx",
            source_family="catalog",
            level="SOURCE",
            discovered_by="test",
            discovery_strategy="region",
        )
        self.registry.register_proposal(candidate)
        self.assertTrue(self.registry.add_region_source_edge("r1", candidate.source_key))
        self.assertFalse(self.registry.add_region_source_edge("r1", candidate.source_key))

    def test_negative_knowledge_ttl_and_semantic_persistence(self):
        self.registry.upsert_negative_knowledge(NegativeKnowledge(
            "region", "r1", "NO_TARGET_YEAR", created_at=1000.0, expires_at=1010.0))
        self.registry.upsert_negative_knowledge(NegativeKnowledge(
            "family", "catalog", "SEMANTIC_REJECTED", created_at=1000.0))
        self.assertEqual(len(self.registry.negative_knowledge_matches(
            scope_kind="region", scope_key="r1", reason_code="NO_TARGET_YEAR")), 1)
        self.now[0] = 1011.0
        self.assertEqual(self.registry.negative_knowledge_matches(
            scope_kind="region", scope_key="r1"), [])
        self.assertEqual(self.registry.prune_expired_negative_knowledge(), 1)
        self.now[0] = 2000.0
        self.assertEqual(len(self.registry.negative_knowledge_matches(
            scope_kind="family", scope_key="catalog")), 1)

    def test_region_reward_is_idempotent(self):
        self.ready(region())
        self.registry.record_region_reward("r1", scout_proxy_eed=2, final_accepted_eed=3, requests=4, bytes=5, cost_seconds=6)
        self.registry.record_region_reward("r1", scout_proxy_eed=2, final_accepted_eed=3, requests=4, bytes=5, cost_seconds=6)
        row = self.registry.connection.execute("SELECT * FROM source_region_rewards WHERE region_id='r1'").fetchone()
        self.assertEqual((row["scout_proxy_eed"], row["final_accepted_eed"], row["requests"]), (2, 3, 4))


if __name__ == "__main__":
    unittest.main()
