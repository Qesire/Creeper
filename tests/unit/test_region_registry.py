from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from creeper.scheduler.leases import StateTransitionError
from creeper.source_discovery.models import SourceCandidate, SourceLevel
from creeper.source_discovery.negative_knowledge import NegativeKnowledge
from creeper.source_discovery.research_models import (
    ExplorationRegion,
    RegionState,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.storage.control_store import ControlStore


def make_region(region_id: str = "r1", **overrides) -> ExplorationRegion:
    values = {
        "region_id": region_id,
        "surface_kind": "STATIC_LIST",
        "root": "https://example.test/catalog",
        "purpose": "historical catalog",
        "query_family_json": json.dumps(
            {"template": "x", "dimensions": {"year": [1996, 1997]}}
        ),
        "enumerator_spec_json": json.dumps(
            {"kind": "STATIC_LIST", "config": {"urls": []}}
        ),
        "artifact_predicate_json": json.dumps({"suffix": [".cdxj"]}),
        "hard_bounds_json": json.dumps(
            {
                "max_queries": 10,
                "max_pages": 10,
                "max_artifacts": 100,
                "max_requests": 10,
                "max_bytes": 1000,
                "max_wall_seconds": 60,
            }
        ),
        "stop_conditions_json": json.dumps(["EXHAUSTED"]),
        "expected_source_family": "catalog",
        "expected_contract_family": "cdxj",
        "context_hash": "ctx",
    }
    values.update(overrides)
    return ExplorationRegion(**values)


class RegionRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.now = [1000.0]
        self.control = ControlStore(Path(self.tmp.name) / "control.sqlite3")
        self.registry = SourceDiscoveryRegistry(
            self.control,
            clock=lambda: self.now[0],
        )

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    def ready(self, region: ExplorationRegion) -> None:
        self.registry.register_region(region)
        self.registry.transition_region(
            region.region_id, RegionState.VALIDATED
        )
        self.registry.transition_region(region.region_id, RegionState.READY)

    def test_registration_dedupes_semantic_identity(self) -> None:
        first, inserted = self.registry.register_region(make_region("r1"))
        second, inserted_again = self.registry.register_region(make_region("r2"))
        self.assertTrue(inserted)
        self.assertFalse(inserted_again)
        self.assertEqual(first.region_id, second.region_id)
        self.assertEqual(first.region_key, second.region_key)

    def test_changed_compiler_identity_registers_new_region(self) -> None:
        _, inserted = self.registry.register_region(make_region("r1"))
        changed, changed_inserted = self.registry.register_region(
            make_region("r2", compiler_version="v7-integrated-l1-next")
        )
        self.assertTrue(inserted)
        self.assertTrue(changed_inserted)
        self.assertEqual(changed.region_id, "r2")

    def test_generation_fencing_and_reclaim_preserve_checkpoint(self) -> None:
        self.ready(make_region())
        generation = self.registry.claim_region("r1")
        checkpoint = {
            "query_index": 2,
            "cursor": "cursor-2",
            "page": 4,
            "requests": 3,
            "bytes_read": 50,
            "results_seen": 10,
            "new_candidates": 8,
            "duplicate_candidates": 2,
        }
        self.registry.checkpoint_region("r1", generation, checkpoint)
        reclaimed = self.registry.claim_region("r1")
        self.assertEqual(reclaimed, generation + 1)
        self.assertEqual(
            self.registry.get_region_checkpoint("r1"),
            checkpoint,
        )
        with self.assertRaisesRegex(RuntimeError, "stale"):
            self.registry.checkpoint_region(
                "r1",
                generation,
                {**checkpoint, "query_index": 3},
            )

    def test_checkpoint_regression_is_rejected(self) -> None:
        self.ready(make_region())
        generation = self.registry.claim_region("r1")
        self.registry.checkpoint_region(
            "r1",
            generation,
            {"query_index": 2, "requests": 4},
        )
        with self.assertRaisesRegex(RuntimeError, "regression"):
            self.registry.checkpoint_region(
                "r1",
                generation,
                {"query_index": 1, "requests": 4},
            )

    def test_illegal_region_transition_is_rejected(self) -> None:
        self.registry.register_region(make_region())
        with self.assertRaises(StateTransitionError):
            self.registry.transition_region("r1", RegionState.EXHAUSTED)

    def test_finish_requires_current_generation(self) -> None:
        self.ready(make_region())
        generation = self.registry.claim_region("r1")
        current = self.registry.claim_region("r1")
        with self.assertRaisesRegex(RuntimeError, "stale"):
            self.registry.finish_region(
                "r1", generation, RegionState.FAILED_RETRYABLE
            )
        result = self.registry.finish_region(
            "r1",
            current,
            RegionState.FAILED_RETRYABLE,
            checkpoint={"query_index": 1, "requests": 1},
        )
        self.assertEqual(result.state, RegionState.FAILED_RETRYABLE)

    def test_region_source_edge_is_idempotent(self) -> None:
        self.ready(make_region())
        candidate = SourceCandidate(
            canonical_entrypoint="https://example.test/data.cdxj",
            source_family="catalog",
            level=SourceLevel.SOURCE,
            discovered_by="test",
            discovery_strategy="region-test",
        )
        self.registry.register_proposal(candidate)
        self.assertTrue(
            self.registry.add_region_source_edge("r1", candidate.source_key)
        )
        self.assertFalse(
            self.registry.add_region_source_edge("r1", candidate.source_key)
        )

    def test_negative_knowledge_ttl_and_permanent_semantic_reject(self) -> None:
        self.registry.upsert_negative_knowledge(
            NegativeKnowledge(
                "region",
                "r1",
                "TEMPORARY_FETCH_FAILURE",
                created_at=1000.0,
                expires_at=1010.0,
            )
        )
        self.registry.upsert_negative_knowledge(
            NegativeKnowledge(
                "family",
                "catalog",
                "SEMANTICALLY_INELIGIBLE",
                created_at=1000.0,
            )
        )
        self.assertEqual(
            len(
                self.registry.negative_knowledge_matches(
                    scope_kind="region",
                    scope_key="r1",
                    reason_code="TEMPORARY_FETCH_FAILURE",
                )
            ),
            1,
        )
        self.now[0] = 1011.0
        self.assertEqual(
            self.registry.negative_knowledge_matches(
                scope_kind="region", scope_key="r1"
            ),
            [],
        )
        self.assertEqual(self.registry.prune_expired_negative_knowledge(), 1)
        self.now[0] = 10_000.0
        self.assertEqual(
            len(
                self.registry.negative_knowledge_matches(
                    scope_kind="family", scope_key="catalog"
                )
            ),
            1,
        )

    def test_final_reward_snapshot_is_idempotent_and_monotonic(self) -> None:
        self.ready(make_region())
        self.registry.record_region_reward(
            "r1",
            scout_proxy_eed=2.0,
            final_accepted_eed=3.0,
            requests=4,
            bytes=5,
            cost_seconds=6.0,
        )
        self.registry.record_region_reward(
            "r1",
            scout_proxy_eed=2.0,
            final_accepted_eed=3.0,
            requests=4,
            bytes=5,
            cost_seconds=6.0,
        )
        self.registry.record_region_reward(
            "r1",
            scout_proxy_eed=1.0,
            final_accepted_eed=1.0,
            requests=1,
            bytes=1,
            cost_seconds=1.0,
        )
        row = self.registry.connection.execute(
            "SELECT * FROM source_region_rewards WHERE region_id = ?",
            ("r1",),
        ).fetchone()
        self.assertEqual(float(row["scout_proxy_eed"]), 2.0)
        self.assertEqual(float(row["final_accepted_eed"]), 3.0)
        self.assertEqual(int(row["requests"]), 4)

    def test_llm_extension_migration_is_additive(self) -> None:
        columns = {
            str(row[1])
            for row in self.registry.connection.execute(
                "PRAGMA table_info(source_llm_episodes)"
            ).fetchall()
        }
        self.assertTrue(
            {
                "trigger_reason",
                "regions_proposed",
                "regions_accepted",
                "deterministic_actions",
                "final_credited_eed",
            }.issubset(columns)
        )


if __name__ == "__main__":
    unittest.main()
