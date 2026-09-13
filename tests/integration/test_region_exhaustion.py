from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.exploration_executor import (
    ExplorationExecutor,
    RegionExecutionCheckpoint,
)
from creeper.source_discovery.query_family import QueryFamily
from creeper.source_discovery.region_compilation import (
    CompiledScoutPlan,
    EnumeratorSpec,
    ExecutionBounds,
)
from creeper.source_discovery.research_models import (
    ExplorationRegion,
    RegionState,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.storage.control_store import ControlStore


class RegionExhaustionIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_registry_fence_and_executor_checkpoint_close_loop(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        try:
            control = ControlStore(Path(tmp.name) / "control.sqlite3")
            registry = SourceDiscoveryRegistry(control)
            region = ExplorationRegion(
                region_id="bulk-72",
                surface_kind="STATIC_LIST",
                root="https://data.example/",
                purpose="72 deterministic historical index queries",
                query_family_json=json.dumps(
                    {
                        "template": (
                            "https://data.example/{year}/"
                            "part-{part}.cdxj"
                        ),
                        "dimensions": {
                            "year": list(range(1996, 2002)),
                            "part": list(range(1, 13)),
                        },
                    }
                ),
                enumerator_spec_json=json.dumps(
                    {
                        "kind": "STATIC_LIST",
                        "config": {"urls": []},
                    }
                ),
                artifact_predicate_json=json.dumps(
                    {"suffix": [".cdxj"]}
                ),
                hard_bounds_json=json.dumps(
                    {
                        "max_queries": 72,
                        "max_pages": 100,
                        "max_artifacts": 72,
                        "max_requests": 100,
                        "max_bytes": 1_000_000,
                        "max_wall_seconds": 60,
                    }
                ),
                stop_conditions_json=json.dumps(
                    ["QUERY_FAMILY_EXHAUSTED"]
                ),
                expected_source_family="historical-index",
                expected_contract_family="CDXJ",
                context_hash="integration",
            )
            registry.register_region(region)
            registry.transition_region(
                region.region_id, RegionState.VALIDATED
            )
            registry.transition_region(region.region_id, RegionState.READY)
            generation = registry.claim_region(region.region_id)

            family = QueryFamily(
                template=(
                    "https://data.example/{year}/part-{part}.cdxj"
                ),
                dimensions={
                    "year": tuple(range(1996, 2002)),
                    "part": tuple(range(1, 13)),
                },
            )
            plan = CompiledScoutPlan(
                region_id=region.region_id,
                region_key=region.region_key,
                source_family=region.expected_source_family,
                surface_kind=region.surface_kind,
                root=region.root,
                query_family=family,
                enumerator=EnumeratorSpec(
                    "STATIC_LIST", {"urls": ()}
                ),
                artifact_predicate=lambda url: url.endswith(".cdxj"),
                hard_bounds=ExecutionBounds(
                    max_queries=72,
                    max_pages=100,
                    max_artifacts=72,
                    max_requests=100,
                    max_bytes=1_000_000,
                    max_wall_seconds=60,
                ),
                stop_conditions=("QUERY_FAMILY_EXHAUSTED",),
            )

            async def commit(batch, checkpoint):
                for candidate in batch:
                    registry.register_proposal(candidate)
                    registry.add_region_source_edge(
                        region.region_id,
                        candidate.source_key,
                    )
                registry.checkpoint_region(
                    region.region_id,
                    generation,
                    checkpoint.as_dict(),
                )

            result = await ExplorationExecutor().execute(
                plan, commit_batch=commit
            )
            self.assertTrue(result.terminal)
            self.assertEqual(len(result.candidates), 72)
            stored_checkpoint = registry.get_region_checkpoint(
                region.region_id
            )
            self.assertEqual(stored_checkpoint["query_index"], 72)
            registry.finish_region(
                region.region_id,
                generation,
                RegionState.EXHAUSTED,
                checkpoint=result.checkpoint.as_dict(),
            )
            self.assertEqual(
                registry.get_region(region.region_id).state,
                RegionState.EXHAUSTED,
            )
            edge_count = registry.connection.execute(
                """
                SELECT COUNT(*) AS n FROM source_region_edges
                WHERE region_id = ?
                """,
                (region.region_id,),
            ).fetchone()["n"]
            self.assertEqual(int(edge_count), 72)
            control.close()
        finally:
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
