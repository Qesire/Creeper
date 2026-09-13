from __future__ import annotations

import unittest

from creeper.source_discovery.exploration_executor import ExplorationExecutor
from creeper.source_discovery.region_compilation import CompiledScoutPlan, ExecutionBounds, EnumeratorSpec
from creeper.source_discovery.query_family import QueryFamily


class RegionExhaustionTests(unittest.IsolatedAsyncioTestCase):
    async def test_72_query_region_exhausts_without_model_calls(self) -> None:
        plan = CompiledScoutPlan(
            region_id="bulk",
            region_key="bulk-key",
            source_family="catalog",
            surface_kind="FILENAME_PATTERN",
            root="https://data.example/",
            query_family=QueryFamily(
                template="https://data.example/{year}/{part}.cdxj",
                dimensions={"year": tuple(range(1996, 2002)), "part": tuple(range(1, 13))},
            ),
            enumerator=EnumeratorSpec("FILENAME_PATTERN", {}),
            artifact_predicate=lambda url: url.endswith(".cdxj"),
            hard_bounds=ExecutionBounds(max_queries=72, max_pages=100, max_artifacts=72, max_requests=72, max_bytes=1_000_000, max_wall_seconds=60),
            stop_conditions=("QUERY_FAMILY_EXHAUSTED",),
        )
        result = await ExplorationExecutor().execute(plan)
        self.assertTrue(result.terminal)
        self.assertEqual(len(result.candidates), 72)
        self.assertEqual(result.checkpoint.query_index, 72)


if __name__ == "__main__":
    unittest.main()
