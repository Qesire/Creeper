from __future__ import annotations

import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from creeper.source_discovery_service import _region_runtime_adapters


class _Checkpoint:
    def __init__(
        self,
        *,
        query_index=0,
        cursor=None,
        page=0,
        requests=0,
        bytes_read=0,
        results_seen=0,
        new_candidates=0,
        duplicate_candidates=0,
    ) -> None:
        self.query_index = query_index
        self.cursor = cursor
        self.page = page
        self.requests = requests
        self.bytes_read = bytes_read
        self.results_seen = results_seen
        self.new_candidates = new_candidates
        self.duplicate_candidates = duplicate_candidates

    def as_dict(self):
        return {
            "query_index": self.query_index,
            "cursor": self.cursor,
            "page": self.page,
            "requests": self.requests,
            "bytes_read": self.bytes_read,
            "results_seen": self.results_seen,
            "new_candidates": self.new_candidates,
            "duplicate_candidates": self.duplicate_candidates,
        }


class _Executor:
    def __init__(self, *, html_fetcher=None, api_fetcher=None) -> None:
        self.html_fetcher = html_fetcher
        self.api_fetcher = api_fetcher

    async def execute(self, plan, *, checkpoint=None, commit_batch=None):
        candidate = SimpleNamespace(source_key="source:test")
        next_checkpoint = _Checkpoint(
            query_index=1,
            requests=1,
            bytes_read=8,
            results_seen=1,
            new_candidates=1,
        )
        commit_batch((candidate,), next_checkpoint)
        return SimpleNamespace(
            terminal=True,
            checkpoint=next_checkpoint,
            candidates=(candidate,),
        )


class _RegionState:
    EXHAUSTED = "EXHAUSTED"
    HOLD = "HOLD"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"


class _Registry:
    def __init__(self) -> None:
        self.region = SimpleNamespace(
            region_id="region:running",
            state=SimpleNamespace(value="RUNNING"),
        )
        self.claims = []
        self.proposals = []
        self.edges = []
        self.checkpoints = []
        self.finishes = []

    def list_regions(self):
        return [self.region]

    def list_executable_regions(self, *, limit):
        self.limit = limit
        return []

    def claim_region(self, region_id):
        self.claims.append(region_id)
        return 7

    def get_region_checkpoint(self, region_id):
        return _Checkpoint(bytes_read=3).as_dict()

    def register_proposal(self, candidate):
        self.proposals.append(candidate.source_key)

    def add_region_source_edge(self, region_id, source_key):
        self.edges.append((region_id, source_key))

    def checkpoint_region(self, region_id, generation, checkpoint):
        self.checkpoints.append((region_id, generation, checkpoint))
        return True

    def finish_region(
        self,
        region_id,
        generation,
        terminal_state,
        *,
        checkpoint=None,
        reason="",
    ):
        self.finishes.append(
            (region_id, generation, terminal_state, checkpoint, reason)
        )
        return self.region


class L8RegionRuntimeContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_running_region_is_reclaimed_and_committed_serially(self) -> None:
        executor_module = types.ModuleType(
            "creeper.source_discovery.exploration_executor"
        )
        executor_module.ExplorationExecutor = _Executor
        executor_module.RegionExecutionCheckpoint = _Checkpoint

        compilation_module = types.ModuleType(
            "creeper.source_discovery.region_compilation"
        )
        compilation_module.compile_region = lambda _region: SimpleNamespace(
            hard_bounds=SimpleNamespace(max_bytes=64)
        )

        models_module = types.ModuleType(
            "creeper.source_discovery.research_models"
        )
        models_module.RegionState = _RegionState

        registry = _Registry()
        modules = {
            executor_module.__name__: executor_module,
            compilation_module.__name__: compilation_module,
            models_module.__name__: models_module,
        }
        with patch.dict(sys.modules, modules):
            planner, execute = _region_runtime_adapters(
                registry,  # type: ignore[arg-type]
                SimpleNamespace(),  # fake client; fake executor performs no I/O
            )
            self.assertIsNotNone(planner)
            self.assertIsNotNone(execute)
            regions = planner()
            self.assertEqual([item.region_id for item in regions], ["region:running"])
            result = await execute(regions[0])

        self.assertTrue(result.terminal)
        self.assertEqual(registry.claims, ["region:running"])
        self.assertEqual(registry.proposals, ["source:test"])
        self.assertEqual(
            registry.edges,
            [("region:running", "source:test")],
        )
        self.assertEqual(len(registry.checkpoints), 1)
        self.assertEqual(registry.checkpoints[0][1], 7)
        self.assertEqual(registry.finishes[0][2], "EXHAUSTED")
        self.assertEqual(registry.finishes[0][3]["new_candidates"], 1)


if __name__ == "__main__":
    unittest.main()
