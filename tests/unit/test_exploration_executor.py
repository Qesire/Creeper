from __future__ import annotations

import asyncio
import unittest

from creeper.source_discovery.exploration_executor import (
    ExplorationExecutor,
    RegionExecutionCheckpoint,
)
from creeper.source_discovery.region_compilation import (
    CompiledScoutPlan,
    ExecutionBounds,
    EnumeratorSpec,
)


class ExplorationExecutorTests(unittest.IsolatedAsyncioTestCase):
    def plan(self, **kwargs) -> CompiledScoutPlan:
        bounds = ExecutionBounds(
            max_queries=kwargs.pop("max_queries", 72),
            max_pages=kwargs.pop("max_pages", 100),
            max_artifacts=kwargs.pop("max_artifacts", 1000),
            max_requests=kwargs.pop("max_requests", 1000),
            max_bytes=kwargs.pop("max_bytes", 10_000_000),
            max_wall_seconds=kwargs.pop("max_wall_seconds", 60),
        )
        return CompiledScoutPlan(
            region_id="r1",
            region_key="r1-key",
            source_family="synthetic",
            surface_kind="STATIC_LIST",
            root="https://data.example/",
            query_family=kwargs.pop("query_family", None),
            enumerator=kwargs.pop("enumerator", EnumeratorSpec("STATIC_LIST", {"urls": kwargs.pop("urls", ())})),
            artifact_predicate=kwargs.pop("artifact_predicate", None),
            hard_bounds=bounds,
            stop_conditions=kwargs.pop("stop_conditions", ("STATIC_LIST_EXHAUSTED",)),
        )

    async def test_static_list_dedupes_links_and_keeps_lineage(self) -> None:
        plan = self.plan(urls=("https://data.example/a.cdxj", "https://data.example/a.cdxj"))
        result = await ExplorationExecutor().execute(plan)
        self.assertTrue(result.terminal)
        self.assertEqual(len(result.candidates), 1)
        self.assertEqual(result.candidates[0].discovered_by, "region:r1")
        self.assertIn("region:r1", result.candidates[0].discovery_strategy)

    async def test_cursor_resumes_after_committed_batch(self) -> None:
        calls: list[object] = []
        async def fetcher(url, params, max_bytes):
            calls.append(params.get("cursor"))
            cursor = params.get("cursor")
            if cursor is None:
                return {"items": [{"url": "https://data.example/0"}], "next": "one", "bytes": 10}
            return {"items": [{"url": "https://data.example/1"}], "next": None, "bytes": 10}

        plan = self.plan(
            enumerator=EnumeratorSpec(
                "CURSOR_API",
                {"endpoint": "https://api.example/items", "params": {}, "record_selector": "items", "next_cursor_selector": "next"},
            ),
            max_pages=10,
        )
        committed: list[RegionExecutionCheckpoint] = []
        async def commit(batch, checkpoint):
            committed.append(checkpoint)
        first = await ExplorationExecutor(api_fetcher=fetcher).execute(plan, commit_batch=commit, stop_after_pages=1)
        self.assertFalse(first.terminal)
        second = await ExplorationExecutor(api_fetcher=fetcher).execute(plan, checkpoint=first.checkpoint, commit_batch=commit)
        self.assertTrue(second.terminal)
        self.assertEqual(calls, [None, "one"])
        self.assertEqual([c.cursor for c in committed], ["one", None])

    async def test_request_bound_is_non_terminal(self) -> None:
        plan = self.plan(
            urls=("https://data.example/a", "https://data.example/b"),
            max_requests=1,
        )
        result = await ExplorationExecutor().execute(plan)
        self.assertFalse(result.terminal)
        self.assertEqual(result.requests, 1)

    async def test_more_than_one_hundred_candidates_from_one_region(self) -> None:
        plan = self.plan(urls=tuple(f"https://data.example/{i}.cdxj" for i in range(101)))
        result = await ExplorationExecutor().execute(plan)
        self.assertEqual(len(result.candidates), 101)

    async def test_common_crawl_is_rejected_by_parent_predicate(self) -> None:
        plan = self.plan(
            urls=("https://data.commoncrawl.org/cc.cdxj",),
            artifact_predicate=lambda url: "commoncrawl" not in url,
        )
        result = await ExplorationExecutor().execute(plan)
        self.assertEqual(result.candidates, ())

    async def test_no_recursive_region_creation(self) -> None:
        plan = self.plan(
            surface_kind="HTML_CATALOG",
            enumerator=EnumeratorSpec("HTML_CATALOG", {"root": "https://data.example/catalog"}),
        )
        async def fetcher(url, max_bytes):
            return {"status": 200, "final_url": url, "body": b'<a href="/one.cdxj">one</a><a href="/nested/catalog">nested</a>', "bytes": 70}
        result = await ExplorationExecutor(html_fetcher=fetcher).execute(plan)
        self.assertEqual([c.canonical_entrypoint for c in result.candidates], ["https://data.example/one.cdxj"])


if __name__ == "__main__":
    unittest.main()
