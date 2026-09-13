from __future__ import annotations

import unittest

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


def plan(
    *,
    enumerator: EnumeratorSpec | None = None,
    query_family: QueryFamily | None = None,
    max_queries: int = 1000,
    max_pages: int = 1000,
    max_artifacts: int = 1000,
    max_requests: int = 1000,
    max_bytes: int = 10_000_000,
) -> CompiledScoutPlan:
    return CompiledScoutPlan(
        region_id="r1",
        region_key="region:r1",
        source_family="synthetic",
        surface_kind=(enumerator.kind if enumerator else "STATIC_LIST"),
        root="https://data.example/",
        query_family=query_family,
        enumerator=enumerator
        or EnumeratorSpec(
            "STATIC_LIST",
            {"urls": ("https://data.example/a.cdxj",)},
        ),
        artifact_predicate=lambda url: url.endswith(".cdxj"),
        hard_bounds=ExecutionBounds(
            max_queries=max_queries,
            max_pages=max_pages,
            max_artifacts=max_artifacts,
            max_requests=max_requests,
            max_bytes=max_bytes,
            max_wall_seconds=60,
        ),
        stop_conditions=("EXHAUSTED",),
    )


class ExplorationExecutorTests(unittest.IsolatedAsyncioTestCase):
    async def test_72_query_family_executes_without_model_dependency(self) -> None:
        family = QueryFamily(
            template="https://data.example/{year}/part-{part}.cdxj",
            dimensions={
                "year": tuple(range(1996, 2002)),
                "part": tuple(range(1, 13)),
            },
        )
        item = plan(
            query_family=family,
            enumerator=EnumeratorSpec("STATIC_LIST", {"urls": ()}),
            max_queries=72,
            max_artifacts=72,
        )
        result = await ExplorationExecutor().execute(item)
        self.assertTrue(result.terminal)
        self.assertEqual(result.checkpoint.query_index, 72)
        self.assertEqual(result.checkpoint.new_candidates, 72)
        self.assertEqual(len(result.candidates), 72)
        self.assertEqual(result.requests, 0)

    async def test_more_than_100_candidates_from_one_plan(self) -> None:
        urls = tuple(
            f"https://data.example/{index}.cdxj" for index in range(101)
        )
        item = plan(
            enumerator=EnumeratorSpec("STATIC_LIST", {"urls": urls}),
            max_artifacts=200,
        )
        result = await ExplorationExecutor().execute(item)
        self.assertTrue(result.terminal)
        self.assertEqual(len(result.candidates), 101)
        self.assertEqual(result.checkpoint.new_candidates, 101)

    async def test_exact_duplicate_does_not_consume_artifact_budget(self) -> None:
        item = plan(
            enumerator=EnumeratorSpec(
                "STATIC_LIST",
                {
                    "urls": (
                        "https://data.example/a.cdxj",
                        "https://data.example/a.cdxj",
                    )
                },
            ),
            max_artifacts=2,
        )
        result = await ExplorationExecutor().execute(item)
        self.assertTrue(result.terminal)
        self.assertEqual(len(result.candidates), 1)
        self.assertEqual(result.checkpoint.new_candidates, 1)
        self.assertEqual(result.checkpoint.duplicate_candidates, 1)

    async def test_commit_failure_does_not_advance_checkpoint(self) -> None:
        calls: list[object | None] = []

        async def fetcher(url, params, max_bytes):
            calls.append(params.get("cursor"))
            return {
                "items": [{"url": "https://data.example/0.cdxj"}],
                "next": "cursor-1",
                "bytes": 10,
            }

        async def fail_commit(batch, checkpoint):
            raise RuntimeError("simulated commit crash")

        item = plan(
            enumerator=EnumeratorSpec(
                "CURSOR_API",
                {
                    "endpoint": "https://api.example/items",
                    "record_selector": "items",
                    "next_cursor_selector": "next",
                },
            ),
        )
        initial = RegionExecutionCheckpoint()
        with self.assertRaisesRegex(RuntimeError, "commit crash"):
            await ExplorationExecutor(api_fetcher=fetcher).execute(
                item,
                checkpoint=initial,
                commit_batch=fail_commit,
            )

        committed: list[RegionExecutionCheckpoint] = []

        async def commit(batch, checkpoint):
            committed.append(checkpoint)

        replay = await ExplorationExecutor(api_fetcher=fetcher).execute(
            item,
            checkpoint=initial,
            commit_batch=commit,
            stop_after_pages=1,
        )
        self.assertFalse(replay.terminal)
        self.assertEqual(calls, [None, None])
        self.assertEqual(replay.checkpoint.cursor, "cursor-1")
        self.assertEqual(len(committed), 1)

    async def test_cursor_resume_starts_from_committed_cursor(self) -> None:
        calls: list[object | None] = []

        async def fetcher(url, params, max_bytes):
            cursor = params.get("cursor")
            calls.append(cursor)
            if cursor is None:
                return {
                    "items": [{"url": "https://data.example/0.cdxj"}],
                    "next": "cursor-1",
                    "bytes": 10,
                }
            return {
                "items": [{"url": "https://data.example/1.cdxj"}],
                "next": None,
                "bytes": 10,
            }

        item = plan(
            enumerator=EnumeratorSpec(
                "CURSOR_API",
                {
                    "endpoint": "https://api.example/items",
                    "record_selector": "items",
                    "next_cursor_selector": "next",
                },
            ),
        )
        first = await ExplorationExecutor(api_fetcher=fetcher).execute(
            item,
            stop_after_pages=1,
        )
        self.assertFalse(first.terminal)
        self.assertEqual(first.checkpoint.cursor, "cursor-1")

        second = await ExplorationExecutor(api_fetcher=fetcher).execute(
            item,
            checkpoint=first.checkpoint,
        )
        self.assertTrue(second.terminal)
        self.assertEqual(calls, [None, "cursor-1"])

    async def test_integer_pagination_resume_does_not_restart_page_one(self) -> None:
        pages: list[int] = []

        async def fetcher(url, page, max_bytes):
            pages.append(page)
            return {
                "items": [f"https://data.example/{page}.cdxj"],
                "bytes": 5,
                "terminal": page == 3,
            }

        item = plan(
            enumerator=EnumeratorSpec(
                "INTEGER_PAGINATION",
                {
                    "url_template": "https://api.example/page/{page}",
                    "start": 1,
                    "step": 1,
                    "max_page": 3,
                    "terminal_condition": "EXPLICIT",
                },
            ),
            max_pages=3,
        )
        first = await ExplorationExecutor(api_fetcher=fetcher).execute(
            item,
            stop_after_pages=1,
        )
        second = await ExplorationExecutor(api_fetcher=fetcher).execute(
            item,
            checkpoint=first.checkpoint,
        )
        self.assertTrue(second.terminal)
        self.assertEqual(pages, [1, 2, 3])

    async def test_request_bound_is_nonterminal_and_checkpointed(self) -> None:
        async def fetcher(url, params, max_bytes):
            return {
                "items": [{"url": "https://data.example/0.cdxj"}],
                "next": "cursor-1",
                "bytes": 10,
            }

        item = plan(
            enumerator=EnumeratorSpec(
                "CURSOR_API",
                {
                    "endpoint": "https://api.example/items",
                    "record_selector": "items",
                    "next_cursor_selector": "next",
                },
            ),
            max_requests=1,
        )
        result = await ExplorationExecutor(api_fetcher=fetcher).execute(item)
        self.assertFalse(result.terminal)
        self.assertEqual(result.requests, 1)
        self.assertEqual(result.checkpoint.cursor, "cursor-1")

    async def test_html_catalog_is_nonrecursive_and_origin_bounded(self) -> None:
        async def fetcher(url, max_bytes):
            return {
                "body": (
                    b'<a href="/one.cdxj">one</a>'
                    b'<a href="/nested/catalog">nested</a>'
                    b'<a href="https://evil.example/two.cdxj">evil</a>'
                ),
                "bytes": 120,
                "status": 200,
            }

        item = plan(
            enumerator=EnumeratorSpec(
                "HTML_CATALOG",
                {
                    "root": "https://data.example/catalog",
                    "origin_policy": "SAME_ORIGIN",
                },
            ),
        )
        result = await ExplorationExecutor(html_fetcher=fetcher).execute(item)
        self.assertTrue(result.terminal)
        self.assertEqual(
            [candidate.canonical_entrypoint for candidate in result.candidates],
            ["https://data.example/one.cdxj"],
        )

    async def test_common_crawl_provenance_is_parent_rejected(self) -> None:
        item = plan(
            enumerator=EnumeratorSpec(
                "STATIC_LIST",
                {"urls": ("https://data.commoncrawl.org/index.cdxj",)},
            )
        )
        result = await ExplorationExecutor().execute(item)
        self.assertTrue(result.terminal)
        self.assertEqual(result.candidates, ())


if __name__ == "__main__":
    unittest.main()
