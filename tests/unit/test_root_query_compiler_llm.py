from __future__ import annotations

import unittest

from creeper.source_research.agent.compiler import (
    CompilerGateError,
    RootQueryCompiler,
    RootQueryCompilerError,
)
from creeper.source_research.agent.context import ResearchCompilerContext


def context(**overrides: object) -> ResearchCompilerContext:
    values: dict[str, object] = {
        "root_id": "datacite",
        "root_capabilities": ("filter:year", "filter:type", "filter:subject"),
        "seed_current_program_exhausted": True,
        "equivalent_unexecuted_program": False,
        "cooldown_satisfied": True,
        "recent_query_hashes": (),
        "unclassified_clusters": (),
        "productive_source_families": ("archive-index",),
        "saturated_source_families": ("modern-only",),
        "negative_knowledge": (),
    }
    values.update(overrides)
    return ResearchCompilerContext(**values)


def response(count: int = 20) -> dict[str, object]:
    return {
        "programs": [
            {
                "root_id": "datacite",
                "strategy": "orthogonal_year_search",
                "queries": [
                    {
                        "query": f'"web archive" {1996 + i % 6}',
                        "filters": {"year": 1996 + i % 6},
                        "expected_signal": "finite historical dataset",
                        "expected_family": "archive-index",
                        "max_pages": 3,
                    }
                    for i in range(count)
                ],
                "hard_max_requests": 80,
                "stop_conditions": ["no new artifact leads", "request budget exhausted"],
            }
        ],
        "pivot_programs": [],
        "new_root_hypotheses": [],
    }


class RootQueryCompilerLLMTests(unittest.TestCase):
    def test_one_call_creates_many_finite_actions(self) -> None:
        calls: list[ResearchCompilerContext] = []
        compiler = RootQueryCompiler(lambda value: calls.append(value) or response())

        result = compiler.compile_root_query_program(context())

        self.assertEqual(len(calls), 1)
        self.assertEqual(len(result.queries), 20)
        self.assertLessEqual(result.hard_max_requests, 80)
        self.assertTrue(all(query.max_pages == 3 for query in result.queries))

    def test_no_call_while_equivalent_program_is_unexecuted(self) -> None:
        called = False

        def model(_: ResearchCompilerContext) -> dict[str, object]:
            nonlocal called
            called = True
            return response()

        with self.assertRaises(CompilerGateError):
            RootQueryCompiler(model).compile_root_query_program(
                context(equivalent_unexecuted_program=True)
            )
        self.assertFalse(called)

    def test_same_context_deduplicates_recent_queries(self) -> None:
        first = RootQueryCompiler(lambda _: response(2)).compile_root_query_program(
            context()
        )
        compiler = RootQueryCompiler(lambda _: response(2))
        result = compiler.compile_root_query_program(
            context(recent_query_hashes=tuple(query.query_hash for query in first.queries))
        )
        self.assertEqual(result.queries, ())

    def test_single_ordinary_url_proposal_is_rejected(self) -> None:
        bad = response(1)
        bad["programs"] = [
            {
                **bad["programs"][0],
                "queries": [
                    {
                        "query": "https://example.org/page.html",
                        "filters": {},
                        "expected_signal": "page",
                        "expected_family": "web",
                        "max_pages": 1,
                    }
                ],
            }
        ]
        with self.assertRaises(RootQueryCompilerError):
            RootQueryCompiler(lambda _: bad).compile_root_query_program(context())

    def test_unsupported_api_filter_is_rejected(self) -> None:
        bad = response(1)
        bad["programs"][0]["queries"][0]["filters"] = {"cursor": "abc"}
        with self.assertRaises(RootQueryCompilerError):
            RootQueryCompiler(lambda _: bad).compile_root_query_program(context())

    def test_cluster_classification_is_not_per_hit(self) -> None:
        payload = {"classifications": [{"cluster_id": "c1", "classification": "MANIFEST", "reusable_surface": True, "rationale": "shared catalog"}]}
        result = RootQueryCompiler(lambda _: payload).classify_result_cluster(
            context(), {"cluster_id": "c1", "size": 3}
        )
        self.assertEqual(result[0]["classification"], "MANIFEST")
        with self.assertRaises(RootQueryCompilerError):
            RootQueryCompiler(lambda _: payload).classify_result_cluster(
                context(), {"cluster_id": "c1", "size": 1}
            )

    def test_new_root_requires_reusable_capabilities(self) -> None:
        payload = {"new_root_hypotheses": [{"kind": "CATALOG", "entrypoint": "https://example.test", "capabilities": ["search"], "rationale": "catalog"}]}
        with self.assertRaises(RootQueryCompilerError):
            RootQueryCompiler(lambda _: payload).propose_new_root(context())


if __name__ == "__main__":
    unittest.main()
