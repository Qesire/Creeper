from __future__ import annotations

import unittest

from creeper.source_discovery.research_compiler import ResearchCompiler
from creeper.source_research.agent.compiler import (
    CompilerGateError,
    PolicyDistillerCompiler,
    RootQueryCompiler,
    RootQueryCompilerError,
    UnifiedCompilerError,
    UnifiedResearchCompiler,
)
from creeper.source_research.agent.context import (
    LearningCompilerContext,
    ResearchCompilerContext,
)
from creeper.source_research.agent.protocol import (
    ExplorationRegionProposal,
    NegativeRuleProposal,
    ProposalType,
    UnifiedLLMTask,
)


def research_context(**overrides):
    values = {
        "root_id": "datacite",
        "root_capabilities": ("filter:year", "search", "enumerate"),
        "seed_current_program_exhausted": True,
        "equivalent_unexecuted_program": False,
        "cooldown_satisfied": True,
        "deterministic_seed_search_available": True,
        "metrics_available": True,
        "recent_query_hashes": (),
    }
    values.update(overrides)
    return ResearchCompilerContext(**values)


def learning_context(**overrides):
    values = {
        "learning_epoch_ready": True,
        "minimum_batch_satisfied": True,
        "replay_available": True,
        "final_reward_available": True,
        "policy_snapshot_id": "policy:1",
        "lineage_available": True,
        "rule_persistence_available": True,
    }
    values.update(overrides)
    return LearningCompilerContext(**values)


def region_payload():
    return {
        "query": "enumerate reusable historical archive indexes",
        "proposals": [
            {
                "type": "ExplorationRegionProposal",
                "proposal_id": "region-1",
                "surface_kind": "HTTP_API",
                "root": "https://example.test/archive",
                "purpose": "enumerate a bounded historical index family",
                "query_family": {
                    "year": [1996, 1997],
                    "kind": ["cdx"],
                },
                "enumerator": "INTEGER_PAGINATION",
                "artifact_predicate": {"suffixes": [".cdx", ".cdx.gz"]},
                "hard_bounds": {
                    "max_requests": 80,
                    "max_pages": 50,
                    "max_artifacts": 100,
                },
                "stop_conditions": [
                    "no next page",
                    "request budget exhausted",
                ],
                "expected_source_family": "HISTORICAL_ARCHIVE_INDEX",
                "expected_contract_family": "CDX",
                "expected_mechanism": "year-filtered index pages",
                "expected_fanout": 100,
                "reuse_key": "archive-index-by-year",
                "confidence": 0.8,
                "validation": {"method": "bounded_canary"},
            }
        ],
    }


def query_program_payload():
    return {
        "query": "compile bounded native root queries",
        "proposals": [
            {
                "type": "QueryProgramProposal",
                "proposal_id": "query-program-1",
                "root_id": "datacite",
                "strategy": "historical web archive dataset discovery",
                "queries": [
                    {
                        "query": "web archive CDX dataset",
                        "filters": {"year": 1999},
                        "expected_signal": "dataset or repository record",
                        "expected_family": "ARCHIVE_INDEX",
                        "max_pages": 4,
                    }
                ],
                "hard_max_requests": 20,
                "stop_conditions": ["query budget exhausted"],
                "reuse_key": "datacite-cdx-family",
            }
        ],
    }


def negative_rule_payload():
    return {
        "query": "distill a reusable negative rule",
        "proposals": [
            {
                "type": "NegativeRuleProposal",
                "proposal_id": "negative-1",
                "generalization_scope": {"root_family": "repository"},
                "preconditions": {"signal": "active-only corpus"},
                "bounded_expansion": {"max_roots": 8},
                "hard_bounds": {"max_requests": 8},
                "stop_conditions": ["scope exhausted"],
                "expected_mechanism": "avoid repeated active-only dead ends",
                "failure_modes": ["historical export exists under a separate surface"],
                "reuse_key": "negative-active-only-corpus",
                "confidence": 0.9,
            }
        ],
    }


class UnifiedCompilerTests(unittest.TestCase):
    def test_execution_region_is_bounded_and_compilable(self):
        envelope = UnifiedResearchCompiler().compile(
            region_payload(),
            task_type=UnifiedLLMTask.DISCOVER_NEW_SOURCE,
        )
        self.assertEqual(len(envelope.proposals), 1)
        self.assertIsInstance(
            envelope.proposals[0], ExplorationRegionProposal
        )

        plans = ResearchCompiler().compile_response(
            region_payload(),
            context_hash="ctx-1",
            episode_id="episode-1",
        )
        self.assertEqual(len(plans), 1)
        self.assertEqual(plans[0].expected_fanout, 100)
        self.assertEqual(plans[0].context_hash, "ctx-1")

    def test_authority_and_runtime_fields_are_rejected(self):
        payload = region_payload()
        payload["hits"] = [{"url": "https://example.test/one"}]
        with self.assertRaisesRegex(UnifiedCompilerError, "forbidden"):
            UnifiedResearchCompiler().compile(
                payload,
                task_type=UnifiedLLMTask.DISCOVER_NEW_SOURCE,
            )

    def test_nested_runtime_and_authority_state_is_rejected(self):
        for field in ("cursor", "hits", "evidence", "submission_authority"):
            payload = region_payload()
            payload["proposals"][0]["validation"] = {field: "forbidden"}
            with self.subTest(field=field):
                with self.assertRaises(UnifiedCompilerError):
                    UnifiedResearchCompiler().compile(
                        payload,
                        task_type=UnifiedLLMTask.DISCOVER_NEW_SOURCE,
                    )

    def test_url_list_is_rejected_in_reusable_region(self):
        payload = region_payload()
        payload["proposals"][0]["query_family"] = {
            "urls": [
                "https://example.test/a",
                "https://example.test/b",
            ]
        }
        with self.assertRaisesRegex(UnifiedCompilerError, "URL list"):
            UnifiedResearchCompiler().compile(
                payload,
                task_type=UnifiedLLMTask.DISCOVER_NEW_SOURCE,
            )

    def test_duplicate_reuse_identity_is_rejected(self):
        payload = region_payload()
        second = dict(payload["proposals"][0])
        second["proposal_id"] = "region-2"
        payload["proposals"].append(second)
        with self.assertRaisesRegex(UnifiedCompilerError, "duplicate reuse_key"):
            UnifiedResearchCompiler().compile(
                payload,
                task_type=UnifiedLLMTask.DISCOVER_NEW_SOURCE,
            )

    def test_root_query_is_suppressed_by_deterministic_frontier(self):
        context = research_context(seed_current_program_exhausted=False)
        with self.assertRaisesRegex(CompilerGateError, "not exhausted"):
            UnifiedResearchCompiler().compile(
                query_program_payload(),
                task_type=UnifiedLLMTask.COMPILE_ROOT_QUERY_PROGRAM,
                context=context,
            )

    def test_equivalent_unexecuted_program_suppresses_llm(self):
        context = research_context(equivalent_unexecuted_program=True)
        with self.assertRaisesRegex(CompilerGateError, "unexecuted"):
            UnifiedResearchCompiler().compile(
                query_program_payload(),
                task_type=UnifiedLLMTask.COMPILE_ROOT_QUERY_PROGRAM,
                context=context,
            )

    def test_unified_root_query_requires_metrics_and_seed_runtime(self):
        for context in (
            research_context(deterministic_seed_search_available=False),
            research_context(metrics_available=False),
        ):
            with self.assertRaises(CompilerGateError):
                UnifiedResearchCompiler().compile(
                    query_program_payload(),
                    task_type=UnifiedLLMTask.COMPILE_ROOT_QUERY_PROGRAM,
                    context=context,
                )

    def test_unsupported_root_native_filter_is_rejected(self):
        payload = query_program_payload()
        payload["proposals"][0]["queries"][0]["filters"] = {"publisher": "x"}
        with self.assertRaisesRegex(UnifiedCompilerError, "unsupported"):
            UnifiedResearchCompiler().compile(
                payload,
                task_type=UnifiedLLMTask.COMPILE_ROOT_QUERY_PROGRAM,
                context=research_context(),
            )

    def test_common_crawl_root_is_rejected(self):
        with self.assertRaisesRegex(UnifiedCompilerError, "Common Crawl"):
            UnifiedResearchCompiler().compile(
                {
                    "query": "propose a reusable root",
                    "proposals": [
                        {
                            "type": "RootSurfaceProposal",
                            "proposal_id": "root-1",
                            "kind": "API",
                            "entrypoint": "https://index.commoncrawl.org/",
                            "capabilities": ["search", "enumerate"],
                            "rationale": "large corpus",
                            "hard_bounds": {"max_requests": 10},
                            "stop_conditions": ["budget exhausted"],
                            "reuse_key": "commoncrawl-index",
                            "confidence": 0.9,
                        }
                    ],
                },
                task_type=UnifiedLLMTask.PROPOSE_NEW_ROOT,
                context=research_context(),
            )

    def test_ordinary_url_is_not_a_query_program(self):
        payload = query_program_payload()
        payload["proposals"][0]["queries"][0] = {
            "query": "https://example.test/item/1",
            "filters": {},
            "expected_signal": "one item",
            "expected_family": "ITEM",
            "max_pages": 1,
        }
        with self.assertRaisesRegex(UnifiedCompilerError, "ordinary URL"):
            UnifiedResearchCompiler().compile(
                payload,
                task_type=UnifiedLLMTask.COMPILE_ROOT_QUERY_PROGRAM,
                context=research_context(),
            )

    def test_learning_requires_replay_and_final_reward(self):
        for context in (
            learning_context(replay_available=False),
            learning_context(final_reward_available=False),
            learning_context(learning_epoch_ready=False),
            learning_context(minimum_batch_satisfied=False),
        ):
            with self.assertRaises(CompilerGateError):
                UnifiedResearchCompiler().compile(
                    negative_rule_payload(),
                    task_type=UnifiedLLMTask.DISTILL_NEGATIVE_CLUSTER,
                    context=context,
                )

    def test_learning_requires_lineage_and_rule_persistence(self):
        for context in (
            learning_context(lineage_available=False),
            learning_context(rule_persistence_available=False),
        ):
            with self.assertRaises(CompilerGateError):
                UnifiedResearchCompiler().compile(
                    negative_rule_payload(),
                    task_type=UnifiedLLMTask.DISTILL_NEGATIVE_CLUSTER,
                    context=context,
                )

    def test_learning_output_remains_proposal_only(self):
        envelope = PolicyDistillerCompiler(
            lambda _: negative_rule_payload()
        ).compile(
            learning_context(),
            task_type=UnifiedLLMTask.DISTILL_NEGATIVE_CLUSTER,
        )
        self.assertEqual(len(envelope.proposals), 1)
        self.assertIsInstance(envelope.proposals[0], NegativeRuleProposal)
        self.assertEqual(
            envelope.proposals[0].proposal_type,
            ProposalType.NEGATIVE_RULE,
        )
        self.assertFalse(hasattr(envelope.proposals[0], "active"))

    def test_context_hash_is_deterministic(self):
        first = research_context()
        second = research_context()
        self.assertEqual(first.context_hash, second.context_hash)


class RootQueryCompatibilityTests(unittest.TestCase):
    def test_legacy_root_query_compiler_public_api_is_preserved(self):
        payload = {
            "programs": [
                {
                    "root_id": "datacite",
                    "strategy": "archive search",
                    "queries": [
                        {
                            "query": "historical web archive index",
                            "filters": {"year": 1998},
                            "expected_signal": "repository record",
                            "expected_family": "ARCHIVE_INDEX",
                            "max_pages": 2,
                        }
                    ],
                    "hard_max_requests": 10,
                    "stop_conditions": ["budget exhausted"],
                }
            ]
        }
        program = RootQueryCompiler(lambda _: payload).compile_root_query_program(
            research_context()
        )
        self.assertEqual(program.root_id, "datacite")
        self.assertEqual(len(program.queries), 1)
        self.assertTrue(program.program_id.startswith("program:"))

    def test_legacy_compiler_rejects_child_pagination_state(self):
        bad = {
            "programs": [],
            "cursor": "child-owned-cursor",
        }
        with self.assertRaisesRegex(
            (RootQueryCompilerError, UnifiedCompilerError), "forbidden"
        ):
            RootQueryCompiler(lambda _: bad).compile_root_query_program(
                research_context()
            )


if __name__ == "__main__":
    unittest.main()
