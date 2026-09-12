from __future__ import annotations

import json
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from creeper.source_discovery.agent_search import (
    CommandAgentSearchExecutor,
    CommandAgentSearchPolicy,
    SearchAgentProtocolError,
)
from creeper.source_discovery.manager import SearchDirective, SearchDirectiveKind
from creeper.source_discovery.models import SourceLevel, SourceState


_HELPER = r'''
import argparse
import json
import time

parser = argparse.ArgumentParser()
parser.add_argument("mode")
parser.add_argument("--request", required=True)
parser.add_argument("--response", required=True)
args = parser.parse_args()
request = json.loads(open(args.request, encoding="utf-8").read())

if args.mode == "sleep":
    time.sleep(5)
elif args.mode == "fail":
    raise SystemExit(7)
elif args.mode == "oversize":
    open(args.response, "w", encoding="utf-8").write("x" * 4096)
elif args.mode == "hypothesis":
    payload = {
        "query": "expand successful annual source",
        "hypotheses": [{
            "hypothesis_id": "annual-cdx",
            "action": "ENUMERATE_TEMPLATE",
            "template": "https://archives.example/{YEAR}/index.cdxj",
            "variables": {"YEAR": [1999, 2000, 2001]},
            "candidate_defaults": {
                "source_family": "BULK_ARTIFACT",
                "level": "SOURCE",
                "expected_year_from": 1996,
                "expected_year_to": 2001,
                "expected_volume": 100000,
                "temporal_semantics_prior": 1.0,
                "enumerability_prior": 0.95,
                "direct_evidence_prior": 1.0,
                "baseline_overlap_prior": 0.5,
                "access_cost_prior": 0.5,
                "adapter_cost_prior": 0.5,
                "confidence": 0.9
            },
            "expected_mechanism": "annual CDXJ siblings",
            "confidence": 0.9,
            "validation": {
                "method": "HEAD_OR_RANGE",
                "max_requests": 3,
                "max_bytes": 65536
            }
        }]
    }
    open(args.response, "w", encoding="utf-8").write(json.dumps(payload))
elif args.mode == "success":
    payload = {
        "query": "historical web archive catalog 1996 2001",
        "candidates": [
            {
                "canonical_entrypoint": "https://archives.example/catalog/",
                "source_family": "RESOURCE_CATALOG",
                "level": "METASOURCE",
                "expected_year_from": 1996,
                "expected_year_to": 2001,
                "expected_volume": 25000,
                "enumerability_prior": 0.9,
                "confidence": 0.75,
            }
        ],
    }
    open(args.response, "w", encoding="utf-8").write(json.dumps(payload))
else:
    raise SystemExit(9)
'''


class CommandAgentSearchExecutorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.helper = self.root / "agent_helper.py"
        self.helper.write_text(textwrap.dedent(_HELPER), encoding="utf-8")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    @staticmethod
    def directive() -> SearchDirective:
        return SearchDirective(
            kind=SearchDirectiveKind.REFILL_RESERVOIR,
            strategy="META_SOURCE_SEARCH",
            desired_candidates=5,
            subject=None,
            reason="cold reserve below target",
        )

    def executor(self, mode: str, **policy_overrides) -> CommandAgentSearchExecutor:
        policy = CommandAgentSearchPolicy(**policy_overrides)
        return CommandAgentSearchExecutor(
            (sys.executable, str(self.helper), mode),
            self.root / f"invocations-{mode}",
            backend="command-agent-test",
            actor="agent:test",
            policy=policy,
        )

    async def test_success_uses_file_contract_and_fixed_creeper_attribution(self) -> None:
        executor = self.executor("success")

        batch = await executor(self.directive())

        self.assertEqual(batch.backend, "command-agent-test")
        self.assertEqual(batch.actor, "agent:test")
        self.assertEqual(batch.query, "historical web archive catalog 1996 2001")
        self.assertEqual(len(batch.candidates), 1)
        candidate = batch.candidates[0]
        self.assertEqual(candidate.level, SourceLevel.METASOURCE)
        self.assertEqual(candidate.state, SourceState.DISCOVERED)
        self.assertEqual(candidate.discovered_by, "agent:test")
        self.assertEqual(candidate.discovery_strategy, "META_SOURCE_SEARCH")

        invocation_dirs = list((self.root / "invocations-success").iterdir())
        self.assertEqual(len(invocation_dirs), 1)
        request = json.loads((invocation_dirs[0] / "request.json").read_text(encoding="utf-8"))
        self.assertEqual(
            request["contract"],
            "creeper.llm-source-intelligence.v2",
        )
        self.assertEqual(request["execution"]["mode"], "SUBAGENT")
        self.assertEqual(request["execution"]["authority"], "proposal_only")
        self.assertEqual(
            request["task"]["task_type"],
            "DISCOVER_NEW_SOURCE",
        )
        self.assertEqual(request["task"]["desired_candidates"], 5)
        # Transitional aliases keep existing finite search helpers compatible.
        self.assertEqual(request["desired_candidates"], 5)
        self.assertEqual(request["target_year_from"], 1996)
        self.assertEqual(request["target_year_to"], 2001)
        self.assertTrue(request["requirements"]["prefer_metasources"])

    async def test_hypothesis_template_is_expanded_and_attributed(self) -> None:
        executor = self.executor("hypothesis")

        batch = await executor(self.directive())

        self.assertEqual(len(batch.candidates), 3)
        self.assertEqual(len(batch.hypotheses), 1)
        self.assertIsNotNone(batch.llm_episode_id)
        hypothesis_id = batch.hypotheses[0]["hypothesis_id"]
        self.assertTrue(str(hypothesis_id).startswith("llm:"))
        attribution = dict(batch.hypothesis_attribution)
        self.assertEqual(len(attribution), 3)
        self.assertEqual(set(attribution.values()), {hypothesis_id})
        self.assertTrue(
            all(
                candidate.direct_evidence_prior == 1.0
                for candidate in batch.candidates
            )
        )

    async def test_oversized_response_fails_closed(self) -> None:
        executor = self.executor("oversize", max_response_bytes=128)

        with self.assertRaisesRegex(SearchAgentProtocolError, "max_response_bytes"):
            await executor(self.directive())

    async def test_nonzero_agent_exit_is_reported(self) -> None:
        executor = self.executor("fail")

        with self.assertRaisesRegex(RuntimeError, "rc=7"):
            await executor(self.directive())

    async def test_agent_timeout_is_hard_bounded(self) -> None:
        executor = self.executor(
            "sleep",
            timeout_seconds=0.05,
            termination_grace_seconds=0.05,
        )

        with self.assertRaisesRegex(TimeoutError, "exceeded"):
            await executor(self.directive())


if __name__ == "__main__":
    unittest.main()
