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
        self.assertEqual(request["contract"], "creeper.search-agent.v1")
        self.assertEqual(request["desired_candidates"], 5)
        self.assertEqual(request["target_year_from"], 1996)
        self.assertEqual(request["target_year_to"], 2001)
        self.assertTrue(request["requirements"]["prefer_metasources"])

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
