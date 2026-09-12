from __future__ import annotations

import json
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from creeper.source_discovery.admission import SearchAdmissionPolicy
from creeper.source_discovery.agent_search import CommandAgentSearchExecutor
from creeper.source_discovery.manager import SearchDirective, SearchDirectiveKind
from creeper.source_discovery.models import SourceCandidate, SourceLevel


class SearchAdmissionPolicyTests(unittest.TestCase):
    def candidate(self, **overrides) -> SourceCandidate:
        values = dict(
            canonical_entrypoint="https://archive.example/catalog/",
            source_family="RESOURCE_CATALOG",
            level=SourceLevel.METASOURCE,
            discovered_by="agent:test",
            discovery_strategy="META_SOURCE_SEARCH",
            expected_year_from=1996,
            expected_year_to=2001,
            expected_volume=200_000,
            enumerability_prior=0.9,
            confidence=0.8,
        )
        values.update(overrides)
        return SourceCandidate(**values)

    def test_accepts_large_enumerable_target_period_source(self) -> None:
        policy = SearchAdmissionPolicy(min_expected_volume=100_000)
        self.assertTrue(policy.accepts(self.candidate()))

    def test_rejects_low_volume_or_wrong_period_before_scout(self) -> None:
        policy = SearchAdmissionPolicy(min_expected_volume=100_000)
        self.assertIn("high-reservoir floor", policy.rejection_reason(self.candidate(expected_volume=9_999)) or "")
        self.assertIn(
            "do not overlap target",
            policy.rejection_reason(
                self.candidate(expected_year_from=2008, expected_year_to=2010)
            )
            or "",
        )


    def test_direct_evidence_bulk_uses_lower_volume_floor(self) -> None:
        policy = SearchAdmissionPolicy(
            min_expected_volume=100_000,
            direct_min_expected_volume=10_000,
        )
        direct = self.candidate(
            canonical_entrypoint="https://archive.example/index-2000.cdx.gz",
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            expected_volume=25_000,
            direct_evidence_prior=1.0,
        )
        self.assertTrue(policy.accepts(direct))
        self.assertIn(
            "direct-evidence floor=10000",
            policy.rejection_reason(
                self.candidate(
                    canonical_entrypoint="https://archive.example/index.cdxj",
                    expected_volume=9_999,
                )
            )
            or "",
        )

    def test_direct_evidence_index_may_self_describe_target_years(self) -> None:
        policy = SearchAdmissionPolicy(
            min_expected_volume=100_000,
            direct_min_expected_volume=10_000,
            require_year_bounds=True,
        )
        direct = self.candidate(
            canonical_entrypoint="https://archive.example/index.cdxj.gz",
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            expected_year_from=None,
            expected_year_to=None,
            expected_volume=25_000,
            direct_evidence_prior=1.0,
        )
        generic = self.candidate(
            canonical_entrypoint="https://archive.example/urls.txt.gz",
            expected_year_from=None,
            expected_year_to=None,
            expected_volume=200_000,
        )

        self.assertTrue(policy.accepts(direct))
        self.assertIn(
            "missing expected target-year bounds",
            policy.rejection_reason(generic) or "",
        )

    def test_rejects_common_crawl_corpus_before_scout(self) -> None:
        policy = SearchAdmissionPolicy(min_expected_volume=100_000)
        reason = policy.rejection_reason(
            self.candidate(
                canonical_entrypoint="https://index.commoncrawl.org/CC-MAIN-2001-01-index",
                source_family="COMMON_CRAWL_CORPUS",
            )
        )
        self.assertIn("Common Crawl corpus", reason or "")


_HELPER = r'''
import argparse
import json
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("--request", required=True)
p.add_argument("--response", required=True)
a = p.parse_args()
request = json.loads(Path(a.request).read_text(encoding="utf-8"))
assert request["admission"]["min_expected_volume"] == 100000
assert request["admission"]["direct_min_expected_volume"] == 10000
assert request["admission"]["direct_evidence_year_bounds_optional"] is True
assert request["requirements"]["prefer_direct_evidence_bulk"] is True
assert ".cdxj.gz" in request["requirements"]["direct_evidence_suffixes"]
assert request["requirements"]["resource_priority"][0].startswith("official archive")
assert "national libraries and web archives" in request["requirements"]["search_targets"]
assert request["requirements"]["avoid_low_yield"][0] == "ordinary archived pages"
payload = {
    "query": "large historical web collection",
    "candidates": [
        {
            "canonical_entrypoint": "https://small.example/list/",
            "source_family": "RESOURCE_CATALOG",
            "level": "METASOURCE",
            "expected_year_from": 1996,
            "expected_year_to": 2001,
            "expected_volume": 25000,
            "enumerability_prior": 0.9,
            "confidence": 0.8
        },
        {
            "canonical_entrypoint": "https://large.example/catalog/",
            "source_family": "RESOURCE_CATALOG",
            "level": "METASOURCE",
            "expected_year_from": 1996,
            "expected_year_to": 2001,
            "expected_volume": 500000,
            "enumerability_prior": 0.9,
            "confidence": 0.8
        }
    ]
}
Path(a.response).write_text(json.dumps(payload), encoding="utf-8")
'''


class AgentAdmissionIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_executor_filters_low_reservoir_and_persists_audit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            helper = root / "agent.py"
            helper.write_text(textwrap.dedent(_HELPER), encoding="utf-8")
            executor = CommandAgentSearchExecutor(
                (sys.executable, str(helper)),
                root / "invocations",
                backend="test-agent",
                actor="agent:test",
                admission_policy=SearchAdmissionPolicy(min_expected_volume=100_000),
            )
            directive = SearchDirective(
                kind=SearchDirectiveKind.REFILL_RESERVOIR,
                strategy="META_SOURCE_SEARCH",
                desired_candidates=5,
                subject=None,
                reason="refill cold reservoir",
            )

            batch = await executor(directive)

            self.assertEqual(len(batch.candidates), 1)
            self.assertEqual(
                batch.candidates[0].canonical_entrypoint,
                "https://large.example/catalog/",
            )
            invocation = next((root / "invocations").iterdir())
            audit = json.loads((invocation / "admission.json").read_text(encoding="utf-8"))
            self.assertEqual(audit["raw_candidate_count"], 2)
            self.assertEqual(audit["accepted_count"], 1)
            self.assertEqual(audit["rejected_count"], 1)
            self.assertIn("high-reservoir floor", audit["rejected"][0]["reason"])


if __name__ == "__main__":
    unittest.main()
