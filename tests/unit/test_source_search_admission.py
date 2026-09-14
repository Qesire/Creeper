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
        self.assertIn("gateway floor", policy.rejection_reason(self.candidate(expected_volume=9_999)) or "")
        self.assertIn(
            "do not overlap target",
            policy.rejection_reason(
                self.candidate(expected_year_from=2008, expected_year_to=2010)
            )
            or "",
        )


    def test_gateway_uses_smaller_but_stricter_role_aware_floor(self) -> None:
        policy = SearchAdmissionPolicy(
            min_expected_volume=100_000,
            gateway_min_expected_volume=50_000,
            gateway_min_enumerability_prior=0.8,
        )
        gateway = self.candidate(
            expected_volume=80_000,
            enumerability_prior=0.9,
        )
        source = self.candidate(
            level=SourceLevel.SOURCE,
            expected_volume=80_000,
            enumerability_prior=0.9,
        )
        weak_gateway = self.candidate(
            expected_volume=80_000,
            enumerability_prior=0.7,
        )

        self.assertTrue(policy.accepts(gateway))
        self.assertIn("source floor=100000", policy.rejection_reason(source) or "")
        self.assertIn(
            "gateway floor=0.8",
            policy.rejection_reason(weak_gateway) or "",
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


class SearchProfileMechanismTests(unittest.TestCase):
    def test_recovery_profile_avoids_crawl_centric_queries(self) -> None:
        directive = SearchDirective(
            kind=SearchDirectiveKind.RECOVER_STAGNATION,
            strategy="RECOVER_STAGNATION",
            desired_candidates=5,
            subject=None,
            reason="recent search tail has zero credited reward",
        )

        profile = CommandAgentSearchExecutor._calibrated_search_profile(directive)
        query_text = " ".join(profile["query_examples"]).lower()

        self.assertEqual(profile["mode"], "recovery")
        self.assertIn("capture mechanism", profile["primary_archetype"])
        self.assertIn("proxy", query_text)
        self.assertIn("dns hostcount", query_text)
        self.assertIn("server survey", query_text)
        self.assertIn("open directory", query_text)
        self.assertNotIn("crawl", query_text)
        self.assertNotIn("warc", query_text)
        self.assertNotIn("cdx", query_text)


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

            invocation = next((root / "invocations").iterdir())
            request = json.loads((invocation / "request.json").read_text(encoding="utf-8"))
            self.assertEqual(request["admission"]["min_expected_volume"], 100000)
            self.assertEqual(
                request["admission"]["gateway_min_expected_volume"],
                50000,
            )
            self.assertEqual(request["admission"]["direct_min_expected_volume"], 10000)
            self.assertEqual(
                request["admission"]["gateway_min_enumerability_prior"],
                0.8,
            )
            self.assertIn(
                "page-graph vertex count",
                request["admission"]["volume_semantics"]["SOURCE"],
            )
            self.assertIs(request["admission"]["direct_evidence_year_bounds_optional"], True)
            self.assertIs(request["requirements"]["prefer_direct_evidence_bulk"], False)
            self.assertIn(".cdxj.gz", request["requirements"]["direct_evidence_suffixes"])
            self.assertTrue(
                request["requirements"]["resource_priority"][0].startswith(
                    "non-snapshot hostname inventories"
                )
            )
            mechanism = request["requirements"]["capture_mechanism_policy"]
            self.assertIn(
                "active HTTP/web-server survey",
                mechanism["preferred_non_snapshot_mechanisms"],
            )
            self.assertIn(
                "passive HTTP proxy/cache/request log",
                mechanism["preferred_non_snapshot_mechanisms"],
            )
            self.assertIn(
                "DNS hostcount/zone/connected-host enumeration",
                mechanism["preferred_non_snapshot_mechanisms"],
            )
            self.assertIn(
                "historical proxy/cache trace repositories",
                request["requirements"]["search_targets"],
            )
            self.assertIn(
                "ordinary archived pages and single-site snapshots",
                request["requirements"]["avoid_low_yield"],
            )
            self.assertTrue(
                any(
                    "privacy-sanitized traces" in item
                    for item in request["requirements"]["avoid_low_yield"]
                )
            )
            self.assertIn(
                "capture mechanism",
                request["requirements"]["query_construction"][
                    "must_name_capture_mechanism"
                ],
            )
            self.assertEqual(
                request["requirements"]["calibrated_search_profile"]["mode"],
                "high_density_refill",
            )

            self.assertEqual(len(batch.candidates), 1)
            self.assertEqual(
                batch.candidates[0].canonical_entrypoint,
                "https://large.example/catalog/",
            )
            audit = json.loads((invocation / "admission.json").read_text(encoding="utf-8"))
            self.assertEqual(audit["raw_candidate_count"], 2)
            self.assertEqual(audit["accepted_count"], 1)
            self.assertEqual(audit["rejected_count"], 1)
            self.assertIn("gateway floor", audit["rejected"][0]["reason"])


if __name__ == "__main__":
    unittest.main()
