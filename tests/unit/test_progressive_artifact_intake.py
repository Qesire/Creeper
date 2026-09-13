from __future__ import annotations

import unittest

from creeper.source_discovery.artifact_intake import (
    ArtifactIntakePolicy,
    assess_artifact_intake,
    classify_artifact,
)
from creeper.source_discovery.coordinator import TriageDisposition, TriageResult
from creeper.source_discovery.index_identity import HistoricalIndexObjectIdentity


class ArtifactIntakeTests(unittest.TestCase):
    def strong_remote_identity(self) -> HistoricalIndexObjectIdentity:
        return HistoricalIndexObjectIdentity(
            kind="remote",
            content_length=1000,
            etag='"stable"',
        )

    def test_strong_identity_and_range_do_not_make_gzip_random_accessible(self) -> None:
        result = assess_artifact_intake(
            TriageResult(
                TriageDisposition.SCOUT,
                status_code=206,
                content_type="application/gzip",
                content_length=1000,
                range_supported=True,
            ),
            url="https://archive.example/hosts.cdxj.gz",
            identity=self.strong_remote_identity(),
            contract_match="CDXJ",
        )

        self.assertTrue(result.access_ok)
        self.assertEqual(result.identity_strength, "STRONG")
        self.assertEqual(result.compression, "gzip")
        self.assertTrue(result.transport_range_supported)
        self.assertFalse(result.random_access_supported)
        self.assertEqual(result.format_kind, "CDXJ")
        self.assertEqual(result.admission, "WARM")

    def test_head_failure_fallback_result_keeps_bounded_access_facts(self) -> None:
        result = assess_artifact_intake(
            TriageResult(
                TriageDisposition.SCOUT,
                status_code=206,
                method="GET",
                content_type="text/plain",
                content_length=10,
                range_supported=True,
            ),
            url="https://archive.example/hosts.cdxj",
            identity=self.strong_remote_identity(),
            contract_match="CDXJ",
        )

        self.assertTrue(result.access_ok)
        self.assertEqual(result.format_kind, "CDXJ")
        self.assertEqual(result.admission, "WARM")

    def test_html_login_page_is_rejected_even_when_http_status_is_success(self) -> None:
        result = assess_artifact_intake(
            TriageResult(
                TriageDisposition.SCOUT,
                status_code=200,
                content_type="text/html; charset=utf-8",
                content_length=512,
                range_supported=False,
            ),
            url="https://archive.example/download",
            identity=None,
            contract_match="CDXJ",
            sample_preview=b"<html><title>Login</title>please sign in</html>",
        )

        self.assertFalse(result.access_ok)
        self.assertEqual(result.admission, "REJECT")
        self.assertIn("HTML/login", result.failure_reason or "")

    def test_unknown_contract_is_hold_not_reject(self) -> None:
        result = assess_artifact_intake(
            TriageResult(
                TriageDisposition.SCOUT,
                status_code=200,
                content_type="application/octet-stream",
                content_length=1000,
                range_supported=True,
            ),
            url="https://archive.example/blob.bin",
            identity=self.strong_remote_identity(),
            contract_match=None,
        )

        self.assertEqual(result.format_kind, "UNKNOWN")
        self.assertEqual(result.admission, "HOLD")
        self.assertEqual(result.failure_reason, "UNKNOWN_CONTRACT_FAMILY")

    def test_blank_contract_is_unknown_work_state(self) -> None:
        result = assess_artifact_intake(
            TriageResult(
                TriageDisposition.SCOUT,
                status_code=200,
                content_type="text/plain",
                content_length=1000,
                range_supported=False,
            ),
            url="https://archive.example/hosts.txt",
            identity=self.strong_remote_identity(),
            contract_match="   ",
        )

        self.assertEqual(result.admission, "HOLD")
        self.assertEqual(result.failure_reason, "UNKNOWN_CONTRACT_FAMILY")

    def test_sample_stats_are_scheduling_facts_not_formal_evidence(self) -> None:
        result = assess_artifact_intake(
            TriageResult(
                TriageDisposition.SCOUT,
                status_code=200,
                content_type="text/plain",
                content_length=100,
                range_supported=False,
            ),
            url="https://archive.example/hosts.txt",
            identity=self.strong_remote_identity(),
            contract_match="HOST_LIST",
            sample_stats={"novel_eed": 99.0},
        )

        self.assertEqual(result.admission, "WARM")
        self.assertEqual(result.sample_stats["novel_eed"], 99.0)
        self.assertFalse(result.sample_is_formal_evidence)

    def test_upstream_hold_remains_hold_work_state(self) -> None:
        result = assess_artifact_intake(
            TriageResult(
                TriageDisposition.HOLD,
                reason="HEAD entrypoint probe returned HTTP 404",
                status_code=404,
            ),
            url="https://archive.example/hosts.cdxj",
            identity=None,
            contract_match="CDXJ",
        )

        self.assertFalse(result.access_ok)
        self.assertEqual(result.admission, "HOLD")
        self.assertIn("not accessible", result.failure_reason or "")

    def test_sample_authority_flag_is_not_caller_settable(self) -> None:
        from creeper.source_discovery.artifact_intake import (
            ArtifactAdmission,
            ArtifactIntakeResult,
        )

        with self.assertRaises(TypeError):
            ArtifactIntakeResult(
                access_ok=True,
                identity_strength="STRONG",
                content_length=1,
                content_type="text/plain",
                compression=None,
                transport_range_supported=False,
                random_access_supported=False,
                format_kind="HOST_LIST",
                contract_match="HOST_LIST",
                sample_stats=None,
                failure_reason=None,
                admission=ArtifactAdmission.WARM,
                sample_is_formal_evidence=True,
            )

    def test_classification_distinguishes_warc_and_compressed_warc(self) -> None:
        self.assertEqual(classify_artifact("https://x.example/crawl.warc"), ("WARC", None))
        self.assertEqual(
            classify_artifact("https://x.example/crawl.warc.gz"),
            ("WARC", "gzip"),
        )


if __name__ == "__main__":
    unittest.main()
