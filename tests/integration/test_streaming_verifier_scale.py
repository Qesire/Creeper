import tempfile
import tracemalloc
import unittest
from pathlib import Path
from decimal import Decimal

from creeper.evidence.policies import EvidenceCapsule
from creeper.submission.artifact_manifest import ArtifactSpec
from creeper.submission.snapshot import SubmissionSnapshot
from creeper.submission.streaming_exporter import build_streaming_submission_zip
from creeper.submission.verify import verify_submission_archive

from tests.integration.test_submission_verifier import _authority_fixture


_ROW_COUNT = 1_000_003


def _evidence_rows():
    for index in range(_ROW_COUNT):
        hostname = f"h{index:07d}.com"
        yield EvidenceCapsule(
            hostname=hostname,
            year=1997,
            provider="wayback",
            temporal_semantics="capture_timestamp_year",
            evidence_timestamp="19970101000000",
            source_locator=f"http://{hostname}/",
            payload_hash="a" * 64,
            policy_version="cdx-v1",
            evidence_type="exact_host_cdx_capture",
            source_id="wayback",
            original_url=f"http://{hostname}/",
            record_locator=f"wayback:{hostname}:1997:record={index}",
            extraction_method="cdx_query_year",
        )


def _snapshot(authority: dict[str, object]) -> SubmissionSnapshot:
    baseline_hashes = {
        name.removesuffix(".txt"): digest
        for name, digest in authority["annual_file_hashes"].items()
    }
    eed = str(_ROW_COUNT)
    return SubmissionSnapshot(
        submission_snapshot_id="million-row-verifier",
        created_at="2026-09-13T06:00:00+00:00",
        baseline_id=str(authority["baseline_id"]),
        baseline_hashes=baseline_hashes,
        normalizer_version="normalizer-v1",
        evidence_policy_version="evidence-v1",
        eed_policy_version="eed-v1",
        novel_records=(),
        novel_eed=eed,
        growth_rate=format(Decimal(_ROW_COUNT) / Decimal("10"), "f"),
        evidence_coverage="1",
        invalid_count=0,
        overlap_count=0,
        source_report_set=("production-source-report.json",),
        cdx_audit_set=("cdx-audit.json",),
        code_revision="e" * 64,
        eed_report={"equivalent_english_domains": eed},
        candidate_file_hash=str(authority["candidate_file_hash"]),
        model_hash=str(authority["model_hash"]),
        baseline_eed=str(authority["baseline_eed"]),
        authority_digest=str(authority["authority_digest"]),
        source_contribution={
            "by_source": {},
            "direct_annual": {"novel_host_years": 0, "novel_eed": "0"},
            "verified_candidate": {
                "novel_host_years": _ROW_COUNT,
                "novel_eed": eed,
            },
            "other_restricted": {"novel_host_years": 0, "novel_eed": "0"},
        },
    )


class StreamingVerifierScaleTests(unittest.TestCase):
    def test_multi_million_logical_rows_export_then_verify_with_bounded_memory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority, authority_path, index_path, model_path = _authority_fixture(root)
            source_root = root / "source"
            source_root.mkdir()
            (source_root / "README.md").write_text("fixture\n", encoding="utf-8")
            documentation = root / "methods.docx"
            documentation.write_bytes(b"docx fixture")
            config = root / "production.toml"
            config.write_text('source_mode = "activated"\n', encoding="utf-8")

            archive = build_streaming_submission_zip(
                _snapshot(authority),
                "million-row",
                root / "out",
                source_root=source_root,
                documentation_path=documentation,
                evidence_records=_evidence_rows(),
                artifact_specs=(
                    ArtifactSpec(
                        logical_role="production_config",
                        source_path=config,
                        archive_path="run/production.toml",
                    ),
                ),
                artifact_allowed_roots=(root,),
            )

            tracemalloc.start()
            try:
                report = verify_submission_archive(
                    archive,
                    baseline_manifest_path=authority_path,
                    baseline_index_path=index_path,
                    eed_model_path=model_path,
                )
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()

            self.assertTrue(report.ready, report.errors[:5])
            self.assertEqual(report.annual_records, _ROW_COUNT)
            self.assertEqual(report.evidence_records, _ROW_COUNT)
            self.assertEqual(report.recomputed_novel_eed, str(_ROW_COUNT))
            self.assertLess(
                peak,
                96 * 1024 * 1024,
                f"verifier peak grew with {_ROW_COUNT} logical rows: {peak} bytes",
            )


if __name__ == "__main__":
    unittest.main()
