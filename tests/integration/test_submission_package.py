import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from creeper.authority.identity import authority_digest
from creeper.evidence.policies import EvidenceCapsule
from creeper.submission.precheck import precheck_submission
from creeper.submission.snapshot import SubmissionSnapshot
from creeper.submission.exporter import build_submission_zip



def _authority_fields(
    baseline_id: str,
    baseline_hashes: dict[str, str],
    *,
    baseline_eed: str = "10",
) -> dict[str, str]:
    candidate = "c" * 64
    model = "d" * 64
    digest = authority_digest(
        baseline_id=baseline_id,
        annual_file_hashes={
            f"{year}.txt": value
            for year, value in baseline_hashes.items()
        },
        candidate_file_hash=candidate,
        model_hash=model,
        baseline_eed=baseline_eed,
    )
    return {
        "candidate_file_hash": candidate,
        "model_hash": model,
        "baseline_eed": baseline_eed,
        "authority_digest": digest,
    }


class SubmissionPackageTests(unittest.TestCase):
    def test_ready_snapshot_exports_all_required_v4_assets(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline_id = "merged260912-3"
            baseline_hashes = {
                str(year): "b" * 64 for year in range(1996, 2002)
            }
            authority = _authority_fields(
                baseline_id,
                baseline_hashes,
                baseline_eed="10",
            )
            documentation = root / "methods.docx"
            documentation.write_bytes(b"docx fixture")
            capsule = EvidenceCapsule(
                "new.example", 1997, "wayback-cdx", "capture_timestamp_year",
                "19970101000000", "http://new.example/", "a" * 64, "cdx-v1"
            )
            snapshot = SubmissionSnapshot(
                submission_snapshot_id="s-1",
                created_at="2026-09-09T00:00:00+00:00",
                baseline_id=baseline_id,
                baseline_hashes=baseline_hashes,
                normalizer_version="official-calculator-regex-v1",
                evidence_policy_version="evidence-v1",
                eed_policy_version="eed-v1",
                novel_records=(capsule,),
                novel_eed="0.5000",
                growth_rate="0.050000",
                evidence_coverage="1.000000",
                invalid_count=0,
                overlap_count=0,
                source_report_set=("source-report.json",),
                cdx_audit_set=("cdx-audit.json",),
                code_revision="c" * 64,
                eed_report={"equivalent_english_domains": "0.5000"},
                **authority,
            )
            report = precheck_submission(snapshot)
            self.assertTrue(report.ready, report.reasons)
            archive = build_submission_zip(
                snapshot,
                "tester",
                root / "out",
                source_root=Path(__file__).parents[2],
                documentation_path=documentation,
            )
            with zipfile.ZipFile(archive) as bundle:
                names = set(bundle.namelist())
                for year in range(1996, 2002):
                    self.assertIn(f"{year}.txt", names)
                self.assertIn("active_candidates.txt", names)
                self.assertIn("isc_reference/manifest.json", names)
                self.assertIn("evidence.jsonl", names)
                self.assertIn("MANIFEST.json", names)
                manifest = json.loads(bundle.read("MANIFEST.json"))
                self.assertEqual(manifest["baseline_id"], "merged260912-3")
                self.assertEqual(manifest["policy_versions"]["eed"], "eed-v1")
                self.assertTrue(all(manifest["entry_sha256"].values()))
                self.assertIn("code/pyproject.toml", names)
                self.assertIn("code/src/creeper/cli.py", names)
                self.assertIn("documentation/methods.docx", names)
                self.assertFalse(any(name.startswith("code/.venv/") for name in names))
                self.assertFalse(any(name.endswith(".egg-info") for name in names))

    def test_precheck_rejects_incomplete_queries_and_common_crawl_scope(self):
        baseline_hashes = {
            str(year): "b" * 64 for year in range(1996, 2002)
        }
        authority = _authority_fields(
            "merged260912-3",
            baseline_hashes,
            baseline_eed="10",
        )
        snapshot = SubmissionSnapshot(
            submission_snapshot_id="s-2",
            created_at="2026-09-09T00:00:00+00:00",
            baseline_id="merged260912-3",
            baseline_hashes=baseline_hashes,
            normalizer_version="normalizer-v1",
            evidence_policy_version="evidence-v1",
            eed_policy_version="eed-v1",
            novel_records=(),
            novel_eed="0",
            growth_rate="0",
            evidence_coverage="1",
            invalid_count=0,
            overlap_count=0,
            source_report_set=("source.json",),
            cdx_audit_set=("audit.json",),
            code_revision="c" * 64,
            eed_report={"equivalent_english_domains": "0"},
            active_candidates=("common-crawl.example",),
            active_candidate_scopes=("common_crawl_corpus_excluded",),
            incomplete_query_count=1,
            **authority,
        )
        report = precheck_submission(snapshot)
        self.assertFalse(report.ready)
        self.assertIn("incomplete queries cannot be submitted as negative evidence", report.reasons)
        self.assertIn("Common Crawl candidate is present in active candidates", report.reasons)
        self.assertIn("formal submission requires at least 5% EED growth", report.reasons)

    def test_precheck_accepts_dynamic_baseline_identity_and_rejects_eed_mismatch(self):
        baseline_hashes = {
            str(year): "c" * 64 for year in range(1996, 2002)
        }
        authority = _authority_fields(
            "merged-next-round",
            baseline_hashes,
            baseline_eed="200",
        )
        snapshot = SubmissionSnapshot(
            submission_snapshot_id="s-3",
            created_at="2026-09-10T00:00:00+00:00",
            baseline_id="merged-next-round",
            baseline_hashes=baseline_hashes,
            normalizer_version="normalizer-v1",
            evidence_policy_version="evidence-v1",
            eed_policy_version="eed-v1",
            novel_records=(),
            novel_eed="10",
            growth_rate="0.05",
            evidence_coverage="1",
            invalid_count=0,
            overlap_count=0,
            source_report_set=("source.json",),
            cdx_audit_set=("audit.json",),
            code_revision="d" * 64,
            eed_report={"equivalent_english_domains": "9"},
            **authority,
        )
        report = precheck_submission(snapshot)
        self.assertFalse(report.ready)
        self.assertNotIn("baseline_id must be merged260909-3", report.reasons)
        self.assertIn("novel_eed must match the exact EED report", report.reasons)


    def test_precheck_rejects_capture_timestamp_year_mismatch(self):
        baseline_hashes = {
            str(year): "b" * 64 for year in range(1996, 2002)
        }
        authority = _authority_fields(
            "merged260912-3",
            baseline_hashes,
            baseline_eed="10",
        )
        capsule = EvidenceCapsule(
            "new.example",
            1997,
            "wayback",
            "capture_timestamp_year",
            "20000101000000",
            "http://new.example/",
            "a" * 64,
            "cdx-v1",
            evidence_type="exact_host_cdx_capture",
        )
        snapshot = SubmissionSnapshot(
            submission_snapshot_id="semantic-year-mismatch",
            created_at="2026-09-13T00:00:00+00:00",
            baseline_id="merged260912-3",
            baseline_hashes=baseline_hashes,
            normalizer_version="normalizer-v1",
            evidence_policy_version="evidence-v1",
            eed_policy_version="eed-v1",
            novel_records=(capsule,),
            novel_eed="1",
            growth_rate="0.1",
            evidence_coverage="1",
            invalid_count=0,
            overlap_count=0,
            source_report_set=("source.json",),
            cdx_audit_set=("audit.json",),
            code_revision="e" * 64,
            eed_report={"equivalent_english_domains": "1"},
            **authority,
        )

        report = precheck_submission(snapshot)

        self.assertFalse(report.ready)
        self.assertTrue(
            any(
                "evidence timestamp year does not match capsule target year"
                in reason
                for reason in report.reasons
            )
        )

    def test_precheck_rejects_contradictory_growth_rate(self):
        baseline_hashes = {
            str(year): "b" * 64 for year in range(1996, 2002)
        }
        authority = _authority_fields(
            "merged260912-3",
            baseline_hashes,
            baseline_eed="10",
        )
        snapshot = SubmissionSnapshot(
            submission_snapshot_id="growth-mismatch",
            created_at="2026-09-13T00:00:00+00:00",
            baseline_id="merged260912-3",
            baseline_hashes=baseline_hashes,
            normalizer_version="normalizer-v1",
            evidence_policy_version="evidence-v1",
            eed_policy_version="eed-v1",
            novel_records=(),
            novel_eed="1",
            growth_rate="0.99",
            evidence_coverage="1",
            invalid_count=0,
            overlap_count=0,
            source_report_set=("source.json",),
            cdx_audit_set=("audit.json",),
            code_revision="f" * 64,
            eed_report={"equivalent_english_domains": "1"},
            **authority,
        )

        report = precheck_submission(snapshot)

        self.assertFalse(report.ready)
        self.assertIn(
            "growth_rate must equal novel_eed / baseline_eed exactly",
            report.reasons,
        )

if __name__ == "__main__":
    unittest.main()
