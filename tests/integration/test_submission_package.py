import hashlib
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.identity import authority_digest
from creeper.evidence.policies import EvidenceCapsule
from creeper.submission.precheck import precheck_submission
from creeper.submission.snapshot import SubmissionSnapshot
from creeper.submission.exporter import build_submission_zip
from creeper.submission.verify import verify_submission_archive



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


    def test_built_direct_cdxj_submission_independently_verifies(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline_dir = root / "merged260912-3"
            baseline_dir.mkdir()
            annual_hashes: dict[str, str] = {}
            for year in range(1996, 2002):
                path = baseline_dir / f"{year}.txt"
                path.write_text("", encoding="utf-8")
                annual_hashes[f"{year}.txt"] = hashlib.sha256(
                    path.read_bytes()
                ).hexdigest()
            candidate_path = baseline_dir / "candidate_pool.txt"
            candidate_path.write_text("", encoding="utf-8")
            model_path = root / "eed-model.json"
            model_path.write_text(
                json.dumps(
                    {
                        "tld": ["com"],
                        "lang": ["eng"],
                        "perc_of_tld": [100],
                    }
                ),
                encoding="utf-8",
            )
            candidate_hash = hashlib.sha256(
                candidate_path.read_bytes()
            ).hexdigest()
            model_hash = hashlib.sha256(model_path.read_bytes()).hexdigest()
            baseline_eed = "10"
            authority = {
                "baseline_id": baseline_dir.name,
                "annual_file_hashes": annual_hashes,
                "candidate_file_hash": candidate_hash,
                "model_hash": model_hash,
                "baseline_eed": baseline_eed,
                "authority_digest": authority_digest(
                    baseline_id=baseline_dir.name,
                    annual_file_hashes=annual_hashes,
                    candidate_file_hash=candidate_hash,
                    model_hash=model_hash,
                    baseline_eed=baseline_eed,
                ),
            }
            authority_path = root / "authority.json"
            authority_path.write_text(json.dumps(authority), encoding="utf-8")
            index_path = root / "baseline.sqlite3"
            index = BaselineIndex.build(
                baseline_dir=baseline_dir,
                output_path=index_path,
                authority_manifest=authority,
            )
            index.close()

            capsule = EvidenceCapsule(
                hostname="direct.com",
                year=1998,
                provider="direct:arquivo-cdxj",
                temporal_semantics="archive_capture_timestamp",
                evidence_timestamp="19980203040506",
                source_locator=(
                    "https://arquivo.pt/datasets/cdxj/file.cdxj:byte:1"
                ),
                payload_hash="a" * 64,
                policy_version="evidence-v1",
                evidence_type="dated_archive_index",
                source_id="arquivo-cdxj",
                original_url="http://direct.com/page",
                record_locator=(
                    "https://arquivo.pt/datasets/cdxj/file.cdxj:byte:1"
                ),
                extraction_method=(
                    "CDX_CAPTURE;"
                    "contract=archive-cdxj-capture-v1@"
                    "archive-capture-contract-v1"
                ),
            )
            snapshot = SubmissionSnapshot(
                submission_snapshot_id="built-direct-v5",
                created_at="2026-09-13T00:00:00+00:00",
                baseline_id=baseline_dir.name,
                baseline_hashes={
                    name.removesuffix(".txt"): digest
                    for name, digest in annual_hashes.items()
                },
                normalizer_version="official-calculator-regex-v1",
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
                code_revision="f" * 64,
                eed_report={"equivalent_english_domains": "1"},
                candidate_file_hash=candidate_hash,
                model_hash=model_hash,
                baseline_eed=baseline_eed,
                authority_digest=authority["authority_digest"],
                source_contribution={
                    "by_source": {
                        "arquivo-cdxj": {
                            "novel_host_years": 1,
                            "novel_eed": "1",
                        }
                    },
                    "direct_annual": {
                        "novel_host_years": 1,
                        "novel_eed": "1",
                    },
                    "verified_candidate": {
                        "novel_host_years": 0,
                        "novel_eed": "0",
                    },
                    "other_restricted": {
                        "novel_host_years": 0,
                        "novel_eed": "0",
                    },
                },
            )
            source_root = root / "source"
            source_root.mkdir()
            (source_root / "README.md").write_text("fixture", encoding="utf-8")
            documentation = root / "methods.docx"
            documentation.write_bytes(b"docx fixture")

            archive = build_submission_zip(
                snapshot,
                "direct-v5",
                root / "out",
                source_root=source_root,
                documentation_path=documentation,
            )
            report = verify_submission_archive(
                archive,
                baseline_manifest_path=authority_path,
                baseline_index_path=index_path,
                eed_model_path=model_path,
            )

            self.assertTrue(report.ready, report.errors)
            self.assertEqual(report.recomputed_novel_eed, "1")
            self.assertEqual(report.recomputed_growth_rate, "0.1")

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
