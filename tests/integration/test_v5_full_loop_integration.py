import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.identity import authority_digest
from creeper.evidence.policies import EvidenceCapsule
from creeper.runtime.submission import export_runtime_submission
from creeper.storage.candidate_store import CandidateStore
from creeper.storage.evidence_store import EvidenceStore
from creeper.submission.artifact_manifest import ArtifactSpec
from creeper.submission.snapshot import SubmissionSnapshot


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class V5FullLoopIntegrationTests(unittest.TestCase):
    def test_runtime_formal_export_streams_store_and_independently_verifies(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline_dir = root / "baseline"
            baseline_dir.mkdir()
            annual_hashes = {}
            for year in range(1996, 2002):
                path = baseline_dir / f"{year}.txt"
                path.write_text("", encoding="utf-8")
                annual_hashes[f"{year}.txt"] = _sha256(path)
            candidate_pool = baseline_dir / "candidate_pool.txt"
            candidate_pool.write_text("", encoding="utf-8")
            model = root / "eed-model.json"
            model.write_text(
                json.dumps(
                    {
                        "tld": ["com"],
                        "lang": ["eng"],
                        "perc_of_tld": [100],
                    }
                ),
                encoding="utf-8",
            )
            baseline_eed = "10"
            authority = {
                "baseline_id": "v4-fixture",
                "annual_file_hashes": annual_hashes,
                "candidate_file_hash": _sha256(candidate_pool),
                "model_hash": _sha256(model),
                "baseline_eed": baseline_eed,
            }
            authority["authority_digest"] = authority_digest(
                baseline_id=authority["baseline_id"],
                annual_file_hashes=annual_hashes,
                candidate_file_hash=authority["candidate_file_hash"],
                model_hash=authority["model_hash"],
                baseline_eed=baseline_eed,
            )
            authority_path = root / "authority.json"
            authority_path.write_text(json.dumps(authority), encoding="utf-8")
            index_path = root / "baseline.sqlite3"
            baseline = BaselineIndex.build(
                baseline_dir=baseline_dir,
                output_path=index_path,
                authority_manifest=authority,
            )
            baseline.close()

            runtime = root / "runtime"
            evidence = EvidenceStore(runtime / "evidence.sqlite3")
            candidates = CandidateStore(runtime / "candidates.sqlite3")
            capsule = EvidenceCapsule(
                hostname="direct.com",
                year=1998,
                provider="direct:arquivo-cdxj",
                temporal_semantics="archive_capture_timestamp",
                evidence_timestamp="19980203040506",
                source_locator="https://arquivo.pt/index/file.cdxj:1",
                payload_hash="a" * 64,
                policy_version="evidence-v1",
                evidence_type="dated_archive_index",
                source_id="arquivo-cdxj",
                original_url="http://direct.com/page",
                record_locator="https://arquivo.pt/index/file.cdxj:1",
                extraction_method=(
                    "CDX_CAPTURE;"
                    "contract=archive-cdxj-capture-v1@"
                    "archive-capture-contract-v1"
                ),
            )
            evidence.put(capsule)

            source_root = root / "source"
            source_root.mkdir()
            (source_root / "README.md").write_text("fixture", encoding="utf-8")
            production_config = source_root / "production.toml"
            production_config.write_text(
                'source_mode = "activated"\n',
                encoding="utf-8",
            )
            documentation = root / "methods.docx"
            documentation.write_bytes(b"docx fixture")

            snapshot = SubmissionSnapshot(
                submission_snapshot_id="v5-runtime-stream",
                created_at="2026-09-13T00:00:00+00:00",
                baseline_id=authority["baseline_id"],
                baseline_hashes={
                    name.removesuffix(".txt"): digest
                    for name, digest in annual_hashes.items()
                },
                normalizer_version="official-calculator-regex-v1",
                evidence_policy_version="evidence-v1",
                eed_policy_version="eed-v1",
                # Formal runtime export deliberately does not materialize the
                # evidence store into this compatibility tuple.
                novel_records=(),
                novel_eed="1",
                growth_rate="0.1",
                evidence_coverage="1",
                invalid_count=0,
                overlap_count=0,
                source_report_set=("source-report.json",),
                cdx_audit_set=("cdx-audit.json",),
                code_revision="f" * 64,
                eed_report={"equivalent_english_domains": "1"},
                candidate_file_hash=authority["candidate_file_hash"],
                model_hash=authority["model_hash"],
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
            artifact = ArtifactSpec(
                logical_role="production_config",
                source_path=production_config,
                archive_path="run/production.toml",
            )

            archive, report = export_runtime_submission(
                snapshot=snapshot,
                evidence_store=evidence,
                candidate_store=candidates,
                baseline_manifest_path=authority_path,
                baseline_index_path=index_path,
                eed_model_path=model,
                name="integration",
                output_dir=root / "out",
                source_root=source_root,
                documentation_path=documentation,
                artifact_specs=(artifact,),
                artifact_allowed_roots=(source_root,),
            )

            self.assertTrue(archive.is_file())
            self.assertTrue(report.ready, report.errors)
            self.assertEqual(report.recomputed_novel_eed, "1")
            self.assertEqual(report.recomputed_growth_rate, "0.1")
            evidence.close()
            candidates.close()


if __name__ == "__main__":
    unittest.main()
