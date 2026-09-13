import hashlib
import json
import tempfile
import unittest
from pathlib import Path
import zipfile

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.identity import authority_digest
from creeper.evidence.policies import EvidenceCapsule
from creeper.storage.evidence_store import EvidenceStore
from creeper.submission.artifact_manifest import ArtifactSpec
from creeper.submission.precheck import precheck_submission
from creeper.submission.snapshot import SubmissionSnapshot
from creeper.submission.streaming_exporter import build_streaming_submission_zip
from creeper.submission_cli import _build_snapshot_from_readiness
from creeper.authority.identity import AuthoritySnapshot


def _authority(root: Path) -> tuple[dict[str, object], Path]:
    baseline_dir = root / "baseline"
    baseline_dir.mkdir()
    for year in range(1996, 2002):
        (baseline_dir / f"{year}.txt").write_text("", encoding="utf-8")
    (baseline_dir / "candidate_pool.txt").write_text("", encoding="utf-8")
    model = root / "model.json"
    model.write_text('{"tld": ["example"]}\n', encoding="utf-8")
    annual = {
        f"{year}.txt": hashlib.sha256(
            (baseline_dir / f"{year}.txt").read_bytes()
        ).hexdigest()
        for year in range(1996, 2002)
    }
    candidate_hash = hashlib.sha256(
        (baseline_dir / "candidate_pool.txt").read_bytes()
    ).hexdigest()
    model_hash = hashlib.sha256(model.read_bytes()).hexdigest()
    manifest = {
        "baseline_id": baseline_dir.name,
        "annual_file_hashes": annual,
        "candidate_file_hash": candidate_hash,
        "model_hash": model_hash,
        "baseline_eed": "10",
        "authority_digest": authority_digest(
            baseline_id=baseline_dir.name,
            annual_file_hashes=annual,
            candidate_file_hash=candidate_hash,
            model_hash=model_hash,
            baseline_eed="10",
        ),
    }
    return manifest, model


def _capsule(hostname: str, year: int) -> EvidenceCapsule:
    return EvidenceCapsule(
        hostname,
        year,
        "wayback",
        "capture_timestamp_year",
        f"{year}0101000000",
        f"http://{hostname}/",
        (hostname + str(year)).encode().hex().ljust(64, "0")[:64],
        "evidence-v1",
    )


def _snapshot(manifest: dict[str, object], *, frontier: int) -> SubmissionSnapshot:
    return SubmissionSnapshot(
        submission_snapshot_id="frontier-snapshot",
        created_at="2026-09-13T08:00:00+00:00",
        baseline_id=str(manifest["baseline_id"]),
        baseline_hashes={
            name.removesuffix(".txt"): digest
            for name, digest in dict(manifest["annual_file_hashes"]).items()
        },
        normalizer_version="normalizer-v1",
        evidence_policy_version="evidence-v1",
        eed_policy_version="eed-v1",
        novel_records=(),
        novel_eed="1",
        growth_rate="0.1",
        evidence_coverage="1",
        invalid_count=0,
        overlap_count=0,
        source_report_set=("source.json",),
        cdx_audit_set=("audit.json",),
        code_revision="c" * 64,
        candidate_file_hash=str(manifest["candidate_file_hash"]),
        model_hash=str(manifest["model_hash"]),
        baseline_eed="10",
        authority_digest=str(manifest["authority_digest"]),
        eed_report={"equivalent_english_domains": "1"},
        source_contribution={
            "by_source": {},
            "direct_annual": {"novel_host_years": 1, "novel_eed": "1"},
            "verified_candidate": {"novel_host_years": 0, "novel_eed": "0"},
            "other_restricted": {"novel_host_years": 0, "novel_eed": "0"},
        },
        observed_baseline_overlap=4,
        output_baseline_overlap=0,
        evidence_sequence_frontier=frontier,
        candidate_snapshot_id="candidate-checkpoint-1",
    )


class FormalFrontierAndOverlapTests(unittest.TestCase):
    def test_observed_overlap_does_not_gate_zero_output_overlap(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest, _ = _authority(Path(tmp))
            snapshot = _snapshot(manifest, frontier=0)
            report = precheck_submission(snapshot)
            self.assertTrue(report.ready, report.reasons)

    def test_old_readiness_baseline_overlap_is_audit_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, model = _authority(root)
            readiness = {
                "authority_digest": manifest["authority_digest"],
                "baseline_signature": manifest["authority_digest"],
                "model_signature": hashlib.sha256(model.read_bytes()).hexdigest(),
                "novel_eed": "1",
                "growth_rate": "0.1",
                "source_contribution": {
                    "by_source": {},
                    "direct_annual": {"novel_host_years": 1, "novel_eed": "1"},
                    "verified_candidate": {"novel_host_years": 0, "novel_eed": "0"},
                    "other_restricted": {"novel_host_years": 0, "novel_eed": "0"},
                },
                "baseline_reconciliation": {
                    "baseline_overlap": 7,
                    "invalid_records": 0,
                    "within_year_duplicates": 0,
                },
            }
            snapshot = _build_snapshot_from_readiness(
                readiness=readiness,
                authority=AuthoritySnapshot.from_manifest(manifest),
                eed_model_path=model,
                snapshot_id="old-report",
                code_revision="d" * 64,
                source_report_set=("source.json",),
                cdx_audit_set=("audit.json",),
                created_at="2026-09-13T08:00:00+00:00",
            )
            self.assertEqual(snapshot.observed_baseline_overlap, 7)
            self.assertEqual(snapshot.output_baseline_overlap, 0)
            self.assertTrue(precheck_submission(snapshot).ready)

    def test_frontier_excludes_evidence_written_after_checkpoint_from_package(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _ = _authority(root)
            evidence = EvidenceStore(root / "evidence.sqlite3")
            evidence.put(_capsule("before.example", 1997))
            frontier = evidence.max_host_year_sequence()
            evidence.put(_capsule("after.example", 1998))

            source_root = root / "source"
            source_root.mkdir()
            (source_root / "README.md").write_text("fixture\n", encoding="utf-8")
            documentation = root / "methods.docx"
            documentation.write_bytes(b"docx")
            config = root / "production.toml"
            config.write_text('source_mode = "activated"\n', encoding="utf-8")
            snapshot = _snapshot(manifest, frontier=frontier)
            try:
                archive = build_streaming_submission_zip(
                    snapshot,
                    "frontier",
                    root / "out",
                    source_root=source_root,
                    documentation_path=documentation,
                    evidence_records=evidence.iter_canonical_host_year_capsules(
                        max_sequence=frontier
                    ),
                    artifact_specs=(
                        ArtifactSpec(
                            logical_role="production_config",
                            source_path=config,
                            archive_path="run/production.toml",
                            allow_external=True,
                        ),
                    ),
                )
                with zipfile.ZipFile(archive) as bundle:
                    self.assertEqual(bundle.read("1997.txt"), b"before.example\n")
                    self.assertEqual(bundle.read("1998.txt"), b"")
                    manifest_payload = json.loads(bundle.read("MANIFEST.json"))
                    self.assertEqual(
                        manifest_payload["evidence_sequence_frontier"], frontier
                    )
            finally:
                evidence.close()


if __name__ == "__main__":
    unittest.main()
