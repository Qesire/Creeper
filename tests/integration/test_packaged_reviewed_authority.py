import hashlib
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.identity import authority_digest
from creeper.evidence.classification import AcquisitionLane, classify_acquisition_lane
from creeper.evidence.contract_registry import (
    ReviewedArtifactBinding,
    ReviewedArtifactIdentity,
    ReviewedContractRegistry,
    ReviewedContractRegistryError,
    ReviewedSourceContractBinding,
)
from creeper.evidence.contracts import EvidenceAuthority, SourceEvidenceContract
from creeper.evidence.policies import EvidenceCapsule
from creeper.submission.artifact_manifest import ArtifactSpec
from creeper.submission.snapshot import SubmissionSnapshot
from creeper.submission.streaming_exporter import build_streaming_submission_zip
from creeper.submission.verify import (
    recompute_submission_archive,
    verify_submission_archive,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _reviewed_registry(locator: str) -> ReviewedContractRegistry:
    contract = SourceEvidenceContract(
        contract_id="reviewed-jsonl-v1",
        authority=EvidenceAuthority.DIRECT_WEB_YEAR,
        parser_kind="jsonl",
        temporal_semantics="capture_timestamp_year",
        evidence_type="reviewed_jsonl_capture",
        hostname_field="hostname",
        timestamp_field="timestamp",
        policy_version="reviewed-jsonl-contract-v1",
    )
    binding = ReviewedSourceContractBinding(
        artifact=ReviewedArtifactBinding(
            locator=locator,
            source_identity=ReviewedArtifactIdentity(
                kind="sha256",
                value="a" * 64,
            ),
            custodian="fixture-custodian",
            edition="2026-09-13",
        ),
        contract=contract,
        review_note="reviewed fixture JSONL authority",
    )
    return ReviewedContractRegistry({locator: binding})


def _capsule(locator: str) -> EvidenceCapsule:
    return EvidenceCapsule(
        hostname="reviewed.com",
        year=1998,
        provider="direct:reviewed-jsonl",
        temporal_semantics="capture_timestamp_year",
        evidence_timestamp="19980203040506",
        source_locator=locator,
        payload_hash="b" * 64,
        policy_version="reviewed-jsonl-contract-v1",
        evidence_type="reviewed_jsonl_capture",
        source_id="reviewed-jsonl",
        original_url="https://reviewed.com/page",
        record_locator=f"{locator}#record-1",
        extraction_method=(
            "reviewed_jsonl;"
            "contract=reviewed-jsonl-v1@reviewed-jsonl-contract-v1"
        ),
    )


def _snapshot(authority: dict[str, object]) -> SubmissionSnapshot:
    baseline_hashes = {
        name.removesuffix(".txt"): digest
        for name, digest in authority["annual_file_hashes"].items()
    }
    return SubmissionSnapshot(
        submission_snapshot_id="packaged-authority-v1",
        created_at="2026-09-13T06:00:00+00:00",
        baseline_id=str(authority["baseline_id"]),
        baseline_hashes=baseline_hashes,
        normalizer_version="normalizer-v1",
        evidence_policy_version="evidence-v1",
        eed_policy_version="eed-v1",
        novel_records=(),
        novel_eed="1",
        growth_rate="0.1",
        evidence_coverage="1",
        invalid_count=0,
        overlap_count=0,
        source_report_set=("source-report.json",),
        cdx_audit_set=("cdx-audit.json",),
        code_revision="c" * 64,
        eed_report={"equivalent_english_domains": "1"},
        candidate_file_hash=str(authority["candidate_file_hash"]),
        model_hash=str(authority["model_hash"]),
        baseline_eed=str(authority["baseline_eed"]),
        authority_digest=str(authority["authority_digest"]),
        source_contribution={
            "direct_annual": {"novel_host_years": 1, "novel_eed": "1"},
            "verified_candidate": {"novel_host_years": 0, "novel_eed": "0"},
            "other_restricted": {"novel_host_years": 0, "novel_eed": "0"},
        },
    )


def _authority_fixture(root: Path) -> tuple[dict[str, object], Path, Path, Path]:
    baseline_dir = root / "baseline"
    baseline_dir.mkdir()
    for year in range(1996, 2002):
        (baseline_dir / f"{year}.txt").write_text("", encoding="utf-8")
    candidate = baseline_dir / "candidate_pool.txt"
    candidate.write_text("", encoding="utf-8")
    model = root / "eed-model.json"
    model.write_text(
        json.dumps({"tld": ["com"], "lang": ["eng"], "perc_of_tld": [100]}),
        encoding="utf-8",
    )
    annual = {
        f"{year}.txt": _sha256(baseline_dir / f"{year}.txt")
        for year in range(1996, 2002)
    }
    authority = {
        "baseline_id": baseline_dir.name,
        "annual_file_hashes": annual,
        "candidate_file_hash": _sha256(candidate),
        "model_hash": _sha256(model),
        "baseline_eed": "10",
    }
    authority["authority_digest"] = authority_digest(**authority)
    authority_path = root / "authority.json"
    authority_path.write_text(json.dumps(authority), encoding="utf-8")
    index_path = root / "baseline.sqlite3"
    index = BaselineIndex.build(
        baseline_dir=baseline_dir,
        output_path=index_path,
        authority_manifest=authority,
    )
    index.close()
    return authority, authority_path, index_path, model


class PackagedReviewedAuthorityTests(unittest.TestCase):
    def test_registry_manifest_is_deterministic_and_strict(self):
        locator = "https://trusted.example/history/records.jsonl"
        registry = _reviewed_registry(locator)

        first = registry.to_manifest_payload()
        second = registry.to_manifest_payload()

        self.assertEqual(first, second)
        rebuilt = ReviewedContractRegistry.from_manifest_payload(first)
        self.assertEqual(dict(rebuilt), dict(registry))
        tampered = json.loads(json.dumps(first))
        tampered["entries"][0]["review_note"] = "changed"
        with self.assertRaises(ReviewedContractRegistryError):
            ReviewedContractRegistry.from_manifest_payload(tampered)

    def test_external_direct_contract_requires_packaged_registry(self):
        locator = "https://trusted.example/history/records.jsonl"
        capsule = _capsule(locator)

        self.assertEqual(classify_acquisition_lane(capsule), AcquisitionLane.UNKNOWN)
        self.assertEqual(
            classify_acquisition_lane(
                capsule,
                reviewed_contracts=_reviewed_registry(locator),
            ),
            AcquisitionLane.DIRECT_ANNUAL,
        )

    def test_archive_rebuilds_reviewed_authority_without_runtime_registry(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority, authority_path, index_path, model_path = _authority_fixture(root)
            source_root = root / "source"
            source_root.mkdir()
            (source_root / "README.md").write_text("fixture\n", encoding="utf-8")
            documentation = root / "methods.docx"
            documentation.write_bytes(b"docx")
            config = root / "production.toml"
            config.write_text('source_mode = "activated"\n', encoding="utf-8")
            report = root / "source-report.json"
            report.write_text("{}\n", encoding="utf-8")
            runtime_registry = root / "runtime-registry.json"
            registry = _reviewed_registry(
                "https://trusted.example/history/records.jsonl"
            )
            runtime_registry.write_text(
                json.dumps(registry.to_manifest_payload()),
                encoding="utf-8",
            )

            archive = build_streaming_submission_zip(
                _snapshot(authority),
                "packaged-authority",
                root / "out",
                source_root=source_root,
                documentation_path=documentation,
                evidence_records=iter((_capsule(registry["https://trusted.example/history/records.jsonl"].locator),)),
                artifact_specs=(
                    ArtifactSpec(
                        logical_role="production_config",
                        source_path=config,
                        archive_path="run/production.toml",
                    ),
                    ArtifactSpec(
                        logical_role="source_report",
                        source_path=report,
                        archive_path="artifacts/source-report.json",
                    ),
                ),
                artifact_allowed_roots=(root,),
                reviewed_contract_registry=registry,
            )
            runtime_registry.unlink()

            with zipfile.ZipFile(archive) as bundle:
                self.assertIn(
                    "artifacts/runtime/reviewed_contract_registry.json",
                    bundle.namelist(),
                )
                manifest = json.loads(bundle.read("MANIFEST.json"))
                rows = manifest["artifacts"]
                self.assertTrue(
                    any(
                        row["archive_path"]
                        == "artifacts/runtime/reviewed_contract_registry.json"
                        for row in rows
                    )
                )

            report = verify_submission_archive(
                archive,
                baseline_manifest_path=authority_path,
                baseline_index_path=index_path,
                eed_model_path=model_path,
            )
            self.assertTrue(report.ready, report.errors)
            recomputed = recompute_submission_archive(
                archive,
                baseline_manifest_path=authority_path,
                baseline_index_path=index_path,
                eed_model_path=model_path,
            )
            self.assertTrue(recomputed.ready, recomputed.errors)

    def test_modified_packaged_identity_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority, authority_path, index_path, model_path = _authority_fixture(root)
            source_root = root / "source"
            source_root.mkdir()
            (source_root / "README.md").write_text("fixture\n", encoding="utf-8")
            documentation = root / "methods.docx"
            documentation.write_bytes(b"docx")
            config = root / "production.toml"
            config.write_text('source_mode = "activated"\n', encoding="utf-8")
            report = root / "source-report.json"
            report.write_text("{}\n", encoding="utf-8")
            registry = _reviewed_registry(
                "https://trusted.example/history/records.jsonl"
            )
            archive = build_streaming_submission_zip(
                _snapshot(authority),
                "tampered-authority",
                root / "out",
                source_root=source_root,
                documentation_path=documentation,
                evidence_records=iter((_capsule(registry["https://trusted.example/history/records.jsonl"].locator),)),
                artifact_specs=(
                    ArtifactSpec("production_config", config, "run/production.toml"),
                    ArtifactSpec("source_report", report, "artifacts/source-report.json"),
                ),
                artifact_allowed_roots=(root,),
                reviewed_contract_registry=registry,
            )
            tampered = root / "tampered.zip"
            registry_name = "artifacts/runtime/reviewed_contract_registry.json"
            with zipfile.ZipFile(archive) as source, zipfile.ZipFile(tampered, "w") as target:
                for info in source.infolist():
                    data = source.read(info.filename)
                    if info.filename == registry_name:
                        payload = json.loads(data)
                        payload["entries"][0]["source_identity"]["value"] = "c" * 64
                        data = json.dumps(payload, indent=2, sort_keys=True).encode()
                    target.writestr(info, data)

            result = verify_submission_archive(
                tampered,
                baseline_manifest_path=authority_path,
                baseline_index_path=index_path,
                eed_model_path=model_path,
            )
            self.assertFalse(result.ready)
            self.assertTrue(
                any(
                    "reviewed contract registry" in error
                    or "registry artifact hash mismatch" in error
                    for error in result.errors
                ),
                result.errors,
            )


if __name__ == "__main__":
    unittest.main()
