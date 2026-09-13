import hashlib
import json
import tempfile
import tracemalloc
import unittest
import zipfile
from pathlib import Path

from creeper.authority.identity import authority_digest
from creeper.evidence.policies import EvidenceCapsule
from creeper.records.candidates import CandidateRecord, CandidateSourceScope
from creeper.storage.candidate_store import CandidateStore
from creeper.storage.evidence_store import EvidenceStore
from creeper.submission.artifact_manifest import (
    ArtifactSpec,
    resolve_artifacts,
    verify_resolved_artifact,
)
from creeper.submission.snapshot import SubmissionSnapshot
from creeper.submission.streaming_exporter import build_streaming_submission_zip


def _snapshot(*, created_at: str = "2026-09-13T06:00:00+00:00") -> SubmissionSnapshot:
    baseline_id = "merged-v5-test"
    baseline_hashes = {str(year): "b" * 64 for year in range(1996, 2002)}
    candidate = "c" * 64
    model = "d" * 64
    baseline_eed = "10"
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
    return SubmissionSnapshot(
        submission_snapshot_id="stream-v5",
        created_at=created_at,
        baseline_id=baseline_id,
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
        source_report_set=("production-source-report.json",),
        cdx_audit_set=("cdx-audit.json",),
        code_revision="e" * 64,
        eed_report={"equivalent_english_domains": "1"},
        candidate_file_hash=candidate,
        model_hash=model,
        baseline_eed=baseline_eed,
        authority_digest=digest,
    )


def _fixture_files(root: Path):
    source_root = root / "source"
    source_root.mkdir()
    (source_root / "README.md").write_text("fixture\n", encoding="utf-8")
    documentation = root / "methods.docx"
    documentation.write_bytes(b"docx fixture")
    config = root / "production.toml"
    config.write_text('source_mode = "activated"\n', encoding="utf-8")
    report = root / "source-report.json"
    report.write_text('{"ready":true}\n', encoding="utf-8")
    artifacts = (
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
    )
    return source_root, documentation, artifacts


class StreamingSubmissionExportTests(unittest.TestCase):
    def test_streaming_bundle_uses_store_iterators_and_closes_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_root, documentation, artifacts = _fixture_files(root)
            evidence = EvidenceStore(root / "evidence.sqlite3")
            evidence.put_many([
                EvidenceCapsule(
                    "alpha.example",
                    1997,
                    "wayback",
                    "capture_timestamp_year",
                    "19970101000000",
                    "http://alpha.example/",
                    "a" * 64,
                    "cdx-v1",
                    "exact_host_cdx_capture",
                    "wayback",
                    "http://alpha.example/",
                    "fixture:alpha",
                    "cdx_query_year",
                ),
                EvidenceCapsule(
                    "zeta.example",
                    2001,
                    "arquivo",
                    "capture_timestamp_year",
                    "20010101000000",
                    "http://zeta.example/",
                    "b" * 64,
                    "arquivo-v1",
                    "source_direct_year",
                    "arquivo",
                    "http://zeta.example/",
                    "fixture:zeta",
                    "direct_cdxj",
                ),
            ])
            # Formal streaming export must never fall back to the full-list API.
            evidence.canonical_host_year_capsules = lambda: (_ for _ in ()).throw(
                AssertionError("full-list evidence API was called")
            )

            candidates = CandidateStore(root / "candidates.sqlite3")
            candidates.record_observations([
                CandidateRecord(
                    "candidate.example",
                    "local-fixture",
                    CandidateSourceScope.LOCAL_DISCOVERY,
                    "fixture://candidate",
                    1998,
                ),
                CandidateRecord(
                    "isc.example",
                    "isc-reference",
                    CandidateSourceScope.LOCAL_DISCOVERY,
                    "fixture://isc",
                    1997,
                ),
                CandidateRecord(
                    "not a hostname",
                    "broken-fixture",
                    CandidateSourceScope.LOCAL_DISCOVERY,
                    "fixture://broken",
                    None,
                ),
            ])

            archive = build_streaming_submission_zip(
                _snapshot(),
                "stream-test",
                root / "out",
                source_root=source_root,
                documentation_path=documentation,
                evidence_records=evidence.iter_canonical_host_year_capsules(batch_size=1),
                candidate_store=candidates,
                artifact_specs=artifacts,
                artifact_allowed_roots=(root,),
            )

            with zipfile.ZipFile(archive) as bundle:
                names = set(bundle.namelist())
                self.assertIn("candidate_ledger/active.jsonl", names)
                self.assertIn("candidate_ledger/unparsed.jsonl", names)
                self.assertIn("candidate_ledger/isc_reference.jsonl", names)
                self.assertIn("run/production.toml", names)
                self.assertIn("artifacts/source-report.json", names)
                self.assertEqual(
                    bundle.read("active_candidates.txt"),
                    b"candidate.example\n",
                )
                self.assertEqual(
                    bundle.read("1997.txt"),
                    b"alpha.example\n",
                )
                self.assertEqual(
                    bundle.read("2001.txt"),
                    b"zeta.example\n",
                )
                manifest = json.loads(bundle.read("MANIFEST.json"))
                self.assertEqual(manifest["exporter"], "streaming-v1")
                self.assertEqual(manifest["novel_records"], 2)
                self.assertEqual(manifest["active_candidates"], 1)
                self.assertEqual(
                    {row["logical_role"] for row in manifest["artifacts"]},
                    {"production_config", "source_report"},
                )
                for name, expected in manifest["entry_sha256"].items():
                    self.assertEqual(
                        hashlib.sha256(bundle.read(name)).hexdigest(),
                        expected,
                    )

            candidates.close()
            evidence.close()

    def test_missing_required_artifact_and_manifest_race_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            missing = ArtifactSpec(
                logical_role="production_config",
                source_path=root / "missing.toml",
                archive_path="run/production.toml",
            )
            with self.assertRaises(FileNotFoundError):
                resolve_artifacts((missing,), allowed_roots=(root,))

            config = root / "production.toml"
            config.write_text("a=1\n", encoding="utf-8")
            resolved = resolve_artifacts(
                (
                    ArtifactSpec(
                        logical_role="production_config",
                        source_path=config,
                        archive_path="run/production.toml",
                    ),
                ),
                allowed_roots=(root,),
            )
            config.write_text("b=2\n", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "changed after manifesting"):
                verify_resolved_artifact(resolved[0])

    def test_large_synthetic_export_has_bounded_python_memory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_root, documentation, artifacts = _fixture_files(root)

            def records():
                for index in range(20_000):
                    hostname = f"h{index:06d}.example"
                    yield EvidenceCapsule(
                        hostname,
                        1999,
                        "fixture",
                        "capture_timestamp_year",
                        "19990101000000",
                        f"http://{hostname}/",
                        f"{index:064x}"[-64:],
                        "fixture-v1",
                        "exact_host_cdx_capture",
                        "fixture",
                        f"http://{hostname}/",
                        f"fixture:{index}",
                        "synthetic",
                    )

            tracemalloc.start()
            archive = build_streaming_submission_zip(
                _snapshot(created_at="2026-09-13T06:01:00+00:00"),
                "memory-test",
                root / "out",
                source_root=source_root,
                documentation_path=documentation,
                evidence_records=records(),
                artifact_specs=artifacts,
                artifact_allowed_roots=(root,),
            )
            _current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            self.assertTrue(archive.is_file())
            # The corpus is streamed through temp files; resident Python memory
            # should stay far below materializing 20k ZIP payloads/JSON strings.
            self.assertLess(peak, 32 * 1024 * 1024)
            with zipfile.ZipFile(archive) as bundle:
                manifest = json.loads(bundle.read("MANIFEST.json"))
                self.assertEqual(manifest["novel_records"], 20_000)
                self.assertEqual(
                    len(bundle.read("1999.txt").splitlines()),
                    20_000,
                )


    def test_formal_streaming_export_is_byte_deterministic_and_requires_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_root, documentation, artifacts = _fixture_files(root)
            records = (
                EvidenceCapsule(
                    "deterministic.example",
                    1998,
                    "fixture",
                    "capture_timestamp_year",
                    "19980101000000",
                    "http://deterministic.example/",
                    "f" * 64,
                    "fixture-v1",
                    "exact_host_cdx_capture",
                    "fixture",
                    "http://deterministic.example/",
                    "fixture:deterministic",
                    "synthetic",
                ),
            )
            snapshot = _snapshot(created_at="2026-09-13T06:02:00+00:00")

            with self.assertRaisesRegex(ValueError, "production_config"):
                build_streaming_submission_zip(
                    snapshot,
                    "missing-config",
                    root / "missing-config-out",
                    source_root=source_root,
                    documentation_path=documentation,
                    evidence_records=iter(records),
                    artifact_specs=(),
                    artifact_allowed_roots=(root,),
                )

            first = build_streaming_submission_zip(
                snapshot,
                "deterministic",
                root / "out-a",
                source_root=source_root,
                documentation_path=documentation,
                evidence_records=iter(records),
                artifact_specs=artifacts,
                artifact_allowed_roots=(root,),
            )
            second = build_streaming_submission_zip(
                snapshot,
                "deterministic",
                root / "out-b",
                source_root=source_root,
                documentation_path=documentation,
                evidence_records=iter(records),
                artifact_specs=artifacts,
                artifact_allowed_roots=(root,),
            )

            self.assertEqual(
                hashlib.sha256(first.read_bytes()).hexdigest(),
                hashlib.sha256(second.read_bytes()).hexdigest(),
            )


if __name__ == "__main__":
    unittest.main()
