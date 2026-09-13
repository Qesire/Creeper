"""Formal V5 submission export from durable runtime state.

The command consumes the incremental readiness authority report, streams
canonical evidence/candidates from the runtime databases, freezes explicitly
named production artifacts, and publishes an archive only after the independent
verifier succeeds.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from creeper.authority.identity import (
    AuthoritySnapshot,
    eed_model_authority_signature,
)
from creeper.runtime.submission import export_runtime_submission
from creeper.storage.candidate_store import CandidateStore
from creeper.storage.evidence_store import EvidenceStore
from creeper.submission.artifact_manifest import ArtifactSpec
from creeper.submission.snapshot import SubmissionSnapshot


def _load_object(path: Path, name: str) -> dict[str, object]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {name}: {path}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a JSON object")
    return value


def _required_text(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"readiness report requires non-empty {key}")
    return value


def _build_snapshot_from_readiness(
    *,
    readiness: dict[str, object],
    authority: AuthoritySnapshot,
    eed_model_path: Path,
    snapshot_id: str,
    code_revision: str,
    source_report_set: tuple[str, ...],
    cdx_audit_set: tuple[str, ...],
    created_at: str,
) -> SubmissionSnapshot:
    report_authority = readiness.get("authority_digest")
    if report_authority and report_authority != authority.authority_digest:
        raise ValueError(
            "readiness authority_digest does not match baseline manifest"
        )
    baseline_signature = readiness.get("baseline_signature")
    if baseline_signature and baseline_signature != authority.authority_digest:
        raise ValueError(
            "readiness baseline_signature does not match baseline authority"
        )
    actual_model = eed_model_authority_signature(Path(eed_model_path))
    if actual_model != authority.model_hash:
        raise ValueError("EED model does not match baseline authority")
    report_model = readiness.get("model_signature")
    if report_model and report_model != actual_model:
        raise ValueError(
            "readiness model_signature does not match supplied EED model"
        )

    source_contribution = readiness.get("source_contribution")
    if not isinstance(source_contribution, dict):
        raise ValueError(
            "readiness report requires authoritative source_contribution"
        )
    expected_lanes = {
        "by_source",
        "direct_annual",
        "verified_candidate",
        "other_restricted",
    }
    if set(source_contribution) != expected_lanes:
        raise ValueError(
            "readiness source_contribution does not match formal V5 lane schema"
        )

    reconciliation = readiness.get("baseline_reconciliation")
    if not isinstance(reconciliation, dict):
        raise ValueError("readiness report requires baseline_reconciliation")

    novel_eed = _required_text(readiness, "novel_eed")
    growth_rate = _required_text(readiness, "growth_rate")
    baseline_hashes = {
        name.removesuffix(".txt"): digest
        for name, digest in authority.annual_file_hashes.items()
    }
    return SubmissionSnapshot(
        submission_snapshot_id=snapshot_id,
        created_at=created_at,
        baseline_id=authority.baseline_id,
        baseline_hashes=baseline_hashes,
        normalizer_version="official-calculator-regex-v1",
        evidence_policy_version="evidence-v1",
        eed_policy_version="official-eed-model-v1",
        # Formal V5 export deliberately streams EvidenceStore instead.
        novel_records=(),
        novel_eed=novel_eed,
        growth_rate=growth_rate,
        evidence_coverage="1",
        invalid_count=int(reconciliation.get("invalid_records", 0) or 0),
        overlap_count=int(reconciliation.get("baseline_overlap", 0) or 0),
        source_report_set=source_report_set,
        cdx_audit_set=cdx_audit_set,
        code_revision=code_revision,
        eed_report={
            "authority": "incremental-readiness-v2",
            "equivalent_english_domains": novel_eed,
        },
        incomplete_query_count=int(
            readiness.get("incomplete_query_count", 0) or 0
        ),
        candidate_file_hash=authority.candidate_file_hash,
        model_hash=authority.model_hash,
        baseline_eed=authority.baseline_eed,
        authority_digest=authority.authority_digest,
        source_contribution=source_contribution,
        within_year_duplicates=int(
            reconciliation.get("within_year_duplicates", 0) or 0
        ),
    )


def run_export(
    *,
    runtime_data_root: Path,
    readiness_report: Path,
    baseline_manifest: Path,
    baseline_index: Path,
    eed_model: Path,
    source_root: Path,
    documentation: Path,
    production_config: Path,
    source_reports: tuple[Path, ...],
    cdx_audits: tuple[Path, ...],
    output_dir: Path,
    name: str,
    snapshot_id: str,
    code_revision: str,
    created_at: str | None = None,
):
    if not source_reports:
        raise ValueError("at least one source report is required")
    if not cdx_audits:
        raise ValueError("at least one CDX audit artifact is required")
    runtime_data_root = Path(runtime_data_root)
    readiness = _load_object(Path(readiness_report), "readiness report")
    authority = AuthoritySnapshot.from_manifest_path(Path(baseline_manifest))
    when = created_at or datetime.now(timezone.utc).isoformat()

    source_report_paths = tuple(
        f"artifacts/source_reports/{path.name}" for path in source_reports
    )
    cdx_audit_paths = tuple(
        f"artifacts/cdx_audit/{path.name}" for path in cdx_audits
    )
    snapshot = _build_snapshot_from_readiness(
        readiness=readiness,
        authority=authority,
        eed_model_path=Path(eed_model),
        snapshot_id=snapshot_id,
        code_revision=code_revision,
        source_report_set=source_report_paths,
        cdx_audit_set=cdx_audit_paths,
        created_at=when,
    )

    specs = [
        ArtifactSpec(
            logical_role="production_config",
            source_path=Path(production_config),
            archive_path=f"artifacts/production/{Path(production_config).name}",
            allow_external=True,
        )
    ]
    specs.extend(
        ArtifactSpec(
            logical_role="source_report",
            source_path=path,
            archive_path=archive_path,
            allow_external=True,
        )
        for path, archive_path in zip(
            source_reports,
            source_report_paths,
            strict=True,
        )
    )
    specs.extend(
        ArtifactSpec(
            logical_role="cdx_audit",
            source_path=path,
            archive_path=archive_path,
            allow_external=True,
        )
        for path, archive_path in zip(
            cdx_audits,
            cdx_audit_paths,
            strict=True,
        )
    )
    topology = runtime_data_root / "topology.json"
    if topology.is_file():
        specs.append(
            ArtifactSpec(
                logical_role="runtime_topology",
                source_path=topology,
                archive_path="artifacts/runtime/topology.json",
                allow_external=True,
            )
        )
    production_value = runtime_data_root / "readiness" / "production_value.json"
    if production_value.is_file():
        specs.append(
            ArtifactSpec(
                logical_role="production_value",
                source_path=production_value,
                archive_path="artifacts/runtime/production_value.json",
                allow_external=True,
            )
        )

    evidence = EvidenceStore(runtime_data_root / "evidence.sqlite3")
    candidates = CandidateStore(runtime_data_root / "candidates.sqlite3")
    try:
        return export_runtime_submission(
            snapshot=snapshot,
            evidence_store=evidence,
            candidate_store=candidates,
            baseline_manifest_path=Path(baseline_manifest),
            baseline_index_path=Path(baseline_index),
            eed_model_path=Path(eed_model),
            name=name,
            output_dir=Path(output_dir),
            source_root=Path(source_root),
            documentation_path=Path(documentation),
            artifact_specs=tuple(specs),
            artifact_allowed_roots=(),
        )
    finally:
        candidates.close()
        evidence.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="creeper-submission-export")
    parser.add_argument("runtime_data_root", type=Path)
    parser.add_argument("--readiness-report", type=Path)
    parser.add_argument("--baseline-manifest", type=Path, required=True)
    parser.add_argument("--baseline-index", type=Path, required=True)
    parser.add_argument("--eed-model", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--documentation", type=Path, required=True)
    parser.add_argument("--production-config", type=Path, required=True)
    parser.add_argument(
        "--source-report",
        type=Path,
        action="append",
        dest="source_reports",
        required=True,
    )
    parser.add_argument(
        "--cdx-audit",
        type=Path,
        action="append",
        dest="cdx_audits",
        required=True,
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--snapshot-id", default="v5-formal-runtime")
    parser.add_argument("--code-revision", required=True)
    args = parser.parse_args(argv)

    readiness_report = args.readiness_report or (
        args.runtime_data_root / "readiness" / "readiness.json"
    )
    try:
        archive, verification = run_export(
            runtime_data_root=args.runtime_data_root,
            readiness_report=readiness_report,
            baseline_manifest=args.baseline_manifest,
            baseline_index=args.baseline_index,
            eed_model=args.eed_model,
            source_root=args.source_root,
            documentation=args.documentation,
            production_config=args.production_config,
            source_reports=tuple(args.source_reports),
            cdx_audits=tuple(args.cdx_audits),
            output_dir=args.output_dir,
            name=args.name,
            snapshot_id=args.snapshot_id,
            code_revision=args.code_revision,
        )
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    print(
        json.dumps(
            {
                "archive": str(archive),
                "verification_ready": verification.ready,
                "annual_records": verification.annual_records,
                "recomputed_novel_eed": verification.recomputed_novel_eed,
                "recomputed_growth_rate": verification.recomputed_growth_rate,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
