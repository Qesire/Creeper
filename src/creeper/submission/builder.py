"""Build a submission snapshot from persisted evidence and authority state."""

from __future__ import annotations

from datetime import datetime, timezone

from creeper.authority.baseline_index import BaselineIndex, YEAR_BITS
from creeper.authority.normalizer import normalize_official
from creeper.evidence.policies import EvidenceCapsule
from creeper.submission.precheck import precheck_submission
from creeper.submission.snapshot import SubmissionSnapshot


def build_snapshot(
    snapshot_id: str,
    capsules: list[EvidenceCapsule],
    index: BaselineIndex,
    baseline_manifest: dict,
    *,
    code_revision: str,
    source_report_set: tuple[str, ...],
    cdx_audit_set: tuple[str, ...],
    eed_report: dict[str, object],
    active_candidates: tuple[str, ...] = (),
    active_candidate_scopes: tuple[str, ...] = (),
    isc_reference: tuple[str, ...] = (),
    unparsed: tuple[str, ...] = (),
    novel_eed: str = "0",
    growth_rate: str = "0",
) -> SubmissionSnapshot:
    baseline_hashes = {
        name.removesuffix(".txt"): value
        for name, value in baseline_manifest.get("annual_file_hashes", {}).items()
    }
    novel: list[EvidenceCapsule] = []
    seen: set[tuple[str, int]] = set()
    invalid_count = overlap_count = 0
    for capsule in capsules:
        hostname = normalize_official(capsule.hostname)
        if hostname is None or capsule.year not in YEAR_BITS:
            invalid_count += 1
            continue
        key = (hostname, capsule.year)
        if key in seen:
            continue
        seen.add(key)
        if index.year_mask(hostname) & YEAR_BITS[capsule.year]:
            overlap_count += 1
            continue
        novel.append(
            EvidenceCapsule(
                hostname,
                capsule.year,
                capsule.provider,
                capsule.temporal_semantics,
                capsule.evidence_timestamp,
                capsule.source_locator,
                capsule.payload_hash,
                capsule.policy_version,
                capsule.evidence_type,
                capsule.source_id,
                capsule.original_url,
                capsule.record_locator,
                capsule.extraction_method,
            )
        )
    coverage = len(novel) / len(capsules) if capsules else 0.0
    snapshot = SubmissionSnapshot(
        submission_snapshot_id=snapshot_id,
        created_at=datetime.now(timezone.utc).isoformat(),
        baseline_id=baseline_manifest.get("baseline_id", ""),
        baseline_hashes=baseline_hashes,
        normalizer_version="official-calculator-regex-v1",
        evidence_policy_version="evidence-v1",
        eed_policy_version="eed-v1",
        novel_records=tuple(novel),
        novel_eed=novel_eed,
        growth_rate=growth_rate,
        evidence_coverage=f"{coverage:.6f}",
        invalid_count=invalid_count,
        overlap_count=overlap_count,
        source_report_set=source_report_set,
        cdx_audit_set=cdx_audit_set,
        code_revision=code_revision,
        active_candidates=active_candidates,
        active_candidate_scopes=active_candidate_scopes,
        isc_reference=isc_reference,
        unparsed=unparsed,
        eed_report=eed_report,
    )
    return SubmissionSnapshot(**{**snapshot.__dict__, "ready": precheck_submission(snapshot).ready})
