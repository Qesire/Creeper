"""Immutable in-memory submission snapshot contract."""

from __future__ import annotations

from dataclasses import dataclass, field

from creeper.evidence.policies import EvidenceCapsule


@dataclass(frozen=True)
class SubmissionSnapshot:
    submission_snapshot_id: str
    created_at: str
    baseline_id: str
    baseline_hashes: dict[str, str]
    normalizer_version: str
    evidence_policy_version: str
    eed_policy_version: str
    novel_records: tuple[EvidenceCapsule, ...]
    novel_eed: str
    growth_rate: str
    evidence_coverage: str
    invalid_count: int
    overlap_count: int
    source_report_set: tuple[str, ...]
    cdx_audit_set: tuple[str, ...]
    code_revision: str
    ready: bool = False
    active_candidates: tuple[str, ...] = ()
    active_candidate_scopes: tuple[str, ...] = ()
    isc_reference: tuple[str, ...] = ()
    unparsed: tuple[str, ...] = ()
    eed_report: dict[str, object] | None = None
    incomplete_query_count: int = 0
    candidate_file_hash: str = ""
    model_hash: str = ""
    baseline_eed: str = "0"
    authority_digest: str = ""
    source_contribution: dict[str, object] | None = None
    within_year_duplicates: int = 0
