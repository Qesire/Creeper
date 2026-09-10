"""Thin bridge from runtime stores to the canonical submission builder."""

from __future__ import annotations

from dataclasses import dataclass

from creeper.authority.baseline_index import YEAR_BITS, BaselineIndex
from creeper.storage.evidence_store import EvidenceStore
from creeper.submission.builder import build_snapshot
from creeper.submission.snapshot import SubmissionSnapshot


@dataclass(frozen=True)
class RuntimeSubmissionContext:
    """All immutable inputs required to build a runtime submission snapshot."""

    baseline_manifest: dict[str, object]
    code_revision: str
    source_report_set: tuple[str, ...]
    cdx_audit_set: tuple[str, ...]
    eed_report: dict[str, object]
    active_candidates: tuple[str, ...] = ()
    active_candidate_scopes: tuple[str, ...] = ()
    isc_reference: tuple[str, ...] = ()
    unparsed: tuple[str, ...] = ()
    novel_eed: str = "0"
    growth_rate: str = "0"


def build_runtime_snapshot(
    *,
    context: RuntimeSubmissionContext,
    evidence_store: EvidenceStore,
    baseline: BaselineIndex,
    snapshot_id: str,
) -> SubmissionSnapshot:
    """Build a snapshot from durable runtime stores using the canonical builder."""
    novel_capsules = [
        capsule
        for capsule in evidence_store.all_capsules()
        if capsule.year in YEAR_BITS
        and not baseline.year_mask(capsule.hostname) & YEAR_BITS[capsule.year]
    ]
    return build_snapshot(
        snapshot_id,
        novel_capsules,
        baseline,
        context.baseline_manifest,
        code_revision=context.code_revision,
        source_report_set=context.source_report_set,
        cdx_audit_set=context.cdx_audit_set,
        eed_report=context.eed_report,
        active_candidates=context.active_candidates,
        active_candidate_scopes=context.active_candidate_scopes,
        isc_reference=context.isc_reference,
        unparsed=context.unparsed,
        novel_eed=context.novel_eed,
        growth_rate=context.growth_rate,
    )
