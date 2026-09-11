"""Thin bridge from runtime stores to the canonical submission builder."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from creeper.authority.baseline_index import YEAR_BITS, BaselineIndex
from creeper.authority.eed import calculate_eed_values
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
    eed_model_path: Path | None = None
    baseline_eed: str = "0"


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
    if context.eed_model_path is None:
        eed_report = {
            "authority": "missing-official-eed-model",
            "equivalent_english_domains": "0",
            "method": "runtime snapshot is not submission-authoritative without the official EED model",
        }
        novel_eed = "0"
        growth_rate = "0"
    else:
        eed_report, _ = calculate_eed_values(
            (capsule.hostname for capsule in novel_capsules),
            Path(context.eed_model_path),
            input_file="<runtime-evidence-store>",
        )
        eed_report = {
            **eed_report,
            "authority": "official-calculator-v1",
            "model_path": str(Path(context.eed_model_path).resolve()),
        }
        novel_eed = str(eed_report["equivalent_english_domains"])
        baseline_eed = Decimal(str(context.baseline_eed))
        growth_rate = (
            format(Decimal(novel_eed) / baseline_eed, "f")
            if baseline_eed > 0
            else "0"
        )
    return build_snapshot(
        snapshot_id,
        novel_capsules,
        baseline,
        context.baseline_manifest,
        code_revision=context.code_revision,
        source_report_set=context.source_report_set,
        cdx_audit_set=context.cdx_audit_set,
        eed_report=eed_report,
        active_candidates=context.active_candidates,
        active_candidate_scopes=context.active_candidate_scopes,
        isc_reference=context.isc_reference,
        unparsed=context.unparsed,
        novel_eed=novel_eed,
        growth_rate=growth_rate,
    )
