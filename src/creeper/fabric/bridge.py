"""Pure adapters from current Creeper work units to Fabric v2 WorkSpecs."""

from __future__ import annotations

from dataclasses import dataclass

from creeper.evidence.policies import EvidenceQueryKey
from creeper.source_discovery.models import SourceCandidate
from creeper.source_discovery.residual_search import QueryPlan

from .models import (
    FabricCapability,
    FabricWorkClass,
    WorkSpec,
)


FABRIC_RESIDUAL_ALGORITHM = "residual-provider-slice-v1"
FABRIC_RESIDUAL_REDUCER_ALGORITHM = "residual-provider-barrier-v1"
FABRIC_SOURCE_TRIAGE_ALGORITHM = "source-triage-v1"
FABRIC_SOURCE_SCOUT_ALGORITHM = "source-scout-v1"
FABRIC_ADAPTER_ALGORITHM = "adapter-compile-v1"
FABRIC_RESERVOIR_ALGORITHM = "reservoir-produce-v1"
FABRIC_EVIDENCE_ALGORITHM = "evidence-complete-v1"


@dataclass(frozen=True, slots=True)
class ResidualSearchDag:
    provider_slices: tuple[WorkSpec, ...]
    reducer: WorkSpec

    def __post_init__(self) -> None:
        if not self.provider_slices:
            raise ValueError("residual search DAG requires provider slices")
        expected = {item.work_key for item in self.provider_slices}
        if set(self.reducer.dependency_work_keys) != expected:
            raise ValueError("residual reducer must depend on every provider slice")


def query_plan_payload(plan: QueryPlan) -> dict[str, object]:
    return {
        "cell": {
            "mechanism": plan.cell.mechanism,
            "institution": plan.cell.institution,
            "period": plan.cell.period,
            "artifact": plan.cell.artifact,
            "key": plan.cell.key,
        },
        "query": plan.query,
        "variant": plan.variant,
        "exclusions": list(plan.exclusions),
        "score": plan.score,
        "mechanism_phrase": plan.mechanism_phrase,
        "include_institution": plan.include_institution,
        "query_shape": plan.query_shape,
    }


def residual_provider_work(plan: QueryPlan, provider: str) -> WorkSpec:
    if not provider.strip():
        raise ValueError("provider must be non-empty")
    payload = query_plan_payload(plan)
    payload["provider"] = provider
    return WorkSpec(
        work_class=FabricWorkClass.RESIDUAL_SEARCH,
        producer="residual-search-provider",
        algorithm_version=FABRIC_RESIDUAL_ALGORITHM,
        partition_key=f"{provider}:{plan.cell.key}",
        input_identity=(
            f"{plan.cell.key}:variant={plan.variant}:provider={provider}"
        ),
        coverage=payload,
        required_capabilities=(FabricCapability.SEARCH_STRUCTURED,),
        priority=float(plan.score),
        queue="network",
        provider=provider,
        network_class="public",
    )


def residual_search_dag(
    plan: QueryPlan,
    providers: tuple[str, ...],
) -> ResidualSearchDag:
    normalized = tuple(dict.fromkeys(item.strip() for item in providers if item.strip()))
    if not normalized:
        raise ValueError("at least one residual provider is required")
    slices = tuple(residual_provider_work(plan, provider) for provider in normalized)
    reducer = WorkSpec(
        work_class=FabricWorkClass.REDUCE_COMMIT,
        producer="residual-search-reducer",
        algorithm_version=FABRIC_RESIDUAL_REDUCER_ALGORITHM,
        partition_key=plan.cell.key,
        input_identity=f"{plan.cell.key}:variant={plan.variant}",
        coverage={
            "plan": query_plan_payload(plan),
            "providers": list(normalized),
        },
        required_capabilities=(FabricCapability.AUTHORITY_REDUCE,),
        priority=float(plan.score) + 1000.0,
        queue="authority",
        dependency_work_keys=tuple(item.work_key for item in slices),
        network_class="private",
    )
    return ResidualSearchDag(slices, reducer)


def _candidate_payload(candidate: SourceCandidate) -> dict[str, object]:
    return {
        "source_key": candidate.source_key,
        "canonical_entrypoint": candidate.canonical_entrypoint,
        "source_family": candidate.source_family,
        "source_level": candidate.level.value,
        "discovered_by": candidate.discovered_by,
        "discovery_strategy": candidate.discovery_strategy,
        "expected_year_from": candidate.expected_year_from,
        "expected_year_to": candidate.expected_year_to,
        "expected_volume": candidate.expected_volume,
        "confidence": candidate.confidence,
    }


def source_triage_work(candidate: SourceCandidate) -> WorkSpec:
    return WorkSpec(
        work_class=FabricWorkClass.SOURCE_TRIAGE,
        producer="source-discovery",
        algorithm_version=FABRIC_SOURCE_TRIAGE_ALGORITHM,
        partition_key=candidate.source_key,
        input_identity=candidate.canonical_entrypoint,
        coverage=_candidate_payload(candidate),
        required_capabilities=(FabricCapability.HTTP_FETCH,),
        priority=float(candidate.scout_priority),
        queue="network",
        network_class="public",
    )


def source_scout_work(
    candidate: SourceCandidate,
    *,
    triage_work: WorkSpec | None = None,
) -> WorkSpec:
    dependencies = () if triage_work is None else (triage_work.work_key,)
    return WorkSpec(
        work_class=FabricWorkClass.SOURCE_SCOUT,
        producer="source-discovery",
        algorithm_version=FABRIC_SOURCE_SCOUT_ALGORITHM,
        partition_key=candidate.source_key,
        input_identity=candidate.canonical_entrypoint,
        coverage=_candidate_payload(candidate),
        required_capabilities=(FabricCapability.SOURCE_SCOUT,),
        priority=float(candidate.scout_priority),
        queue="network",
        network_class="public",
        dependency_work_keys=dependencies,
    )


def adapter_compile_work(
    candidate: SourceCandidate,
    *,
    sample_fingerprint: str,
    scout_work: WorkSpec | None = None,
) -> WorkSpec:
    if not sample_fingerprint.strip():
        raise ValueError("sample_fingerprint must be non-empty")
    dependencies = () if scout_work is None else (scout_work.work_key,)
    payload = _candidate_payload(candidate)
    payload["sample_fingerprint"] = sample_fingerprint
    return WorkSpec(
        work_class=FabricWorkClass.ADAPTER_COMPILE,
        producer="unknown-format-adapter",
        algorithm_version=FABRIC_ADAPTER_ALGORITHM,
        partition_key=candidate.source_key,
        input_identity=f"{candidate.source_key}:{sample_fingerprint}",
        coverage=payload,
        required_capabilities=(FabricCapability.ADAPTER_LLM,),
        priority=100.0 + float(candidate.scout_priority),
        queue="adapter",
        dependency_work_keys=dependencies,
    )


def reservoir_produce_work(
    *,
    reservoir_id: str,
    partition_key: str,
    cursor_start: str | None,
    max_records: int,
    max_bytes: int,
    evidence_mode: str,
) -> WorkSpec:
    if not reservoir_id.strip() or not partition_key.strip():
        raise ValueError("reservoir_id and partition_key are required")
    if max_records < 1 or max_bytes < 1:
        raise ValueError("reservoir bounds must be positive")
    return WorkSpec(
        work_class=FabricWorkClass.RESERVOIR_PRODUCE,
        producer="source-producer",
        algorithm_version=FABRIC_RESERVOIR_ALGORITHM,
        partition_key=partition_key,
        input_identity=f"{reservoir_id}:{cursor_start or 'START'}",
        coverage={
            "reservoir_id": reservoir_id,
            "cursor_start": cursor_start,
            "max_records": int(max_records),
            "max_bytes": int(max_bytes),
            "evidence_mode": evidence_mode,
        },
        required_capabilities=(FabricCapability.STREAM_BULK,),
        priority=10.0 if evidence_mode == "direct_year" else 1.0,
        queue="bulk",
    )


def evidence_complete_work(key: EvidenceQueryKey) -> WorkSpec:
    scope = key.temporal_scope
    return WorkSpec(
        work_class=FabricWorkClass.EVIDENCE_COMPLETE,
        producer="evidence-planner",
        algorithm_version=FABRIC_EVIDENCE_ALGORITHM,
        partition_key=f"{key.provider}:{key.hostname[:2]}",
        input_identity=(
            f"{key.hostname}:{scope.year_from}-{scope.year_to}:"
            f"{key.provider}:{key.policy_version}"
        ),
        coverage={
            "hostname": key.hostname,
            "year_from": scope.year_from,
            "year_to": scope.year_to,
            "provider": key.provider,
            "policy_version": key.policy_version,
        },
        required_capabilities=(FabricCapability.EVIDENCE_QUERY,),
        priority=1.0,
        queue="evidence",
        provider=key.provider,
        network_class="public",
    )
