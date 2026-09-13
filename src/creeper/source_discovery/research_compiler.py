"""Deterministic execution-plane compiler for unified LLM proposals.

L6 compiles proposal structure only.  Region persistence/execution belongs to L1
and rule persistence belongs to L3.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Callable, Mapping

from creeper.source_discovery.models import (
    canonicalize_source_entrypoint,
    is_common_crawl_provenance,
)
from creeper.source_research.agent.compiler import (
    UnifiedCompilerError,
    UnifiedResearchCompiler,
)
from creeper.source_research.agent.protocol import (
    ContractFamilyProposal,
    ExplorationRegionProposal,
    ProposalType,
    UnifiedLLMTask,
    stable_identity,
)


class ResearchCompilerError(ValueError):
    """An execution-plane proposal is unsafe, unbounded, or non-reusable."""


class RegionSurfaceKind(StrEnum):
    WEB_SEARCH = "WEB_SEARCH"
    HTML_CATALOG = "HTML_CATALOG"
    HTTP_API = "HTTP_API"
    REPOSITORY = "REPOSITORY"
    MANIFEST = "MANIFEST"
    FILENAME_FAMILY = "FILENAME_FAMILY"
    LOCAL_HINT = "LOCAL_HINT"


class RegionEnumeratorKind(StrEnum):
    STATIC_LIST = "STATIC_LIST"
    HTML_CATALOG = "HTML_CATALOG"
    INTEGER_PAGINATION = "INTEGER_PAGINATION"
    CURSOR_API = "CURSOR_API"
    FILENAME_PATTERN = "FILENAME_PATTERN"
    MANIFEST_FILE = "MANIFEST_FILE"


class RegionState(StrEnum):
    PROPOSED = "PROPOSED"
    VALIDATED = "VALIDATED"
    READY = "READY"
    RUNNING = "RUNNING"
    EXHAUSTED = "EXHAUSTED"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class ResearchCompilerPolicy:
    min_expected_fanout: int = 100
    max_regions_per_response: int = 8
    max_query_expansion: int = 512
    max_template_expansion: int = 4096
    max_requests_per_region: int = 80
    target_year_from: int = 1996
    target_year_to: int = 2001

    def __post_init__(self) -> None:
        if self.min_expected_fanout < 1 or self.max_regions_per_response < 1:
            raise ValueError("region limits must be positive")
        if self.max_query_expansion < 1 or self.max_template_expansion < 1:
            raise ValueError("expansion limits must be positive")
        if self.max_requests_per_region < 1:
            raise ValueError("max_requests_per_region must be positive")
        if self.target_year_from > self.target_year_to:
            raise ValueError("target years are reversed")


@dataclass(frozen=True)
class CompiledScoutPlan:
    proposal_id: str
    region_key: str
    reuse_key: str
    surface_kind: RegionSurfaceKind
    root: str
    query_family: dict[str, Any]
    enumerator_spec: dict[str, Any]
    artifact_predicate: dict[str, Any]
    hard_bounds: dict[str, int]
    stop_conditions: tuple[str, ...]
    expected_source_family: str
    expected_contract_family: str
    expected_mechanism: str
    expected_fanout: int
    confidence: float
    validation: dict[str, Any]
    state: RegionState = RegionState.VALIDATED
    context_hash: str = ""
    created_by_episode_id: str = ""

    def as_region(self) -> dict[str, Any]:
        return {
            "region_id": self.proposal_id,
            "region_key": self.region_key,
            "reuse_key": self.reuse_key,
            "source": self.root,
            "surface_kind": self.surface_kind.value,
            "root": self.root,
            "query_family": self.query_family,
            "enumerator_spec": self.enumerator_spec,
            "artifact_predicate": self.artifact_predicate,
            "hard_bounds": self.hard_bounds,
            "stop_conditions": list(self.stop_conditions),
            "expected_source_family": self.expected_source_family,
            "expected_contract_family": self.expected_contract_family,
            "expected_mechanism": self.expected_mechanism,
            "expected_fanout": self.expected_fanout,
            "confidence": self.confidence,
            "validation": self.validation,
            "state": self.state.value,
            "context_hash": self.context_hash,
            "created_by_episode_id": self.created_by_episode_id,
        }


@dataclass(frozen=True)
class CompiledContractPlan:
    proposal_id: str
    contract_family: str
    recognition: dict[str, Any]
    extraction: dict[str, Any]
    hard_bounds: dict[str, int]
    stop_conditions: tuple[str, ...]
    expected_mechanism: str
    reuse_key: str
    confidence: float
    validation: dict[str, Any]
    context_hash: str = ""
    created_by_episode_id: str = ""


class ResearchCompiler:
    """Compile execution-plane proposals without touching durable state."""

    def __init__(
        self,
        *,
        policy: ResearchCompilerPolicy | None = None,
        negative_knowledge: Callable[[str], bool] | None = None,
    ) -> None:
        self.policy = policy or ResearchCompilerPolicy()
        self.negative_knowledge = negative_knowledge
        self.unified = UnifiedResearchCompiler()

    def compile_response(
        self,
        response: Mapping[str, Any],
        *,
        context_hash: str = "",
        episode_id: str = "",
        task_type: str | UnifiedLLMTask = UnifiedLLMTask.DISCOVER_NEW_SOURCE,
    ) -> tuple[CompiledScoutPlan, ...]:
        """Compatibility entry point returning only executable region proposals."""

        regions, _contracts = self.compile_execution_response(
            response,
            context_hash=context_hash,
            episode_id=episode_id,
            task_type=task_type,
        )
        return regions

    def compile_execution_response(
        self,
        response: Mapping[str, Any],
        *,
        context_hash: str = "",
        episode_id: str = "",
        task_type: str | UnifiedLLMTask = UnifiedLLMTask.DISCOVER_NEW_SOURCE,
    ) -> tuple[tuple[CompiledScoutPlan, ...], tuple[CompiledContractPlan, ...]]:
        normalized = self._normalize_legacy(response)
        try:
            envelope = self.unified.compile(
                normalized,
                task_type=task_type,
                context_hash=context_hash,
            )
        except (UnifiedCompilerError, ValueError) as exc:
            raise ResearchCompilerError(str(exc)) from exc

        regions: list[CompiledScoutPlan] = []
        contracts: list[CompiledContractPlan] = []
        for proposal in envelope.proposals:
            if isinstance(proposal, ExplorationRegionProposal):
                regions.append(
                    self._compile_region(
                        proposal,
                        context_hash=envelope.context_hash,
                        episode_id=episode_id,
                    )
                )
            elif isinstance(proposal, ContractFamilyProposal):
                contracts.append(
                    CompiledContractPlan(
                        proposal_id=proposal.proposal_id,
                        contract_family=proposal.contract_family,
                        recognition=dict(proposal.recognition),
                        extraction=dict(proposal.extraction),
                        hard_bounds=dict(proposal.hard_bounds),
                        stop_conditions=proposal.stop_conditions,
                        expected_mechanism=proposal.expected_mechanism,
                        reuse_key=proposal.reuse_key,
                        confidence=proposal.confidence,
                        validation=dict(proposal.validation),
                        context_hash=envelope.context_hash,
                        created_by_episode_id=episode_id,
                    )
                )
            else:
                raise ResearchCompilerError(
                    "non-execution proposal reached execution compiler"
                )

        if len(regions) > self.policy.max_regions_per_response:
            raise ResearchCompilerError("too many regions in one response")
        keys = [item.region_key for item in regions]
        if len(keys) != len(set(keys)):
            raise ResearchCompilerError("duplicate region keys")
        return tuple(regions), tuple(contracts)

    def _normalize_legacy(
        self, response: Mapping[str, Any]
    ) -> dict[str, Any]:
        if not isinstance(response, Mapping):
            raise ResearchCompilerError("research response must be an object")
        if "proposals" in response:
            return dict(response)

        allowed = {"query", "regions", "contract_proposals"}
        if set(response) - allowed:
            raise ResearchCompilerError("unknown research response fields")
        query = response.get("query")
        if not isinstance(query, str) or not query.strip():
            raise ResearchCompilerError("research query must be non-empty")
        regions = response.get("regions", [])
        contracts = response.get("contract_proposals", [])
        if not isinstance(regions, list) or not isinstance(contracts, list):
            raise ResearchCompilerError(
                "regions and contract_proposals must be arrays"
            )

        proposals: list[dict[str, Any]] = []
        for raw in regions:
            if not isinstance(raw, Mapping):
                raise ResearchCompilerError("region must be an object")
            item = dict(raw)
            item["type"] = ProposalType.EXPLORATION_REGION.value
            if "reuse_key" not in item:
                item["reuse_key"] = stable_identity(
                    "region-reuse",
                    {
                        "root": item.get("root"),
                        "query_family": item.get("query_family"),
                        "enumerator": item.get("enumerator"),
                    },
                )
            proposals.append(item)

        for raw in contracts:
            if not isinstance(raw, Mapping):
                raise ResearchCompilerError(
                    "contract proposal must be an object"
                )
            item = dict(raw)
            item["type"] = ProposalType.CONTRACT_FAMILY.value
            if "reuse_key" not in item:
                item["reuse_key"] = stable_identity(
                    "contract-reuse",
                    {
                        "contract_family": item.get("contract_family"),
                        "recognition": item.get("recognition"),
                    },
                )
            proposals.append(item)
        return {"query": query.strip(), "proposals": proposals}

    def _compile_region(
        self,
        proposal: ExplorationRegionProposal,
        *,
        context_hash: str,
        episode_id: str,
    ) -> CompiledScoutPlan:
        try:
            surface = RegionSurfaceKind(proposal.surface_kind)
            enumerator = RegionEnumeratorKind(proposal.enumerator)
        except ValueError as exc:
            raise ResearchCompilerError(
                "unsupported surface or enumerator"
            ) from exc

        try:
            root = canonicalize_source_entrypoint(proposal.root)
        except (TypeError, ValueError) as exc:
            raise ResearchCompilerError(f"invalid region root: {exc}") from exc

        if is_common_crawl_provenance(
            root,
            proposal.expected_source_family,
            proposal.expected_contract_family,
        ):
            raise ResearchCompilerError(
                "Common Crawl active-corpus discovery is excluded"
            )
        if self.negative_knowledge is not None and self.negative_knowledge(root):
            raise ResearchCompilerError(
                "root is blocked by negative knowledge"
            )

        bounds = dict(proposal.hard_bounds)
        if bounds.get("max_requests", 1) > self.policy.max_requests_per_region:
            raise ResearchCompilerError(
                "region request bound exceeds policy"
            )
        fanout = self._verified_fanout(
            proposal.query_family,
            enumerator,
            bounds,
            proposal.expected_fanout,
        )
        if (
            fanout < self.policy.min_expected_fanout
            and not self._small_region_exception(surface)
        ):
            raise ResearchCompilerError(
                "expected fanout is below minimum"
            )

        return CompiledScoutPlan(
            proposal_id=proposal.proposal_id,
            region_key=self._region_key(
                root, proposal.query_family, enumerator.value
            ),
            reuse_key=proposal.reuse_key,
            surface_kind=surface,
            root=root,
            query_family=dict(proposal.query_family),
            enumerator_spec={
                "kind": enumerator.value,
                "config": dict(proposal.enumerator_config),
            },
            artifact_predicate=dict(proposal.artifact_predicate),
            hard_bounds=bounds,
            stop_conditions=proposal.stop_conditions,
            expected_source_family=proposal.expected_source_family,
            expected_contract_family=proposal.expected_contract_family,
            expected_mechanism=proposal.expected_mechanism,
            expected_fanout=fanout,
            confidence=proposal.confidence,
            validation=dict(proposal.validation),
            context_hash=context_hash,
            created_by_episode_id=episode_id,
        )

    def _verified_fanout(
        self,
        query_family: Mapping[str, Any],
        enumerator: RegionEnumeratorKind,
        bounds: Mapping[str, int],
        declared: int,
    ) -> int:
        cardinality = 1
        finite_dimension_seen = False
        for value in query_family.values():
            if isinstance(value, list):
                if (
                    not value
                    or any(isinstance(item, (dict, list)) for item in value)
                ):
                    raise ResearchCompilerError(
                        "query family lists must be finite scalar arrays"
                    )
                finite_dimension_seen = True
                cardinality *= len({str(item) for item in value})
                if cardinality > self.policy.max_template_expansion:
                    raise ResearchCompilerError(
                        "query family expansion exceeds policy"
                    )

        page_factor = 1
        if enumerator in {
            RegionEnumeratorKind.INTEGER_PAGINATION,
            RegionEnumeratorKind.CURSOR_API,
            RegionEnumeratorKind.HTML_CATALOG,
        }:
            page_factor = bounds.get(
                "max_pages", bounds.get("max_requests", 1)
            )
        elif enumerator is RegionEnumeratorKind.FILENAME_PATTERN:
            page_factor = bounds.get(
                "max_items", bounds.get("max_artifacts", 1)
            )
        computed_upper = cardinality * page_factor
        if computed_upper > self.policy.max_template_expansion:
            raise ResearchCompilerError(
                "compiled fanout exceeds policy"
            )
        if declared > self.policy.max_template_expansion:
            raise ResearchCompilerError(
                "declared fanout exceeds policy"
            )

        # Declared fanout is an estimate, never permission to exceed the
        # deterministic bound.  Use the tighter number when dimensions exist.
        if finite_dimension_seen or page_factor > 1:
            return min(declared, max(1, computed_upper))
        return declared

    @staticmethod
    def _small_region_exception(surface: RegionSurfaceKind) -> bool:
        return surface in {
            RegionSurfaceKind.HTML_CATALOG,
            RegionSurfaceKind.MANIFEST,
            RegionSurfaceKind.REPOSITORY,
        }

    @staticmethod
    def _region_key(
        root: str,
        query_family: Mapping[str, Any],
        enumerator: str,
    ) -> str:
        encoded = json.dumps(
            {
                "root": root,
                "query_family": dict(query_family),
                "enumerator": enumerator,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return "region:" + hashlib.sha256(encoded).hexdigest()
