"""Unified bounded LLM proposal protocol for Creeper research.

This module contains proposal-only value objects.  It deliberately has no
persistence, scheduler, evidence-store, or runtime-process dependencies.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping


class ProposalPlane(StrEnum):
    EXECUTION = "EXECUTION"
    RESEARCH = "RESEARCH"
    LEARNING = "LEARNING"


class ProposalType(StrEnum):
    EXPLORATION_REGION = "ExplorationRegionProposal"
    CONTRACT_FAMILY = "ContractFamilyProposal"
    QUERY_PROGRAM = "QueryProgramProposal"
    PIVOT_PROGRAM = "PivotProgramProposal"
    ROOT_SURFACE = "RootSurfaceProposal"
    QUERY_FAMILY = "QueryFamilyProposal"
    PIVOT_RULE = "PivotRuleProposal"
    NEGATIVE_RULE = "NegativeRuleProposal"
    FAMILY_RECOGNIZER = "FamilyRecognizerProposal"


class UnifiedLLMTask(StrEnum):
    # Execution plane.
    DISCOVER_NEW_SOURCE = "DISCOVER_NEW_SOURCE"
    EXPLOIT_SUCCESS_PATTERN = "EXPLOIT_SUCCESS_PATTERN"
    INTERPRET_STRUCTURE = "INTERPRET_STRUCTURE"
    RECOVER_STAGNATION = "RECOVER_STAGNATION"
    INTERPRET_EVIDENCE_CONTRACT = "INTERPRET_EVIDENCE_CONTRACT"

    # Research plane.
    COMPILE_ROOT_QUERY_PROGRAM = "COMPILE_ROOT_QUERY_PROGRAM"
    CLASSIFY_RESULT_CLUSTER = "CLASSIFY_RESULT_CLUSTER"
    COMPILE_PIVOT_PROGRAM = "COMPILE_PIVOT_PROGRAM"
    PROPOSE_NEW_ROOT = "PROPOSE_NEW_ROOT"
    RECOVER_ROOT_STAGNATION = "RECOVER_ROOT_STAGNATION"

    # Learning plane.
    DISTILL_SUCCESS_MOTIF = "DISTILL_SUCCESS_MOTIF"
    MUTATE_PRODUCTIVE_QUERY_FAMILY = "MUTATE_PRODUCTIVE_QUERY_FAMILY"
    DISTILL_NEGATIVE_CLUSTER = "DISTILL_NEGATIVE_CLUSTER"
    PROPOSE_ORTHOGONAL_ROOT = "PROPOSE_ORTHOGONAL_ROOT"
    INTERPRET_POLICY_DRIFT = "INTERPRET_POLICY_DRIFT"


TASK_ALLOWED_PROPOSALS: dict[UnifiedLLMTask, frozenset[ProposalType]] = {
    UnifiedLLMTask.DISCOVER_NEW_SOURCE: frozenset(
        {ProposalType.EXPLORATION_REGION, ProposalType.CONTRACT_FAMILY}
    ),
    UnifiedLLMTask.EXPLOIT_SUCCESS_PATTERN: frozenset(
        {ProposalType.EXPLORATION_REGION, ProposalType.CONTRACT_FAMILY}
    ),
    UnifiedLLMTask.INTERPRET_STRUCTURE: frozenset(
        {ProposalType.EXPLORATION_REGION, ProposalType.CONTRACT_FAMILY}
    ),
    UnifiedLLMTask.RECOVER_STAGNATION: frozenset(
        {ProposalType.EXPLORATION_REGION, ProposalType.CONTRACT_FAMILY}
    ),
    UnifiedLLMTask.INTERPRET_EVIDENCE_CONTRACT: frozenset(
        {ProposalType.CONTRACT_FAMILY, ProposalType.EXPLORATION_REGION}
    ),
    UnifiedLLMTask.COMPILE_ROOT_QUERY_PROGRAM: frozenset(
        {ProposalType.QUERY_PROGRAM}
    ),
    UnifiedLLMTask.CLASSIFY_RESULT_CLUSTER: frozenset(
        {ProposalType.CONTRACT_FAMILY, ProposalType.FAMILY_RECOGNIZER}
    ),
    UnifiedLLMTask.COMPILE_PIVOT_PROGRAM: frozenset(
        {ProposalType.PIVOT_PROGRAM}
    ),
    UnifiedLLMTask.PROPOSE_NEW_ROOT: frozenset(
        {ProposalType.ROOT_SURFACE}
    ),
    UnifiedLLMTask.RECOVER_ROOT_STAGNATION: frozenset(
        {ProposalType.QUERY_PROGRAM, ProposalType.PIVOT_PROGRAM, ProposalType.ROOT_SURFACE}
    ),
    UnifiedLLMTask.DISTILL_SUCCESS_MOTIF: frozenset(
        {ProposalType.QUERY_FAMILY, ProposalType.PIVOT_RULE, ProposalType.FAMILY_RECOGNIZER}
    ),
    UnifiedLLMTask.MUTATE_PRODUCTIVE_QUERY_FAMILY: frozenset(
        {ProposalType.QUERY_FAMILY}
    ),
    UnifiedLLMTask.DISTILL_NEGATIVE_CLUSTER: frozenset(
        {ProposalType.NEGATIVE_RULE, ProposalType.FAMILY_RECOGNIZER}
    ),
    UnifiedLLMTask.PROPOSE_ORTHOGONAL_ROOT: frozenset(
        {ProposalType.ROOT_SURFACE}
    ),
    UnifiedLLMTask.INTERPRET_POLICY_DRIFT: frozenset(
        {
            ProposalType.QUERY_FAMILY,
            ProposalType.PIVOT_RULE,
            ProposalType.NEGATIVE_RULE,
            ProposalType.FAMILY_RECOGNIZER,
        }
    ),
}


def stable_identity(prefix: str, payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return f"{prefix}:" + hashlib.sha256(encoded).hexdigest()


def _require_text(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _require_stops(values: tuple[str, ...]) -> tuple[str, ...]:
    if not values or any(not isinstance(item, str) or not item.strip() for item in values):
        raise ValueError("stop_conditions must contain non-empty strings")
    return tuple(dict.fromkeys(item.strip() for item in values))


def _require_bounds(bounds: Mapping[str, int]) -> Mapping[str, int]:
    if not bounds:
        raise ValueError("hard_bounds must be non-empty")
    for key, value in bounds.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError("hard_bounds keys must be non-empty strings")
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError("hard_bounds values must be positive integers")
    return bounds


@dataclass(frozen=True)
class RootQuery:
    query: str
    filters: Mapping[str, Any] = field(default_factory=dict)
    expected_signal: str = ""
    expected_family: str = ""
    max_pages: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "query", _require_text(self.query, "query"))
        if isinstance(self.max_pages, bool) or not isinstance(self.max_pages, int) or self.max_pages < 1:
            raise ValueError("max_pages must be a positive integer")

    @property
    def query_hash(self) -> str:
        return stable_identity(
            "query",
            {"query": self.query, "filters": dict(self.filters)},
        )


@dataclass(frozen=True)
class RootQueryProgram:
    """Compatibility value object consumed by the V7.1 deterministic adapters."""

    root_id: str
    strategy: str
    queries: tuple[RootQuery, ...]
    hard_max_requests: int
    stop_conditions: tuple[str, ...]
    compiler_version: str = "integrated-l6-v1"
    context_hash: str = ""
    program_id: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "root_id", _require_text(self.root_id, "root_id"))
        object.__setattr__(self, "strategy", _require_text(self.strategy, "strategy"))
        if not self.queries:
            raise ValueError("queries must be non-empty")
        if isinstance(self.hard_max_requests, bool) or not isinstance(self.hard_max_requests, int) or self.hard_max_requests < 1:
            raise ValueError("hard_max_requests must be positive")
        object.__setattr__(self, "stop_conditions", _require_stops(self.stop_conditions))
        if not self.program_id:
            object.__setattr__(
                self,
                "program_id",
                stable_identity(
                    "program",
                    {
                        "root_id": self.root_id,
                        "strategy": self.strategy,
                        "queries": [
                            {"query": q.query, "filters": dict(q.filters)}
                            for q in self.queries
                        ],
                        "context_hash": self.context_hash,
                    },
                ),
            )


@dataclass(frozen=True)
class ExplorationRegionProposal:
    proposal_id: str
    surface_kind: str
    root: str
    purpose: str
    query_family: Mapping[str, Any]
    enumerator: str
    artifact_predicate: Mapping[str, Any]
    hard_bounds: Mapping[str, int]
    stop_conditions: tuple[str, ...]
    expected_source_family: str
    expected_contract_family: str
    expected_mechanism: str
    expected_fanout: int
    reuse_key: str
    confidence: float
    validation: Mapping[str, Any]

    proposal_type: ProposalType = field(
        default=ProposalType.EXPLORATION_REGION, init=False
    )

    def __post_init__(self) -> None:
        for name in (
            "proposal_id", "surface_kind", "root", "purpose", "enumerator",
            "expected_source_family", "expected_contract_family",
            "expected_mechanism", "reuse_key",
        ):
            object.__setattr__(self, name, _require_text(getattr(self, name), name))
        _require_bounds(self.hard_bounds)
        object.__setattr__(self, "stop_conditions", _require_stops(self.stop_conditions))
        if isinstance(self.expected_fanout, bool) or not isinstance(self.expected_fanout, int) or self.expected_fanout < 1:
            raise ValueError("expected_fanout must be a positive integer")
        if isinstance(self.confidence, bool) or not isinstance(self.confidence, (int, float)) or not 0 <= self.confidence <= 1:
            raise ValueError("confidence must be between 0 and 1")


@dataclass(frozen=True)
class ContractFamilyProposal:
    proposal_id: str
    contract_family: str
    recognition: Mapping[str, Any]
    extraction: Mapping[str, Any]
    hard_bounds: Mapping[str, int]
    stop_conditions: tuple[str, ...]
    expected_mechanism: str
    reuse_key: str
    confidence: float
    validation: Mapping[str, Any]

    proposal_type: ProposalType = field(
        default=ProposalType.CONTRACT_FAMILY, init=False
    )

    def __post_init__(self) -> None:
        for name in ("proposal_id", "contract_family", "expected_mechanism", "reuse_key"):
            object.__setattr__(self, name, _require_text(getattr(self, name), name))
        _require_bounds(self.hard_bounds)
        object.__setattr__(self, "stop_conditions", _require_stops(self.stop_conditions))
        if isinstance(self.confidence, bool) or not isinstance(self.confidence, (int, float)) or not 0 <= self.confidence <= 1:
            raise ValueError("confidence must be between 0 and 1")


@dataclass(frozen=True)
class QueryProgramProposal:
    proposal_id: str
    root_id: str
    strategy: str
    queries: tuple[RootQuery, ...]
    hard_max_requests: int
    stop_conditions: tuple[str, ...]
    reuse_key: str
    context_hash: str = ""

    proposal_type: ProposalType = field(
        default=ProposalType.QUERY_PROGRAM, init=False
    )

    def __post_init__(self) -> None:
        for name in ("proposal_id", "root_id", "strategy", "reuse_key"):
            object.__setattr__(self, name, _require_text(getattr(self, name), name))
        if not self.queries:
            raise ValueError("queries must be non-empty")
        if isinstance(self.hard_max_requests, bool) or not isinstance(self.hard_max_requests, int) or self.hard_max_requests < 1:
            raise ValueError("hard_max_requests must be positive")
        object.__setattr__(self, "stop_conditions", _require_stops(self.stop_conditions))

    @property
    def program_id(self) -> str:
        return stable_identity(
            "program",
            {
                "root_id": self.root_id,
                "strategy": self.strategy,
                "queries": [
                    {"query": q.query, "filters": dict(q.filters)} for q in self.queries
                ],
                "reuse_key": self.reuse_key,
                "context_hash": self.context_hash,
            },
        )


@dataclass(frozen=True)
class PivotProgramProposal(QueryProgramProposal):
    proposal_type: ProposalType = field(
        default=ProposalType.PIVOT_PROGRAM, init=False
    )


@dataclass(frozen=True)
class RootSurfaceProposal:
    proposal_id: str
    kind: str
    entrypoint: str
    capabilities: tuple[str, ...]
    rationale: str
    hard_bounds: Mapping[str, int]
    stop_conditions: tuple[str, ...]
    reuse_key: str
    confidence: float

    proposal_type: ProposalType = field(
        default=ProposalType.ROOT_SURFACE, init=False
    )

    def __post_init__(self) -> None:
        for name in ("proposal_id", "kind", "entrypoint", "rationale", "reuse_key"):
            object.__setattr__(self, name, _require_text(getattr(self, name), name))
        if not self.capabilities or any(not isinstance(x, str) or not x.strip() for x in self.capabilities):
            raise ValueError("capabilities must be non-empty strings")
        _require_bounds(self.hard_bounds)
        object.__setattr__(self, "stop_conditions", _require_stops(self.stop_conditions))
        if isinstance(self.confidence, bool) or not isinstance(self.confidence, (int, float)) or not 0 <= self.confidence <= 1:
            raise ValueError("confidence must be between 0 and 1")


@dataclass(frozen=True)
class _LearningRule:
    proposal_id: str
    generalization_scope: Mapping[str, Any]
    preconditions: Mapping[str, Any]
    bounded_expansion: Mapping[str, Any]
    hard_bounds: Mapping[str, int]
    stop_conditions: tuple[str, ...]
    expected_mechanism: str
    failure_modes: tuple[str, ...]
    reuse_key: str
    confidence: float

    def __post_init__(self) -> None:
        for name in ("proposal_id", "expected_mechanism", "reuse_key"):
            object.__setattr__(self, name, _require_text(getattr(self, name), name))
        _require_bounds(self.hard_bounds)
        object.__setattr__(self, "stop_conditions", _require_stops(self.stop_conditions))
        if not self.failure_modes or any(not isinstance(x, str) or not x.strip() for x in self.failure_modes):
            raise ValueError("failure_modes must contain non-empty strings")
        if isinstance(self.confidence, bool) or not isinstance(self.confidence, (int, float)) or not 0 <= self.confidence <= 1:
            raise ValueError("confidence must be between 0 and 1")


@dataclass(frozen=True)
class QueryFamilyProposal(_LearningRule):
    proposal_type: ProposalType = field(default=ProposalType.QUERY_FAMILY, init=False)


@dataclass(frozen=True)
class PivotRuleProposal(_LearningRule):
    proposal_type: ProposalType = field(default=ProposalType.PIVOT_RULE, init=False)


@dataclass(frozen=True)
class NegativeRuleProposal(_LearningRule):
    proposal_type: ProposalType = field(default=ProposalType.NEGATIVE_RULE, init=False)


@dataclass(frozen=True)
class FamilyRecognizerProposal(_LearningRule):
    proposal_type: ProposalType = field(default=ProposalType.FAMILY_RECOGNIZER, init=False)


UnifiedProposal = (
    ExplorationRegionProposal
    | ContractFamilyProposal
    | QueryProgramProposal
    | PivotProgramProposal
    | RootSurfaceProposal
    | QueryFamilyProposal
    | PivotRuleProposal
    | NegativeRuleProposal
    | FamilyRecognizerProposal
)


@dataclass(frozen=True)
class ProposalEnvelope:
    query: str
    task_type: UnifiedLLMTask
    context_hash: str
    proposals: tuple[UnifiedProposal, ...]

    contract: str = "creeper.llm-research-compiler.v1"

    def __post_init__(self) -> None:
        object.__setattr__(self, "query", _require_text(self.query, "query"))
        if not isinstance(self.context_hash, str):
            raise ValueError("context_hash must be a string")
        allowed = TASK_ALLOWED_PROPOSALS[self.task_type]
        invalid = [item.proposal_type for item in self.proposals if item.proposal_type not in allowed]
        if invalid:
            raise ValueError(
                f"task {self.task_type.value} cannot emit proposal types "
                + ", ".join(item.value for item in invalid)
            )
