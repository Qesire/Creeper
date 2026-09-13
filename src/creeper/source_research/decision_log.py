"""Decision-log adapters. All adaptive choices carry replay propensities."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Protocol

from .policy import ExplorationMode, PolicyDecision, PolicyLevel


@dataclass(frozen=True)
class RegistryDecisionRecord:
    """Structural adapter matching L3 ResearchRegistry.record_decision."""

    decision_id: str
    task_id: str
    arm_id: str
    policy_version: str
    policy_snapshot_id: str
    propensity: float
    context_hash: str
    chosen_at: float
    metadata: Mapping[str, Any] = field(default_factory=dict)


class DecisionRegistry(Protocol):
    def record_decision(self, decision: object) -> bool: ...


@dataclass(frozen=True)
class LoggedDecision:
    decision_id: str
    task_id: str
    policy_id: str
    policy_version: str
    policy_snapshot_id: str
    level: PolicyLevel
    context_features: Mapping[str, Any]
    context_hash: str
    candidate_action_ids: tuple[str, ...]
    chosen_action_id: str
    propensity: float
    exploration_mode: ExplorationMode
    timestamp: float
    probabilities: Mapping[str, float] = field(default_factory=dict)
    baseline_version: str = ""
    selection_nonce: str = ""


def _metadata(decision: PolicyDecision) -> dict[str, Any]:
    return {
        "policy_id": decision.policy_id,
        "level": decision.level.value,
        "context_features": dict(decision.context_features),
        "candidate_action_ids": list(decision.candidate_action_ids),
        "chosen_action": decision.chosen_action_id,
        "chosen_probability": decision.chosen_probability,
        "probabilities": dict(decision.probabilities),
        "scores": dict(decision.scores),
        "exploration_mode": decision.exploration_mode.value,
        "baseline_version": decision.baseline_version,
        "selection_nonce": decision.selection_nonce,
    }


def to_registry_record(decision: PolicyDecision) -> RegistryDecisionRecord:
    return RegistryDecisionRecord(
        decision_id=decision.decision_id,
        task_id=decision.task_id,
        arm_id=decision.chosen_action_id,
        policy_version=decision.policy_version,
        policy_snapshot_id=decision.policy_snapshot_id,
        propensity=decision.chosen_probability,
        context_hash=decision.context_hash,
        chosen_at=decision.timestamp,
        metadata=_metadata(decision),
    )


def persist_decision(registry: DecisionRegistry, decision: PolicyDecision) -> bool:
    return bool(registry.record_decision(to_registry_record(decision)))


def logged_decision_from_mapping(row: Mapping[str, Any]) -> LoggedDecision:
    raw_meta = row.get("metadata", row.get("metadata_json", {}))
    if isinstance(raw_meta, str):
        metadata = json.loads(raw_meta or "{}")
    else:
        metadata = dict(raw_meta or {})
    candidates = tuple(
        str(value) for value in metadata.get("candidate_action_ids", ())
    )
    chosen = str(row.get("arm_id") or metadata.get("chosen_action") or "")
    if not candidates and chosen:
        candidates = (chosen,)
    return LoggedDecision(
        decision_id=str(row["decision_id"]),
        task_id=str(row.get("task_id", "")),
        policy_id=str(metadata.get("policy_id", "")),
        policy_version=str(row.get("policy_version", "")),
        policy_snapshot_id=str(row.get("policy_snapshot_id", "")),
        level=PolicyLevel(metadata.get("level", PolicyLevel.ROOT.value)),
        context_features=dict(metadata.get("context_features", {})),
        context_hash=str(row.get("context_hash", "")),
        candidate_action_ids=candidates,
        chosen_action_id=chosen,
        propensity=float(row.get("propensity", metadata.get("chosen_probability", 0.0))),
        exploration_mode=ExplorationMode(
            metadata.get("exploration_mode", ExplorationMode.EXPLOIT.value)
        ),
        timestamp=float(row.get("chosen_at", row.get("timestamp", 0.0))),
        probabilities=dict(metadata.get("probabilities", {})),
        baseline_version=str(metadata.get("baseline_version", "")),
        selection_nonce=str(metadata.get("selection_nonce", "")),
    )


def logged_decisions_from_rows(rows: Iterable[Mapping[str, Any]]) -> tuple[LoggedDecision, ...]:
    return tuple(logged_decision_from_mapping(row) for row in rows)


class MemoryDecisionLog:
    def __init__(self) -> None:
        self._events: dict[str, LoggedDecision] = {}

    def append(self, decision: PolicyDecision) -> bool:
        if decision.decision_id in self._events:
            return False
        self._events[decision.decision_id] = LoggedDecision(
            decision_id=decision.decision_id,
            task_id=decision.task_id,
            policy_id=decision.policy_id,
            policy_version=decision.policy_version,
            policy_snapshot_id=decision.policy_snapshot_id,
            level=decision.level,
            context_features=dict(decision.context_features),
            context_hash=decision.context_hash,
            candidate_action_ids=decision.candidate_action_ids,
            chosen_action_id=decision.chosen_action_id,
            propensity=decision.chosen_probability,
            exploration_mode=decision.exploration_mode,
            timestamp=decision.timestamp,
            probabilities=dict(decision.probabilities),
            baseline_version=decision.baseline_version,
            selection_nonce=decision.selection_nonce,
        )
        return True

    def events(self) -> tuple[LoggedDecision, ...]:
        return tuple(self._events[key] for key in sorted(self._events))


__all__ = [
    "DecisionRegistry",
    "LoggedDecision",
    "MemoryDecisionLog",
    "RegistryDecisionRecord",
    "logged_decision_from_mapping",
    "logged_decisions_from_rows",
    "persist_decision",
    "to_registry_record",
]
