"""Hierarchical adaptive scheduler over the L3 durable research kernel."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol, Sequence

from .bandit import BanditStats
from .decision_log import persist_decision
from .policy import (
    ActionCandidate,
    HierarchicalAdaptivePolicy,
    PolicyDecision,
    PolicyLevel,
)
from .saturation import SaturationTracker


@dataclass(frozen=True)
class RegistryPolicySnapshot:
    snapshot_id: str
    policy_version: str
    schema_version: int
    parameters: Mapping[str, Any]
    created_at: float
    active: bool = False


class AdaptiveRegistry(Protocol):
    def record_decision(self, decision: object) -> bool: ...
    def upsert_policy_snapshot(self, snapshot: object) -> None: ...
    def arm_stats(self, *, policy_version: str) -> Sequence[object]: ...
    def rebuild_arm_stats(self, *, policy_version: str, schema_version: int) -> Sequence[object]: ...


class AdaptiveResearchScheduler:
    """Allocate budget one hierarchy level at a time; never flatten the action universe."""

    def __init__(
        self,
        *,
        policy: HierarchicalAdaptivePolicy,
        registry: AdaptiveRegistry,
        saturation: SaturationTracker | None = None,
    ) -> None:
        self.policy = policy
        self.registry = registry
        self.saturation = saturation

    def persist_snapshot(self, *, created_at: float, active: bool = False) -> None:
        self.registry.upsert_policy_snapshot(
            RegistryPolicySnapshot(
                snapshot_id=self.policy.config.snapshot_id,
                policy_version=self.policy.config.version,
                schema_version=self.policy.config.schema_version,
                parameters=self.policy.snapshot_parameters(),
                created_at=created_at,
                active=active,
            )
        )

    def current_stats(self) -> dict[str, BanditStats]:
        return {
            item.arm_id: BanditStats.from_object(item)
            for item in self.registry.arm_stats(policy_version=self.policy.config.version)
        }

    def rebuild_derived_stats(self) -> dict[str, BanditStats]:
        rows = self.registry.rebuild_arm_stats(
            policy_version=self.policy.config.version,
            schema_version=self.policy.config.schema_version,
        )
        return {item.arm_id: BanditStats.from_object(item) for item in rows}

    def choose(
        self,
        *,
        task_id: str,
        level: PolicyLevel | str,
        candidates: Sequence[ActionCandidate],
        context_features: Mapping[str, Any],
        timestamp: float,
        selection_nonce: str = "",
        stats_by_arm: Mapping[str, BanditStats | object] | None = None,
    ) -> PolicyDecision:
        level = PolicyLevel(level)
        filtered = list(candidates)
        if self.saturation is not None and level is PolicyLevel.ROOT:
            filtered = [
                candidate
                for candidate in filtered
                if self.saturation.eligible(candidate.action_id, now=timestamp)
            ]
        decision = self.policy.select(
            task_id=task_id,
            level=level,
            candidates=filtered,
            stats_by_arm=stats_by_arm or self.current_stats(),
            context_features=context_features,
            timestamp=timestamp,
            selection_nonce=selection_nonce,
        )
        persist_decision(self.registry, decision)
        return decision

    def choose_path(
        self,
        *,
        task_id: str,
        hierarchy: Sequence[tuple[PolicyLevel | str, Sequence[ActionCandidate]]],
        context_features: Mapping[str, Any],
        timestamp: float,
        selection_nonce: str = "",
        stats_by_arm: Mapping[str, BanditStats | object] | None = None,
    ) -> tuple[PolicyDecision, ...]:
        """Make explicit L0→L3 decisions, logging a propensity at each level."""
        decisions: list[PolicyDecision] = []
        parent: list[str] = []
        parent_decision_ids: list[str] = []
        for index, (level, candidates) in enumerate(hierarchy):
            context = dict(context_features)
            context["parent_path"] = tuple(parent)
            context["parent_decision_ids"] = tuple(parent_decision_ids)
            decision = self.choose(
                task_id=task_id,
                level=level,
                candidates=candidates,
                context_features=context,
                timestamp=timestamp,
                selection_nonce=f"{selection_nonce}:{index}",
                stats_by_arm=stats_by_arm,
            )
            decisions.append(decision)
            parent.append(decision.chosen_action_id)
            parent_decision_ids.append(decision.decision_id)
        return tuple(decisions)


__all__ = ["AdaptiveRegistry", "AdaptiveResearchScheduler", "RegistryPolicySnapshot"]
