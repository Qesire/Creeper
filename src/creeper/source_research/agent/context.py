"""Bounded context objects for the unified LLM research compiler.

Contexts contain compact metadata only.  They never carry hit bodies, cursors,
artifact payloads, baseline paths, database paths, or evidence authority.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Mapping

from .protocol import ProposalPlane, UnifiedLLMTask


_MAX_CONTEXT_ITEMS = 64


def _bounded_strings(values: tuple[str, ...], name: str) -> tuple[str, ...]:
    if len(values) > _MAX_CONTEXT_ITEMS:
        raise ValueError(f"{name} exceeds {_MAX_CONTEXT_ITEMS} items")
    if any(not isinstance(item, str) or not item.strip() for item in values):
        raise ValueError(f"{name} must contain non-empty strings")
    return tuple(item.strip() for item in values)


@dataclass(frozen=True)
class ResearchCompilerContext:
    """Stable research-root context, compatible with the V7.1 compiler surface."""

    root_id: str
    root_capabilities: tuple[str, ...]
    seed_current_program_exhausted: bool
    equivalent_unexecuted_program: bool
    cooldown_satisfied: bool
    deterministic_seed_search_available: bool = False
    metrics_available: bool = False
    recent_query_hashes: tuple[str, ...] = ()
    unclassified_clusters: tuple[str, ...] = ()
    productive_source_families: tuple[str, ...] = ()
    saturated_source_families: tuple[str, ...] = ()
    negative_knowledge: tuple[str, ...] = ()
    baseline_scale: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.root_id, str) or not self.root_id.strip():
            raise ValueError("root_id is required")
        object.__setattr__(
            self, "root_capabilities",
            _bounded_strings(self.root_capabilities, "root_capabilities"),
        )
        for name in (
            "recent_query_hashes",
            "unclassified_clusters",
            "productive_source_families",
            "saturated_source_families",
            "negative_knowledge",
        ):
            object.__setattr__(
                self, name, _bounded_strings(getattr(self, name), name)
            )
        if len(self.baseline_scale) > 16:
            raise ValueError("baseline_scale is too large")
        if any(
            not isinstance(key, str)
            or not key.strip()
            or not isinstance(value, str)
            or not value.strip()
            for key, value in self.baseline_scale
        ):
            raise ValueError("baseline_scale must contain non-empty string pairs")

    def as_prompt_payload(self) -> dict[str, object]:
        return {
            "root_id": self.root_id.strip(),
            "root_capabilities": list(self.root_capabilities),
            "seed_current_program_exhausted": self.seed_current_program_exhausted,
            "equivalent_unexecuted_program": self.equivalent_unexecuted_program,
            "cooldown_satisfied": self.cooldown_satisfied,
            "deterministic_seed_search_available": self.deterministic_seed_search_available,
            "metrics_available": self.metrics_available,
            "recent_query_hashes": list(self.recent_query_hashes),
            "unclassified_clusters": list(self.unclassified_clusters),
            "productive_source_families": list(self.productive_source_families),
            "saturated_source_families": list(self.saturated_source_families),
            "negative_knowledge": list(self.negative_knowledge),
            "baseline_scale": dict(self.baseline_scale),
            "authority": "proposal_only",
        }

    @property
    def context_hash(self) -> str:
        return digest_context(self.as_prompt_payload())


@dataclass(frozen=True)
class LearningCompilerContext:
    """Compact delayed-reward batch summary used only at explicit learning epochs."""

    learning_epoch_ready: bool
    minimum_batch_satisfied: bool
    replay_available: bool
    final_reward_available: bool
    policy_snapshot_id: str
    lineage_available: bool = False
    rule_persistence_available: bool = False
    successful_reuse_keys: tuple[str, ...] = ()
    failed_reuse_keys: tuple[str, ...] = ()
    productive_families: tuple[str, ...] = ()
    saturated_families: tuple[str, ...] = ()
    drift_signals: tuple[str, ...] = ()
    reward_summary: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.policy_snapshot_id, str) or not self.policy_snapshot_id.strip():
            raise ValueError("policy_snapshot_id is required")
        for name in (
            "successful_reuse_keys",
            "failed_reuse_keys",
            "productive_families",
            "saturated_families",
            "drift_signals",
        ):
            object.__setattr__(
                self, name, _bounded_strings(getattr(self, name), name)
            )
        if len(self.reward_summary) > 32:
            raise ValueError("reward_summary is too large")
        if any(
            not isinstance(key, str)
            or not key.strip()
            or not isinstance(value, str)
            or not value.strip()
            for key, value in self.reward_summary
        ):
            raise ValueError("reward_summary must contain non-empty string pairs")

    def as_prompt_payload(self) -> dict[str, object]:
        return {
            "learning_epoch_ready": self.learning_epoch_ready,
            "minimum_batch_satisfied": self.minimum_batch_satisfied,
            "replay_available": self.replay_available,
            "final_reward_available": self.final_reward_available,
            "policy_snapshot_id": self.policy_snapshot_id.strip(),
            "lineage_available": self.lineage_available,
            "rule_persistence_available": self.rule_persistence_available,
            "successful_reuse_keys": list(self.successful_reuse_keys),
            "failed_reuse_keys": list(self.failed_reuse_keys),
            "productive_families": list(self.productive_families),
            "saturated_families": list(self.saturated_families),
            "drift_signals": list(self.drift_signals),
            "reward_summary": dict(self.reward_summary),
            "authority": "proposal_only",
            "reward_authority": "FINAL_delayed_reward_only",
        }

    @property
    def context_hash(self) -> str:
        return digest_context(self.as_prompt_payload())


@dataclass(frozen=True)
class UnifiedCompilerRequest:
    """Serializable parent-to-child request for all new L6 slow-intelligence roles."""

    task_type: UnifiedLLMTask
    plane: ProposalPlane
    trigger_reason: str
    objective: str
    context: Mapping[str, Any]
    context_hash: str
    hard_limits: Mapping[str, int] = field(
        default_factory=lambda: {
            "max_proposals": 8,
            "max_requests_per_program": 80,
            "max_template_expansion": 4096,
        }
    )
    contract: str = "creeper.llm-research-compiler.v1"
    prompt_version: str = "unified-research-compiler-v1"

    def __post_init__(self) -> None:
        if not isinstance(self.trigger_reason, str) or not self.trigger_reason.strip():
            raise ValueError("trigger_reason is required")
        if not isinstance(self.objective, str) or not self.objective.strip():
            raise ValueError("objective is required")
        if not isinstance(self.context_hash, str):
            raise ValueError("context_hash must be a string")
        if not self.hard_limits or any(
            not isinstance(key, str)
            or isinstance(value, bool)
            or not isinstance(value, int)
            or value < 1
            for key, value in self.hard_limits.items()
        ):
            raise ValueError("hard_limits must be positive integer bounds")

    def as_payload(self) -> dict[str, object]:
        return {
            "contract": self.contract,
            "prompt_version": self.prompt_version,
            "execution": {
                "authority": "proposal_only",
                "mode": "RESEARCH_COMPILER",
            },
            "task": {
                "task_type": self.task_type.value,
                "plane": self.plane.value,
                "trigger_reason": self.trigger_reason.strip(),
                "objective": self.objective.strip(),
            },
            "context_hash": self.context_hash,
            "context": dict(self.context),
            "hard_limits": dict(self.hard_limits),
            "constraints": {
                "target_year_from": 1996,
                "target_year_to": 2001,
                "common_crawl_active_corpus_excluded": True,
                "evidence_authority": False,
                "submission_authority": False,
                "scheduler_authority": False,
                "durable_state_authority": False,
            },
        }


def digest_context(context: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(context),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
