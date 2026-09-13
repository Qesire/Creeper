"""Deterministic gate for bounded background research scheduling.

The gate is deliberately side-effect free.  It observes a parent-computed
snapshot and returns at most one bounded directive.  Durable episode creation,
child execution, proposal validation, and all authority-bearing commits remain
outside this module.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ResearchTriggerReason(StrEnum):
    FRONTIER_EXHAUSTED = "FRONTIER_EXHAUSTED"
    READY_INVENTORY_LOW = "READY_INVENTORY_LOW"
    SUSTAINED_FINAL_YIELD_COLLAPSE = "SUSTAINED_FINAL_YIELD_COLLAPSE"
    UNKNOWN_STRUCTURE_FAMILY = "UNKNOWN_STRUCTURE_FAMILY"
    UNKNOWN_CONTRACT_FAMILY = "UNKNOWN_CONTRACT_FAMILY"
    EXPLICIT_OPERATOR_REQUEST = "EXPLICIT_OPERATOR_REQUEST"


@dataclass(frozen=True)
class ResearchTriggerSnapshot:
    """Scheduler facts only; never raw records or authority handles."""

    ready_minutes: float | None = None
    executable_regions: int = 0
    pending_region_count: int = 0
    deterministic_candidate_backlog: int = 0
    deterministic_refill_available: bool = False
    productive_direct_inventory: int = 0
    final_eed_per_hour_15m: float | None = None
    final_eed_per_hour_60m: float | None = None
    recent_zero_reward_tail: int = 0
    closed_source_runs: int = 0
    unknown_structure_blockers: int = 0
    unknown_contract_blockers: int = 0
    active_llm_episode_id: str | None = None
    active_llm_episode_context: str | None = None
    last_llm_started_at: float | None = None
    same_context_failures: int = 0
    context_hash: str = ""
    subject: str | None = None
    operator_requested: bool = False
    now: float | None = None

    def __post_init__(self) -> None:
        counts = (
            self.executable_regions,
            self.pending_region_count,
            self.deterministic_candidate_backlog,
            self.productive_direct_inventory,
            self.recent_zero_reward_tail,
            self.closed_source_runs,
            self.unknown_structure_blockers,
            self.unknown_contract_blockers,
            self.same_context_failures,
        )
        if any(not isinstance(value, int) or value < 0 for value in counts):
            raise ValueError("research trigger counts must be non-negative integers")
        if self.ready_minutes is not None and self.ready_minutes < 0:
            raise ValueError("ready_minutes must be non-negative")
        for name in ("final_eed_per_hour_15m", "final_eed_per_hour_60m"):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"{name} must be non-negative")


@dataclass(frozen=True)
class ResearchTriggerDecision:
    allow: bool
    reason: ResearchTriggerReason | None = None
    task_type: str | None = None
    subject: str | None = None
    explanation: str = ""
    context_key: str = ""
    desired_regions: int = 0


@dataclass(frozen=True)
class ResearchDirective:
    """One bounded parent-to-child scheduling instruction."""

    task_type: str
    trigger_reason: ResearchTriggerReason
    strategy: str
    subject: str | None
    desired_regions: int
    reason: str
    context_key: str

    def __post_init__(self) -> None:
        if self.desired_regions != 1:
            raise ValueError("L8 permits at most one research directive per observation")
        if not self.task_type.strip() or not self.strategy.strip():
            raise ValueError("research directive identity is required")


@dataclass(frozen=True)
class ResearchTriggerGate:
    min_seconds_between_llm_starts: float = 120.0
    same_context_failure_cooldown_seconds: float = 600.0
    ready_minutes_threshold: float = 30.0
    stagnation_min_closed_runs: int = 3
    stagnation_zero_tail: int = 3
    stagnation_yield_fraction: float = 0.25

    def __post_init__(self) -> None:
        if self.min_seconds_between_llm_starts < 0:
            raise ValueError("min_seconds_between_llm_starts must be non-negative")
        if self.same_context_failure_cooldown_seconds < 0:
            raise ValueError(
                "same_context_failure_cooldown_seconds must be non-negative"
            )
        if self.ready_minutes_threshold <= 0:
            raise ValueError("ready_minutes_threshold must be positive")
        if self.stagnation_min_closed_runs < 2:
            raise ValueError("stagnation_min_closed_runs must be at least two")
        if self.stagnation_zero_tail < 2:
            raise ValueError("stagnation_zero_tail must be at least two")
        if not 0 < self.stagnation_yield_fraction <= 1:
            raise ValueError("stagnation_yield_fraction must be within (0, 1]")

    @staticmethod
    def _context_key(snapshot: ResearchTriggerSnapshot) -> str:
        return snapshot.context_hash or "default"

    def _cooled_down(self, snapshot: ResearchTriggerSnapshot) -> bool:
        if snapshot.last_llm_started_at is None or snapshot.now is None:
            return True
        elapsed = snapshot.now - snapshot.last_llm_started_at
        if elapsed < 0:
            return False
        interval = (
            self.same_context_failure_cooldown_seconds
            if snapshot.same_context_failures > 0
            else self.min_seconds_between_llm_starts
        )
        return elapsed >= interval

    def _decision(
        self,
        snapshot: ResearchTriggerSnapshot,
        reason: ResearchTriggerReason,
        task_type: str,
        explanation: str,
    ) -> ResearchTriggerDecision:
        return ResearchTriggerDecision(
            allow=True,
            reason=reason,
            task_type=task_type,
            subject=snapshot.subject,
            explanation=explanation,
            context_key=self._context_key(snapshot),
            desired_regions=1,
        )

    def _cooldown_block(self, snapshot: ResearchTriggerSnapshot) -> ResearchTriggerDecision:
        return ResearchTriggerDecision(
            allow=False,
            explanation="global or same-context LLM cooldown is active",
            context_key=self._context_key(snapshot),
        )

    def decide(self, snapshot: ResearchTriggerSnapshot) -> ResearchTriggerDecision:
        """Apply the integrated L8 trigger precedence without side effects."""

        # 1. Active-call guard.
        if snapshot.active_llm_episode_id is not None:
            return ResearchTriggerDecision(
                allow=False,
                explanation="an LLM research episode is already active",
                context_key=self._context_key(snapshot),
            )

        # 2. High-value structure/contract blockers may bypass unrelated
        # deterministic frontier suppression, but never cooldown or concurrency.
        if snapshot.unknown_contract_blockers:
            if not self._cooled_down(snapshot):
                return self._cooldown_block(snapshot)
            return self._decision(
                snapshot,
                ResearchTriggerReason.UNKNOWN_CONTRACT_FAMILY,
                "INTERPRET_EVIDENCE_CONTRACT",
                "a high-value source has unknown evidence semantics",
            )
        if snapshot.unknown_structure_blockers:
            if not self._cooled_down(snapshot):
                return self._cooldown_block(snapshot)
            return self._decision(
                snapshot,
                ResearchTriggerReason.UNKNOWN_STRUCTURE_FAMILY,
                "INTERPRET_STRUCTURE",
                "a high-value source has unknown reusable structure",
            )

        # 3. Ordinary LLM discovery is suppressed while deterministic work can
        # still supply the pipeline.
        if (
            snapshot.executable_regions > 0
            or snapshot.pending_region_count > 0
            or snapshot.deterministic_candidate_backlog > 0
            or snapshot.deterministic_refill_available
        ):
            return ResearchTriggerDecision(
                allow=False,
                explanation="deterministic work can still supply the pipeline",
                context_key=self._context_key(snapshot),
            )

        if not self._cooled_down(snapshot):
            return self._cooldown_block(snapshot)

        # 4. Ready-inventory starvation.
        low_inventory = (
            snapshot.ready_minutes is not None
            and snapshot.ready_minutes < self.ready_minutes_threshold
        )
        if low_inventory:
            task = (
                "EXPLOIT_SUCCESS_PATTERN"
                if snapshot.productive_direct_inventory > 0
                else "DISCOVER_NEW_SOURCE"
            )
            return self._decision(
                snapshot,
                ResearchTriggerReason.READY_INVENTORY_LOW,
                task,
                "ready inventory is below the research threshold",
            )

        # 5. Fully exhausted deterministic frontier with no ready inventory.
        frontier_exhausted = (
            snapshot.ready_minutes in (None, 0.0)
            and snapshot.executable_regions == 0
            and snapshot.pending_region_count == 0
            and snapshot.deterministic_candidate_backlog == 0
            and not snapshot.deterministic_refill_available
        )
        if frontier_exhausted:
            task = (
                "EXPLOIT_SUCCESS_PATTERN"
                if snapshot.productive_direct_inventory > 0
                else "DISCOVER_NEW_SOURCE"
            )
            return self._decision(
                snapshot,
                ResearchTriggerReason.FRONTIER_EXHAUSTED,
                task,
                "no executable deterministic frontier remains",
            )

        # 6. Sustained FINAL-yield collapse.  Proxy hit/page counts are
        # intentionally absent from this decision.
        sustained_collapse = (
            snapshot.closed_source_runs >= self.stagnation_min_closed_runs
            and snapshot.recent_zero_reward_tail >= self.stagnation_zero_tail
            and snapshot.final_eed_per_hour_15m is not None
            and snapshot.final_eed_per_hour_60m is not None
            and snapshot.final_eed_per_hour_15m
            <= self.stagnation_yield_fraction
            * max(snapshot.final_eed_per_hour_60m, 1e-12)
        )
        if sustained_collapse:
            return self._decision(
                snapshot,
                ResearchTriggerReason.SUSTAINED_FINAL_YIELD_COLLAPSE,
                "RECOVER_STAGNATION",
                "recent FINAL yield rate collapsed below its 60-minute baseline",
            )

        # 7. Explicit operator request is still bounded and does not override
        # deterministic-first scheduling.
        if snapshot.operator_requested:
            return self._decision(
                snapshot,
                ResearchTriggerReason.EXPLICIT_OPERATOR_REQUEST,
                "DISCOVER_NEW_SOURCE",
                "operator requested one bounded research directive",
            )

        return ResearchTriggerDecision(
            allow=False,
            explanation="no integrated research trigger is currently justified",
            context_key=self._context_key(snapshot),
        )
