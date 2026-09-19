"""Pure inventory planning for source discovery and source-reservoir refill."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import math

from creeper.source_discovery.models import (
    SourceCandidate,
    SourceLevel,
    SourceState,
    is_background_bulk_candidate,
)
from creeper.source_discovery.overlap import MinHashSketch
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.residual_search import QueryPlan, SearchCellScheduler
from creeper.source_discovery.value import InterpretableSourceValueModel
from creeper.source_discovery.unknown_format import parse_unknown_format_reason


class SearchDirectiveKind(StrEnum):
    DIRECT_EVIDENCE = "DIRECT_EVIDENCE"
    REFILL_RESERVOIR = "REFILL_RESERVOIR"
    EXPLOIT_SOURCE_FAMILY = "EXPLOIT_SOURCE_FAMILY"
    DISCOVER_NEW_FAMILY = "DISCOVER_NEW_FAMILY"
    INTERPRET_STRUCTURE = "INTERPRET_STRUCTURE"
    UNKNOWN_FORMAT = "UNKNOWN_FORMAT"
    RECOVER_STAGNATION = "RECOVER_STAGNATION"


class SourceIntelligenceTask(StrEnum):
    """Finite Codex subagent roles requested by the deterministic parent."""

    DISCOVER_NEW_SOURCE = "DISCOVER_NEW_SOURCE"
    EXPLOIT_SUCCESS_PATTERN = "EXPLOIT_SUCCESS_PATTERN"
    INTERPRET_STRUCTURE = "INTERPRET_STRUCTURE"
    COMPILE_ADAPTER = "COMPILE_ADAPTER"
    RECOVER_STAGNATION = "RECOVER_STAGNATION"


@dataclass(frozen=True)
class SourcePoolTargets:
    active_min: int = 2
    active_target: int = 3
    warm_min: int = 10
    warm_target: int = 20
    cold_min: int = 50
    cold_target: int = 100
    max_cold_credit_per_origin: int = 12
    triage_batch: int = 16
    scout_parallelism: int = 4
    max_search_directives: int = 2

    def __post_init__(self) -> None:
        values = (
            self.active_min,
            self.active_target,
            self.warm_min,
            self.warm_target,
            self.cold_min,
            self.cold_target,
            self.max_cold_credit_per_origin,
            self.triage_batch,
            self.scout_parallelism,
            self.max_search_directives,
        )
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in values
        ):
            raise ValueError("source pool targets must be non-negative integers")
        if self.active_min > self.active_target:
            raise ValueError("active_min cannot exceed active_target")
        if self.warm_min > self.warm_target:
            raise ValueError("warm_min cannot exceed warm_target")
        if self.cold_min > self.cold_target:
            raise ValueError("cold_min cannot exceed cold_target")
        if self.max_cold_credit_per_origin < 1:
            raise ValueError("max_cold_credit_per_origin must be positive")
        if self.triage_batch < 1 or self.scout_parallelism < 1:
            raise ValueError("triage and scout capacities must be positive")
        if self.max_search_directives < 1:
            raise ValueError("max_search_directives must be positive")


@dataclass(frozen=True)
class SearchDirective:
    kind: SearchDirectiveKind
    strategy: str
    desired_candidates: int
    subject: str | None
    reason: str
    task_type: SourceIntelligenceTask = SourceIntelligenceTask.DISCOVER_NEW_SOURCE

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", SearchDirectiveKind(self.kind))
        object.__setattr__(self, "task_type", SourceIntelligenceTask(self.task_type))
        if not isinstance(self.strategy, str) or not self.strategy.strip():
            raise ValueError("search directive strategy is required")
        if (
            isinstance(self.desired_candidates, bool)
            or not isinstance(self.desired_candidates, int)
            or self.desired_candidates < 1
        ):
            raise ValueError("desired_candidates must be a positive integer")
        if self.subject is not None and (
            not isinstance(self.subject, str) or not self.subject.strip()
        ):
            raise ValueError("search directive subject must be non-empty when provided")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise ValueError("search directive reason is required")

    @property
    def dedup_key(self) -> str:
        return f"{self.strategy}:{self.subject or '*'}"


@dataclass(frozen=True)
class ReservoirPlan:
    active_count: int
    warm_count: int
    cold_count: int
    effective_cold_count: int
    triage_source_keys: tuple[str, ...]
    scout_source_keys: tuple[str, ...]
    activate_source_keys: tuple[str, ...]
    search_directives: tuple[SearchDirective, ...]
    deterministic_search_plans: tuple[QueryPlan, ...] = ()
    background_triage_source_keys: tuple[str, ...] = ()
    background_scout_source_keys: tuple[str, ...] = ()
    background_activate_source_keys: tuple[str, ...] = ()
    search_zero_new_streak: int = 0
    search_adaptive_cooldown_seconds: float = 0.0
    search_call_budget: int = 0

    @property
    def needs_search(self) -> bool:
        return bool(self.search_directives or self.deterministic_search_plans)


class SourceReservoirManager:
    """Plan finite source work before requesting more agent/search supply.

    Search is consumption-driven, but cold inventory receives bounded credit
    per origin so thousands of sibling shards cannot masquerade as independent
    discovery opportunities. Deterministic CDX/CDXJ work is excluded from this
    foreground inventory and advances only on the idle background lane.
    """

    # These strategies encode a specialized task contract. Recycling one as a
    # generic REFILL_RESERVOIR arm can collide with the dedicated directive's
    # strategy/subject dedup key and silently change its task_type.
    _SPECIALIZED_SEARCH_STRATEGIES = frozenset(
        {
            "DIRECT_EVIDENCE_BULK",
            "EXPLOIT_DIRECT_ORIGIN",
            "EXPLOIT_SUCCESS",
            "EXPLORE_NEW_FAMILY",
            "INTERPRET_STRUCTURE",
            "COMPILE_ADAPTER",
            "RECOVER_STAGNATION",
        }
    )

    _COLD_STATES = frozenset(
        {
            SourceState.DISCOVERED,
            SourceState.TRIAGED,
            SourceState.SCOUT_READY,
            SourceState.SCOUTING,
        }
    )

    def __init__(
        self,
        registry: SourceDiscoveryRegistry,
        *,
        targets: SourcePoolTargets | None = None,
        search_cooldown_seconds: float = 0.0,
        search_ucb_exploration: float = 0.35,
        stagnation_window: int = 6,
        residual_search_scheduler: SearchCellScheduler | None = None,
    ) -> None:
        for name, value in (
            ("search_cooldown_seconds", search_cooldown_seconds),
            ("search_ucb_exploration", search_ucb_exploration),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value < 0
            ):
                raise ValueError(f"{name} must be finite and non-negative")
        if (
            isinstance(stagnation_window, bool)
            or not isinstance(stagnation_window, int)
            or stagnation_window < 2
        ):
            raise ValueError("stagnation_window must be an integer >= 2")
        self.registry = registry
        self.targets = targets or SourcePoolTargets()
        self.search_cooldown_seconds = float(search_cooldown_seconds)
        self.search_ucb_exploration = float(search_ucb_exploration)
        self.stagnation_window = int(stagnation_window)
        self.residual_search_scheduler = residual_search_scheduler
        self.value_model = InterpretableSourceValueModel(registry)

    def _usable(self, candidates: list[SourceCandidate]) -> list[SourceCandidate]:
        return [
            candidate
            for candidate in candidates
            if self.registry.suppression_reason(candidate) is None
        ]

    def _effective_cold_count(
        self,
        candidates: list[SourceCandidate],
    ) -> int:
        """Return diversity-bounded cold scheduling credit.

        Raw inventory remains available for reporting through cold_count.
        Suppressed candidates are excluded by the caller before this method is
        reached, so a suppressed source/family/origin contributes no credit.
        """
        per_origin: dict[str, int] = {}
        for candidate in candidates:
            if (
                candidate.state not in self._COLD_STATES
                or is_background_bulk_candidate(candidate)
            ):
                continue
            per_origin[candidate.origin] = per_origin.get(candidate.origin, 0) + 1
        cap = self.targets.max_cold_credit_per_origin
        return sum(min(count, cap) for count in per_origin.values())

    @staticmethod
    def _is_productive_direct_inventory(
        candidate: SourceCandidate,
    ) -> bool:
        """Scheduling-only direct-source predicate; never evidence authority."""
        return (
            candidate.state in {SourceState.WARM, SourceState.ACTIVE}
            and candidate.direct_evidence_prior >= 0.5
        )

    def _unknown_format_holds(self) -> list[SourceCandidate]:
        """Return concrete sources blocked only on an unknown textual layout."""

        holds = [
            candidate
            for candidate in self.registry.list_candidates(state=SourceState.HOLD)
            if (
                candidate.level is SourceLevel.SOURCE
                and parse_unknown_format_reason(candidate.state_reason) is not None
                and self.registry.suppression_reason(candidate) is None
            )
        ]
        holds.sort(key=lambda item: (-item.scout_priority, item.source_key))
        return holds


    def _overlap_penalty(
        self,
        candidate: SourceCandidate,
        active: list[SourceCandidate],
    ) -> float:
        raw = self.registry.get_overlap_sketch(candidate.source_key)
        if raw is None:
            return 0.0
        sketch = MinHashSketch(raw)
        similarities: list[float] = []
        for other in active:
            other_raw = self.registry.get_overlap_sketch(other.source_key)
            if other_raw is None or len(other_raw) != len(raw):
                continue
            similarities.append(
                sketch.similarity(MinHashSketch(other_raw))
            )
        return max(similarities, default=0.0)

    def _candidate_value(
        self,
        candidate: SourceCandidate,
        *,
        active: list[SourceCandidate],
    ) -> float:
        estimate = self.value_model.estimate(
            candidate,
            overlap_penalty=self._overlap_penalty(candidate, active),
        )
        measurement = self.registry.get_scout_measurement(candidate.source_key)
        residual_bonus = (
            0.0
            if measurement is None
            else 0.01 * measurement.residual_opportunity
        )
        return estimate.score + residual_bonus

    def _rank_warm(
        self,
        candidates: list[SourceCandidate],
        *,
        active: list[SourceCandidate],
    ) -> list[SourceCandidate]:
        return sorted(
            candidates,
            key=lambda candidate: (
                self._candidate_value(candidate, active=active),
                candidate.source_key,
            ),
            reverse=True,
        )

    def _best_measured_family(
        self,
        candidates: list[SourceCandidate],
    ) -> tuple[str, float] | None:
        totals: dict[str, tuple[float, float]] = {}
        for candidate in candidates:
            if candidate.state not in {SourceState.WARM, SourceState.ACTIVE}:
                continue
            measurement = self.registry.get_scout_measurement(candidate.source_key)
            if measurement is None or measurement.elapsed_seconds <= 0:
                continue
            eed, elapsed = totals.get(candidate.source_family, (0.0, 0.0))
            totals[candidate.source_family] = (
                eed + measurement.novel_eed_for_ranking,
                elapsed + measurement.elapsed_seconds,
            )
        scored = [
            (family, eed / elapsed)
            for family, (eed, elapsed) in totals.items()
            if elapsed > 0 and eed > 0
        ]
        if not scored:
            return None
        return max(scored, key=lambda item: (item[1], item[0]))

    def _best_measured_direct_origin(
        self,
        candidates: list[SourceCandidate],
    ) -> tuple[str, float] | None:
        totals: dict[str, tuple[float, float]] = {}
        for candidate in candidates:
            if (
                candidate.state not in {SourceState.WARM, SourceState.ACTIVE}
                or candidate.direct_evidence_prior < 0.5
            ):
                continue
            measurement = self.registry.get_scout_measurement(candidate.source_key)
            if measurement is None or measurement.elapsed_seconds <= 0:
                continue
            eed, elapsed = totals.get(candidate.origin, (0.0, 0.0))
            totals[candidate.origin] = (
                eed + measurement.novel_eed_for_ranking,
                elapsed + measurement.elapsed_seconds,
            )
        scored = [
            (origin, eed / elapsed)
            for origin, (eed, elapsed) in totals.items()
            if elapsed > 0 and eed > 0
        ]
        if not scored:
            return None
        return max(scored, key=lambda item: (item[1], item[0]))

    def _best_observed_search_strategy(self) -> str:
        """Choose an observed strategy by UCB rather than greedy lock-in."""
        rewards = [
            reward
            for reward in self.registry.strategy_rewards()
            if (
                reward.episodes > 0
                and reward.search_cost_seconds > 0
                and reward.strategy not in self._SPECIALIZED_SEARCH_STRATEGIES
                and not reward.strategy.startswith("COMPILE_ADAPTER:")
            )
        ]
        if not rewards:
            return "META_SOURCE_SEARCH"
        total_episodes = sum(item.episodes for item in rewards)
        scale = max(
            1.0,
            max((item.reward_per_cost for item in rewards), default=0.0),
        )

        def ucb(reward) -> tuple[float, str]:
            bonus = (
                self.search_ucb_exploration
                * scale
                * (math.log(total_episodes + 1.0) / reward.episodes) ** 0.5
            )
            return reward.reward_per_cost + bonus, reward.strategy

        return max(rewards, key=ucb).strategy

    def _recent_search_supply(self) -> tuple[int, int, int]:
        """Return (zero-new-source streak, new sources, completed episodes).

        FINAL/scout reward is intentionally too delayed to throttle expensive
        agent calls. This immediate signal counts a source only in the episode
        that first proposed it, so repeated famous archive URLs are treated as
        zero supply rather than apparent progress.
        """
        rows = self.registry.connection.execute(
            """
            SELECT episode_id, new_sources
            FROM source_search_episodes
            WHERE finished_at IS NOT NULL
            ORDER BY finished_at DESC, episode_id DESC
            LIMIT ?
            """,
            (self.stagnation_window,),
        ).fetchall()
        new_sources = sum(int(row["new_sources"] or 0) for row in rows)
        zero_streak = 0
        for row in rows:
            if int(row["new_sources"] or 0) > 0:
                break
            zero_streak += 1
        return zero_streak, new_sources, len(rows)

    def _adaptive_search_cooldown(self) -> float:
        zero_streak, _new_sources, episodes = self._recent_search_supply()
        if episodes < 2 or zero_streak < 2:
            # One empty result reduces concurrency but does not freeze every
            # orthogonal search arm; the existing per-strategy cooldown is
            # sufficient to stop immediate repetition of that same query shape.
            return 0.0
        multiplier = min(8, 2 ** min(zero_streak - 1, 3))
        return self.search_cooldown_seconds * float(multiplier)

    def _global_search_available(self) -> bool:
        """Apply behavior-driven pacing across strategies, not just per arm."""
        cooldown = self._adaptive_search_cooldown()
        if cooldown <= 0:
            return True
        row = self.registry.connection.execute(
            """
            SELECT MAX(finished_at) AS finished_at
            FROM source_search_episodes
            WHERE finished_at IS NOT NULL
            """
        ).fetchone()
        if row is None or row["finished_at"] is None:
            return True
        return (
            float(self.registry.clock()) - float(row["finished_at"])
            >= cooldown
        )

    def _adaptive_search_budget(self, *, adapter_hold: bool) -> int:
        """Return the automatic LLM budget for validated adapter blockers only."""
        if not adapter_hold:
            return 0
        if self.targets.max_search_directives < 1:
            return 0
        # Adapter compilation is per-source and already protected by the
        # strategy cooldown. Deterministic search yield/stagnation must not
        # suppress or inflate this non-search engineering action.
        return 1


    def _is_stagnating(self) -> bool:
        rows = self.registry.connection.execute(
            """
            SELECT accepted_novel_eed
            FROM source_search_episodes
            WHERE finished_at IS NOT NULL
            ORDER BY finished_at DESC
            LIMIT ?
            """,
            (self.stagnation_window,),
        ).fetchall()
        if len(rows) < self.stagnation_window:
            return False
        return sum(float(row["accepted_novel_eed"]) for row in rows) <= 0.0

    def _llm_task_ucb(
        self,
        task_type: SourceIntelligenceTask,
    ) -> float:
        """Estimate value of spending one more Codex call on this task."""
        rewards = {
            str(item["task_type"]): item
            for item in self.registry.llm_task_rewards()
        }
        item = rewards.get(task_type.value)
        if item is None or int(item["episodes"]) < 1:
            # Every task class gets at least one exploration opportunity.
            return float("inf")
        episodes = int(item["episodes"])
        cost = float(item["cost_seconds"])
        credited = float(item["credited_eed"])
        mean = credited / cost if cost > 0 else 0.0
        total_episodes = sum(
            max(0, int(row["episodes"]))
            for row in rewards.values()
        )
        scale = max(
            1.0,
            max(
                (
                    float(row["credited_eed"])
                    / float(row["cost_seconds"])
                    if float(row["cost_seconds"]) > 0
                    else 0.0
                )
                for row in rewards.values()
            ),
        )
        bonus = (
            self.search_ucb_exploration
            * scale
            * (math.log(total_episodes + 1.0) / episodes) ** 0.5
        )
        return mean + bonus

    def _strategy_available(self, strategy: str) -> bool:
        """Rate-limit completed search strategies without adding another broker."""
        if self.search_cooldown_seconds <= 0:
            return True
        row = self.registry.connection.execute(
            """
            SELECT MAX(finished_at) AS finished_at
            FROM source_search_episodes
            WHERE strategy = ? AND finished_at IS NOT NULL
            """,
            (strategy,),
        ).fetchone()
        if row is None or row["finished_at"] is None:
            return True
        elapsed = float(self.registry.clock()) - float(row["finished_at"])
        return elapsed >= self.search_cooldown_seconds

    def _deterministic_search_plans(
        self,
        *,
        effective_cold_count: int,
    ) -> tuple[QueryPlan, ...]:
        if (
            self.residual_search_scheduler is None
            or effective_cold_count >= self.targets.cold_min
        ):
            return ()
        gap = max(1, self.targets.cold_target - effective_cold_count)
        limit = min(gap, self.targets.max_search_directives)
        return self.residual_search_scheduler.next_plans(limit=limit)

    def _search_directives(
        self,
        *,
        effective_cold_count: int,
        projected_warm: int,
        candidates: list[SourceCandidate],
        deterministic_refill_managed: bool = False,
        deterministic_plans_available: bool = False,
    ) -> tuple[SearchDirective, ...]:
        """Request automatic LLM work only to compile unsupported formats.

        Source discovery and refill belong to deterministic residual search and
        bounded structural crawling. Exhausting the finite residual program is
        an observable saturation condition, not permission to ask an LLM for a
        new mechanism, root, query family, or URL. Likewise, a structural HOLD
        remains deterministic/manual work rather than an automatic LLM search
        opportunity.

        Legacy arguments remain for API/test compatibility and explicit
        operator-driven workflows.
        """

        del (
            effective_cold_count,
            projected_warm,
            candidates,
            deterministic_refill_managed,
            deterministic_plans_available,
        )
        unknown_format_holds = self._unknown_format_holds()
        if not unknown_format_holds:
            return ()

        subject_candidate = unknown_format_holds[0]
        strategy = f"COMPILE_ADAPTER:{subject_candidate.source_key}"
        if not self._strategy_available(strategy):
            return ()

        capacity = self._adaptive_search_budget(adapter_hold=True)
        if capacity <= 0:
            return ()

        return (
            SearchDirective(
                kind=SearchDirectiveKind.UNKNOWN_FORMAT,
                strategy=strategy,
                desired_candidates=1,
                subject=subject_candidate.canonical_entrypoint,
                reason=subject_candidate.state_reason,
                task_type=SourceIntelligenceTask.COMPILE_ADAPTER,
            ),
        )

    def plan(self) -> ReservoirPlan:
        # Deterministic bulk artifacts/catalogs are not source-search inventory.
        # They are still durable candidates, but progress only through the
        # coordinator's idle background lane.
        schedulable_states = (
            SourceState.DISCOVERED,
            SourceState.TRIAGED,
            SourceState.SCOUT_READY,
            SourceState.SCOUTING,
            SourceState.WARM,
            SourceState.ACTIVE,
        )
        candidates = self._usable(
            self.registry.list_candidates_in_states(schedulable_states)
        )
        foreground = [
            candidate
            for candidate in candidates
            if not is_background_bulk_candidate(candidate)
        ]
        background = [
            candidate
            for candidate in candidates
            if is_background_bulk_candidate(candidate)
        ]

        by_state: dict[SourceState, list[SourceCandidate]] = {
            state: [] for state in SourceState
        }
        for candidate in foreground:
            by_state[candidate.state].append(candidate)

        background_by_state: dict[SourceState, list[SourceCandidate]] = {
            state: [] for state in SourceState
        }
        for candidate in background:
            background_by_state[candidate.state].append(candidate)

        active = by_state[SourceState.ACTIVE]
        warm = by_state[SourceState.WARM]
        cold_count = sum(len(by_state[state]) for state in self._COLD_STATES)
        effective_cold_count = self._effective_cold_count(foreground)

        warm_ranked = self._rank_warm(warm, active=active)
        activation_slots = max(0, self.targets.active_target - len(active))
        activate = warm_ranked[:activation_slots]
        projected_warm = len(warm) - len(activate)

        discovered = sorted(
            by_state[SourceState.DISCOVERED],
            key=lambda item: (
                -self._candidate_value(item, active=active),
                item.source_key,
            ),
        )
        triage = discovered[: self.targets.triage_batch]

        scouting_now = len(by_state[SourceState.SCOUTING])
        scout_slots = max(0, self.targets.scout_parallelism - scouting_now)
        scout_candidates = self._usable(
            by_state[SourceState.SCOUT_READY]
        )
        scout_candidates.sort(
            key=lambda item: (
                -self._candidate_value(item, active=active),
                item.source_key,
            )
        )
        scout = scout_candidates[:scout_slots]

        deterministic_search_plans = self._deterministic_search_plans(
            effective_cold_count=effective_cold_count,
        )
        directives = self._search_directives(
            effective_cold_count=effective_cold_count,
            projected_warm=projected_warm,
            candidates=foreground,
            deterministic_refill_managed=self.residual_search_scheduler is not None,
            deterministic_plans_available=bool(deterministic_search_plans),
        )

        # Background work is intentionally serialized. The coordinator will
        # execute at most one of these steps only when foreground work is empty.
        background_triage = sorted(
            background_by_state[SourceState.DISCOVERED],
            key=lambda item: item.source_key,
        )[:1]
        background_scout = sorted(
            background_by_state[SourceState.SCOUT_READY],
            key=lambda item: item.source_key,
        )[:1]
        background_activate = self._rank_warm(
            background_by_state[SourceState.WARM],
            active=background_by_state[SourceState.ACTIVE],
        )[:1]

        adapter_hold = bool(self._unknown_format_holds())
        zero_new_streak, _recent_new_sources, _episodes = (
            self._recent_search_supply()
        )
        return ReservoirPlan(
            active_count=len(active),
            warm_count=len(warm),
            cold_count=cold_count,
            effective_cold_count=effective_cold_count,
            triage_source_keys=tuple(item.source_key for item in triage),
            scout_source_keys=tuple(item.source_key for item in scout),
            activate_source_keys=tuple(item.source_key for item in activate),
            search_directives=directives,
            deterministic_search_plans=deterministic_search_plans,
            background_triage_source_keys=tuple(
                item.source_key for item in background_triage
            ),
            background_scout_source_keys=tuple(
                item.source_key for item in background_scout
            ),
            background_activate_source_keys=tuple(
                item.source_key for item in background_activate
            ),
            search_zero_new_streak=zero_new_streak,
            search_adaptive_cooldown_seconds=self._adaptive_search_cooldown(),
            search_call_budget=self._adaptive_search_budget(
                adapter_hold=adapter_hold
            ),
        )

