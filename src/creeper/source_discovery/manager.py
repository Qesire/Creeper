"""Pure inventory planning for source discovery and source-reservoir refill."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from creeper.source_discovery.models import SourceCandidate, SourceState
from creeper.source_discovery.registry import SourceDiscoveryRegistry


class SearchDirectiveKind(StrEnum):
    REFILL_RESERVOIR = "REFILL_RESERVOIR"
    EXPLOIT_SOURCE_FAMILY = "EXPLOIT_SOURCE_FAMILY"
    DISCOVER_NEW_FAMILY = "DISCOVER_NEW_FAMILY"


@dataclass(frozen=True)
class SourcePoolTargets:
    active_min: int = 2
    active_target: int = 3
    warm_min: int = 10
    warm_target: int = 20
    cold_min: int = 50
    cold_target: int = 100
    triage_batch: int = 16
    scout_parallelism: int = 4
    max_search_directives: int = 3

    def __post_init__(self) -> None:
        values = (
            self.active_min,
            self.active_target,
            self.warm_min,
            self.warm_target,
            self.cold_min,
            self.cold_target,
            self.triage_batch,
            self.scout_parallelism,
            self.max_search_directives,
        )
        if any(not isinstance(value, int) or value < 0 for value in values):
            raise ValueError("source pool targets must be non-negative integers")
        if self.active_min > self.active_target:
            raise ValueError("active_min cannot exceed active_target")
        if self.warm_min > self.warm_target:
            raise ValueError("warm_min cannot exceed warm_target")
        if self.cold_min > self.cold_target:
            raise ValueError("cold_min cannot exceed cold_target")
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

    @property
    def dedup_key(self) -> str:
        return f"{self.strategy}:{self.subject or '*'}"


@dataclass(frozen=True)
class ReservoirPlan:
    active_count: int
    warm_count: int
    cold_count: int
    triage_source_keys: tuple[str, ...]
    scout_source_keys: tuple[str, ...]
    activate_source_keys: tuple[str, ...]
    search_directives: tuple[SearchDirective, ...]

    @property
    def needs_search(self) -> bool:
        return bool(self.search_directives)


class SourceReservoirManager:
    """Plan finite source work before requesting more agent/search supply.

    Search is consumption-driven: existing DISCOVERED/TRIAGED/SCOUT_READY work
    is drained before the manager asks agents for additional candidates. This
    keeps expensive search roughly coupled to source consumption rather than to
    wall-clock uptime.
    """

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
    ) -> None:
        if search_cooldown_seconds < 0:
            raise ValueError("search_cooldown_seconds must be non-negative")
        self.registry = registry
        self.targets = targets or SourcePoolTargets()
        self.search_cooldown_seconds = float(search_cooldown_seconds)

    def _usable(self, candidates: list[SourceCandidate]) -> list[SourceCandidate]:
        return [
            candidate
            for candidate in candidates
            if self.registry.suppression_reason(candidate) is None
        ]

    def _rank_warm(self, candidates: list[SourceCandidate]) -> list[SourceCandidate]:
        def key(candidate: SourceCandidate) -> tuple[float, int, str]:
            measurement = self.registry.get_scout_measurement(candidate.source_key)
            if measurement is None:
                return (0.0, 0, candidate.source_key)
            return (
                measurement.novel_eed_per_second,
                measurement.direct_host_years,
                candidate.source_key,
            )

        return sorted(candidates, key=key, reverse=True)

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
                eed + measurement.novel_eed,
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

    def _best_observed_search_strategy(self) -> str:
        rewards = [
            reward
            for reward in self.registry.strategy_rewards()
            if reward.episodes > 0
            and reward.search_cost_seconds > 0
            and reward.reward_per_cost > 0
        ]
        if not rewards:
            return "META_SOURCE_SEARCH"
        return max(
            rewards,
            key=lambda reward: (reward.reward_per_cost, reward.strategy),
        ).strategy

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

    def _search_directives(
        self,
        *,
        cold_count: int,
        projected_warm: int,
        candidates: list[SourceCandidate],
    ) -> tuple[SearchDirective, ...]:
        if cold_count >= self.targets.cold_min:
            return ()

        # Every parallel search shares one finite refill budget. Building the
        # strategy set first and allocating the deficit second prevents N
        # concurrent search workers from each assuming they own the full gap.
        gap = self.targets.cold_target - cold_count
        if gap <= 0:
            return ()

        specs: list[tuple[SearchDirectiveKind, str, str | None, str]] = []
        seen: set[str] = set()

        def add_spec(
            kind: SearchDirectiveKind,
            strategy: str,
            subject: str | None,
            reason: str,
        ) -> None:
            if len(specs) >= self.targets.max_search_directives:
                return
            dedup_key = f"{strategy}:{subject or '*'}"
            if dedup_key in seen or not self._strategy_available(strategy):
                return
            seen.add(dedup_key)
            specs.append((kind, strategy, subject, reason))

        best_family = self._best_measured_family(candidates)
        if projected_warm < self.targets.warm_min and best_family is not None:
            family, value = best_family
            add_spec(
                SearchDirectiveKind.EXPLOIT_SOURCE_FAMILY,
                "EXPLOIT_SUCCESS",
                family,
                f"warm reserve low; measured family yield={value:.6g} novel EED/s",
            )

        best_strategy = self._best_observed_search_strategy()
        add_spec(
            SearchDirectiveKind.REFILL_RESERVOIR,
            best_strategy,
            None,
            f"usable cold reserve {cold_count} below minimum {self.targets.cold_min}",
        )
        add_spec(
            SearchDirectiveKind.DISCOVER_NEW_FAMILY,
            "EXPLORE_NEW_FAMILY",
            None,
            "retain explicit exploration while refilling the candidate reserve",
        )

        # If the remaining gap is smaller than the strategy set, fewer searches
        # are launched rather than assigning a fake minimum of one to every arm.
        selected = specs[: min(len(specs), gap)]
        if not selected:
            return ()
        base, remainder = divmod(gap, len(selected))
        directives = [
            SearchDirective(
                kind=kind,
                strategy=strategy,
                desired_candidates=base + (1 if index < remainder else 0),
                subject=subject,
                reason=reason,
            )
            for index, (kind, strategy, subject, reason) in enumerate(selected)
        ]
        assert sum(item.desired_candidates for item in directives) == gap
        return tuple(directives)

    def plan(self) -> ReservoirPlan:
        all_candidates = self.registry.list_candidates()
        candidates = self._usable(all_candidates)
        by_state: dict[SourceState, list[SourceCandidate]] = {
            state: [] for state in SourceState
        }
        for candidate in candidates:
            by_state[candidate.state].append(candidate)

        active = by_state[SourceState.ACTIVE]
        warm = by_state[SourceState.WARM]
        cold_count = sum(len(by_state[state]) for state in self._COLD_STATES)

        warm_ranked = self._rank_warm(warm)
        activation_slots = max(0, self.targets.active_target - len(active))
        activate = warm_ranked[:activation_slots]
        projected_warm = len(warm) - len(activate)

        discovered = sorted(
            by_state[SourceState.DISCOVERED],
            key=lambda item: (-item.scout_priority, item.source_key),
        )
        triage = discovered[: self.targets.triage_batch]

        scouting_now = len(by_state[SourceState.SCOUTING])
        scout_slots = max(0, self.targets.scout_parallelism - scouting_now)
        scout = self.registry.rank_scout_candidates(limit=scout_slots)

        directives = self._search_directives(
            cold_count=cold_count,
            projected_warm=projected_warm,
            candidates=candidates,
        )
        return ReservoirPlan(
            active_count=len(active),
            warm_count=len(warm),
            cold_count=cold_count,
            triage_source_keys=tuple(item.source_key for item in triage),
            scout_source_keys=tuple(item.source_key for item in scout),
            activate_source_keys=tuple(item.source_key for item in activate),
            search_directives=directives,
        )
