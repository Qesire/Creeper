"""FINAL production-value estimates for activated source scheduling.

The model is deliberately authority-scoped. Closed source-run outcomes dominate
scout proxies; scout proxies are used only before FINAL exposure exists.
Exploration remains bounded so one zero run cannot permanently starve a source.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from creeper.source_discovery.models import SourceCandidate
from creeper.source_discovery.registry import SourceDiscoveryRegistry, SourceRunOutcome


@dataclass(frozen=True)
class ProductionValueEstimate:
    expected_final_eed: float
    expected_cost: float
    recent_marginal_eed_per_second: float
    recent_marginal_eed_per_request: float
    closed_runs: int
    zero_runs: int
    uncertainty_bonus: float
    exploration_floor: float
    success_probability: float
    score: float


class ProductionValueModel:
    """Auditable FINAL-first scheduling value with bounded exploration."""

    def __init__(
        self,
        registry: SourceDiscoveryRegistry,
        *,
        recent_window: int = 8,
        ewma_alpha: float = 0.6,
        exploration_weight: float = 0.05,
        exploration_floor: float = 0.01,
        scout_proxy_weight: float = 0.25,
    ) -> None:
        if recent_window < 1:
            raise ValueError("recent_window must be positive")
        if not 0 < ewma_alpha <= 1:
            raise ValueError("ewma_alpha must be within (0, 1]")
        if min(exploration_weight, exploration_floor, scout_proxy_weight) < 0:
            raise ValueError("production value weights must be non-negative")
        self.registry = registry
        self.recent_window = int(recent_window)
        self.ewma_alpha = float(ewma_alpha)
        self.exploration_weight = float(exploration_weight)
        self.base_exploration_floor = float(exploration_floor)
        self.scout_proxy_weight = float(scout_proxy_weight)

    @staticmethod
    def _ewma(values: list[float], alpha: float) -> float:
        if not values:
            return 0.0
        # Values are supplied oldest -> newest so recent outcomes dominate.
        estimate = float(values[0])
        for value in values[1:]:
            estimate = alpha * float(value) + (1.0 - alpha) * estimate
        return estimate

    def _authority(
        self,
        baseline_signature: str | None,
        model_signature: str | None,
    ) -> tuple[str, str] | None:
        if (baseline_signature is None) != (model_signature is None):
            raise ValueError(
                "baseline_signature and model_signature must be provided together"
            )
        if baseline_signature is not None:
            return baseline_signature, model_signature
        return self.registry.current_scout_authority

    def _recent_runs(
        self,
        candidate: SourceCandidate,
        authority: tuple[str, str],
    ) -> list[SourceRunOutcome]:
        rows = self.registry.list_source_run_outcomes(
            candidate.source_key,
            baseline_signature=authority[0],
            model_signature=authority[1],
            closed_only=True,
        )
        return rows[-self.recent_window :]

    def _bootstrap_proxy(self, candidate: SourceCandidate) -> tuple[float, float]:
        measurement = self.registry.get_scout_measurement(candidate.source_key)
        if measurement is None:
            return (
                max(0.01, float(candidate.scout_priority)),
                max(
                    1.0,
                    1.0
                    + float(candidate.access_cost_prior)
                    + float(candidate.adapter_cost_prior),
                ),
            )
        return (
            max(0.0, float(measurement.novel_eed_for_ranking)),
            max(1e-3, float(measurement.elapsed_seconds)),
        )

    def estimate(
        self,
        candidate: SourceCandidate,
        *,
        baseline_signature: str | None = None,
        model_signature: str | None = None,
    ) -> ProductionValueEstimate:
        authority = self._authority(baseline_signature, model_signature)
        proxy_eed, proxy_cost = self._bootstrap_proxy(candidate)
        if authority is None:
            bonus = self.exploration_weight
            floor = self.base_exploration_floor
            proxy_rate = proxy_eed / max(1e-6, proxy_cost)
            return ProductionValueEstimate(
                expected_final_eed=proxy_eed,
                expected_cost=proxy_cost,
                recent_marginal_eed_per_second=0.0,
                recent_marginal_eed_per_request=0.0,
                closed_runs=0,
                zero_runs=0,
                uncertainty_bonus=bonus,
                exploration_floor=floor,
                success_probability=0.5,
                score=max(floor, self.scout_proxy_weight * proxy_rate + bonus),
            )

        all_runs = self.registry.list_source_run_outcomes(
            candidate.source_key,
            baseline_signature=authority[0],
            model_signature=authority[1],
            closed_only=True,
        )
        closed_runs = len(all_runs)
        zero_runs = sum(1 for run in all_runs if run.final_accepted_eed <= 0.0)
        successes = closed_runs - zero_runs
        success_probability = (successes + 1.0) / (closed_runs + 2.0)

        uncertainty = self.exploration_weight / math.sqrt(closed_runs + 1.0)
        floor = self.base_exploration_floor / math.sqrt(closed_runs + 1.0)

        if closed_runs == 0:
            proxy_rate = proxy_eed / max(1e-6, proxy_cost)
            return ProductionValueEstimate(
                expected_final_eed=proxy_eed,
                expected_cost=proxy_cost,
                recent_marginal_eed_per_second=0.0,
                recent_marginal_eed_per_request=0.0,
                closed_runs=0,
                zero_runs=0,
                uncertainty_bonus=uncertainty,
                exploration_floor=floor,
                success_probability=success_probability,
                score=max(
                    floor,
                    self.scout_proxy_weight * proxy_rate + uncertainty,
                ),
            )

        recent = self._recent_runs(candidate, authority)
        # list_source_run_outcomes is oldest -> newest already.
        eeds = [run.final_accepted_eed for run in recent]
        costs = [max(1e-6, run.resource_cost_seconds) for run in recent]
        requests = [
            max(1, run.source_requests + run.provider_requests)
            for run in recent
        ]
        per_second = [
            eed / cost for eed, cost in zip(eeds, costs, strict=True)
        ]
        per_request = [
            eed / request for eed, request in zip(eeds, requests, strict=True)
        ]

        expected_eed = self._ewma(eeds, self.ewma_alpha)
        expected_cost = self._ewma(costs, self.ewma_alpha)
        recent_per_second = self._ewma(per_second, self.ewma_alpha)
        recent_per_request = self._ewma(per_request, self.ewma_alpha)

        exploit = success_probability * recent_per_second
        score = max(floor, exploit + uncertainty)
        return ProductionValueEstimate(
            expected_final_eed=expected_eed,
            expected_cost=expected_cost,
            recent_marginal_eed_per_second=recent_per_second,
            recent_marginal_eed_per_request=recent_per_request,
            closed_runs=closed_runs,
            zero_runs=zero_runs,
            uncertainty_bonus=uncertainty,
            exploration_floor=floor,
            success_probability=success_probability,
            score=score,
        )
