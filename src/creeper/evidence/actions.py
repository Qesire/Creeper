"""Evidence-action classification and empirical-Bayes value primitives.

These objects do not execute provider work. EvidenceQueryKey remains the
durable authority identity and AsyncEvidenceWorker remains the only provider
executor. This module only gives those existing tasks a stable action class and
an interpretable reward-per-request estimate.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum

from creeper.evidence.policies import EvidenceQueryKey


class EvidenceActionKind(StrEnum):
    EXACT = "exact"
    RANGE = "range"
    DOMAIN = "domain"
    RDAP = "rdap"


# Bootstrap means are expressed as final baseline-external host-years per
# provider request. They preserve the old qualitative ordering while only
# contributing four pseudo-requests; production evidence quickly dominates.
ACTION_PRIOR_YIELD: dict[EvidenceActionKind, float] = {
    EvidenceActionKind.EXACT: 0.25,
    EvidenceActionKind.RANGE: 0.50,
    EvidenceActionKind.DOMAIN: 1.00,
    EvidenceActionKind.RDAP: 0.50,
}
ACTION_PRIOR_STRENGTH = 4.0
ACTION_RETRY_PENALTY = 0.25
RANGE_ADAPTATION_FULL_REQUESTS = 50
RANGE_EXPLORATION_FLOOR = 0.05
RANGE_EXPLORATION_CEILING = 0.95


def adaptive_range_first_fraction(
    base_fraction: float,
    *,
    exact_posterior: float,
    range_posterior: float,
    evidence_requests: int,
    reward_authoritative: bool,
) -> float:
    """Blend configured cold-start policy into learned exact/range allocation.

    No formal readiness reward means no behavior change. Once final reward is
    authoritative, at most 50 effective requests are required for the learned
    target to fully replace the configured bootstrap fraction. A small
    exploration floor/ceiling prevents permanent action starvation.
    """

    values = (base_fraction, exact_posterior, range_posterior)
    if any(not math.isfinite(float(value)) or value < 0 for value in values):
        raise ValueError("adaptive range inputs must be finite and non-negative")
    if base_fraction > 1:
        raise ValueError("base_fraction must be within [0, 1]")
    if evidence_requests < 0:
        raise ValueError("evidence_requests must be non-negative")
    if base_fraction == 0 or not reward_authoritative:
        return float(base_fraction)

    total = exact_posterior + range_posterior
    if total <= 0:
        target = float(base_fraction)
    else:
        target = range_posterior / total
    target = min(
        RANGE_EXPLORATION_CEILING,
        max(RANGE_EXPLORATION_FLOOR, target),
    )
    confidence = min(
        1.0,
        evidence_requests / float(RANGE_ADAPTATION_FULL_REQUESTS),
    )
    return (
        (1.0 - confidence) * float(base_fraction)
        + confidence * target
    )


def classify_evidence_action_fields(
    *,
    provider: str,
    year_from: int,
    year_to: int,
    policy_version: str,
) -> EvidenceActionKind:
    if provider == "rdap":
        return EvidenceActionKind.RDAP
    if policy_version.startswith("cdx-domain-"):
        return EvidenceActionKind.DOMAIN
    if year_from == year_to:
        return EvidenceActionKind.EXACT
    return EvidenceActionKind.RANGE


def classify_evidence_action(key: EvidenceQueryKey) -> EvidenceActionKind:
    scope = key.temporal_scope
    return classify_evidence_action_fields(
        provider=key.provider,
        year_from=scope.year_from,
        year_to=scope.year_to,
        policy_version=key.policy_version,
    )


def action_prior_yield(kind: EvidenceActionKind | str) -> float:
    return ACTION_PRIOR_YIELD[EvidenceActionKind(kind)]


@dataclass(frozen=True)
class EvidenceActionValueStats:
    action_kind: EvidenceActionKind
    attempts: int
    provider_requests: int
    provider_elapsed_milliseconds: int
    final_novel_host_years: int
    final_novel_eed: float
    posterior_host_years_per_request: float
    final_eed_per_request: float

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "action_kind",
            EvidenceActionKind(self.action_kind),
        )
        for name in (
            "attempts",
            "provider_requests",
            "provider_elapsed_milliseconds",
            "final_novel_host_years",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        for name in (
            "final_novel_eed",
            "posterior_host_years_per_request",
            "final_eed_per_request",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and non-negative")


def posterior_host_year_yield(
    kind: EvidenceActionKind | str,
    *,
    final_novel_host_years: int,
    provider_requests: int,
    attempts: int,
    prior_strength: float = ACTION_PRIOR_STRENGTH,
) -> float:
    kind = EvidenceActionKind(kind)
    if (
        final_novel_host_years < 0
        or provider_requests < 0
        or attempts < 0
        or prior_strength <= 0
        or not math.isfinite(prior_strength)
    ):
        raise ValueError("invalid evidence-action posterior inputs")
    effective_requests = max(provider_requests, attempts)
    prior = action_prior_yield(kind)
    return (
        float(final_novel_host_years) + prior_strength * prior
    ) / (float(effective_requests) + prior_strength)


def task_value_score(
    kind: EvidenceActionKind | str,
    *,
    eed_weight: float,
    final_novel_host_years: int,
    provider_requests: int,
    attempts: int,
    task_attempt: int = 0,
) -> float:
    if (
        not math.isfinite(float(eed_weight))
        or eed_weight < 0
        or task_attempt < 0
    ):
        raise ValueError("invalid evidence task value inputs")
    posterior = posterior_host_year_yield(
        kind,
        final_novel_host_years=final_novel_host_years,
        provider_requests=provider_requests,
        attempts=attempts,
    )
    return (
        float(eed_weight)
        * posterior
        / (1.0 + ACTION_RETRY_PENALTY * task_attempt)
    )
