"""Competition-facing throughput projections kept separate from official score."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable


CONSERVATIVE_EED_WEIGHT = Decimal("0.56")
DEFAULT_TARGETS = (Decimal("100000"), Decimal("250000"), Decimal("500000"), Decimal("1000000"))


def _decimal(value: Decimal | int | float | str) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


@dataclass(frozen=True)
class PerformanceReference:
    """A bounded, externally supplied comparison result.

    The reference is deliberately data-only. Callers should record its source
    and verification status in the report because it is not an organizer score.
    """

    observation_days: Decimal
    annual_raw: Decimal
    annual_eed: Decimal
    candidate_raw: Decimal
    candidate_eed: Decimal
    baseline_eed: Decimal | None = None


@dataclass(frozen=True)
class TargetProjection:
    target_eed_per_day: Decimal
    raw_at_reference_weight: Decimal
    raw_at_conservative_weight: Decimal
    multiple_of_reference_annual_rate: Decimal
    eta_to_five_percent_days: Decimal | None


@dataclass(frozen=True)
class PerformanceModel:
    reference: PerformanceReference
    annual_eed_per_day: Decimal
    candidate_eed_per_day: Decimal
    combined_eed_per_day_scenario: Decimal
    annual_weight: Decimal
    candidate_weight: Decimal
    targets: tuple[TargetProjection, ...]

    def to_dict(self) -> dict[str, object]:
        def text(value: Decimal | None) -> str | None:
            return None if value is None else format(value, "f")

        return {
            "reference": {
                "observation_days": text(self.reference.observation_days),
                "annual_raw": text(self.reference.annual_raw),
                "annual_eed": text(self.reference.annual_eed),
                "candidate_raw": text(self.reference.candidate_raw),
                "candidate_eed": text(self.reference.candidate_eed),
                "baseline_eed": text(self.reference.baseline_eed),
            },
            "annual_eed_per_day": text(self.annual_eed_per_day),
            "candidate_eed_per_day": text(self.candidate_eed_per_day),
            "combined_eed_per_day_scenario": text(self.combined_eed_per_day_scenario),
            "annual_raw_to_eed_weight": text(self.annual_weight),
            "candidate_raw_to_eed_weight": text(self.candidate_weight),
            "targets": [
                {
                    "target_eed_per_day": text(item.target_eed_per_day),
                    "raw_at_reference_weight": text(item.raw_at_reference_weight),
                    "raw_at_conservative_weight": text(item.raw_at_conservative_weight),
                    "multiple_of_reference_annual_rate": text(
                        item.multiple_of_reference_annual_rate
                    ),
                    "eta_to_five_percent_days": text(item.eta_to_five_percent_days),
                }
                for item in self.targets
            ],
            "score_interpretation": {
                "annual_is_primary": True,
                "candidate_added_to_official_total": False,
                "candidate_combination_is_scenario_only": True,
            },
        }


def build_performance_model(
    reference: PerformanceReference,
    *,
    targets: Iterable[Decimal | int | float | str] = DEFAULT_TARGETS,
    conservative_weight: Decimal | int | float | str = CONSERVATIVE_EED_WEIGHT,
) -> PerformanceModel:
    days = _decimal(reference.observation_days)
    annual_raw = _decimal(reference.annual_raw)
    annual_eed = _decimal(reference.annual_eed)
    candidate_raw = _decimal(reference.candidate_raw)
    candidate_eed = _decimal(reference.candidate_eed)
    baseline_eed = None if reference.baseline_eed is None else _decimal(reference.baseline_eed)
    conservative = _decimal(conservative_weight)
    if any(value <= 0 for value in (days, annual_raw, annual_eed, candidate_raw, candidate_eed)):
        raise ValueError("reference days, raw counts, and EED values must be positive")
    if not 0 < conservative <= 1:
        raise ValueError("conservative weight must be in (0, 1]")
    annual_weight = annual_eed / annual_raw
    candidate_weight = candidate_eed / candidate_raw
    annual_rate = annual_eed / days
    candidate_rate = candidate_eed / days
    projections = []
    for raw_target in targets:
        target = _decimal(raw_target)
        if target <= 0:
            raise ValueError("target EED/day must be positive")
        projections.append(
            TargetProjection(
                target,
                target / annual_weight,
                target / conservative,
                target / annual_rate,
                (baseline_eed * Decimal("0.05") / target) if baseline_eed else None,
            )
        )
    return PerformanceModel(
        PerformanceReference(days, annual_raw, annual_eed, candidate_raw, candidate_eed, baseline_eed),
        annual_rate,
        candidate_rate,
        annual_rate + candidate_rate,
        annual_weight,
        candidate_weight,
        tuple(projections),
    )
