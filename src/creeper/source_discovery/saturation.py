"""Origin-level saturation control for measured source siblings.

This module is deliberately a control-plane component. It consumes durable
scout measurements, makes an interpretable scheduling decision, and writes only
an ORIGIN suppression through SourceDiscoveryRegistry. It never changes source
family authority, annual evidence authority, candidate lineage, or scout audit
rows.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from creeper.source_discovery.models import (
    MeasurementMode,
    SuppressionScope,
    source_origin,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry


@dataclass(frozen=True)
class SaturationPolicy:
    """Deterministic thresholds for zero-yield origin saturation."""

    min_measured_siblings: int = 32
    min_total_observations: int = 0
    max_total_novel_eed_for_zero_class: float = 0.0
    suppression_ttl_seconds: float | None = 6 * 60 * 60

    def __post_init__(self) -> None:
        if self.min_measured_siblings < 1:
            raise ValueError("min_measured_siblings must be positive")
        if self.min_total_observations < 0:
            raise ValueError("min_total_observations must be non-negative")
        if self.max_total_novel_eed_for_zero_class < 0:
            raise ValueError(
                "max_total_novel_eed_for_zero_class must be non-negative"
            )
        if (
            self.suppression_ttl_seconds is not None
            and self.suppression_ttl_seconds <= 0
        ):
            raise ValueError(
                "suppression_ttl_seconds must be positive when configured"
            )


@dataclass(frozen=True)
class SaturationDecision:
    """Auditable result for one canonical source origin."""

    origin: str
    measured_sources: int
    positive_sources: int
    total_observations: int
    total_novel_eed: float
    should_suppress: bool
    reason: str


@dataclass(frozen=True)
class _MeasuredSource:
    origin: str
    observed_count: int
    novel_eed: float


class SourceSaturationController:
    """Collapse repeated zero-yield sibling measurements into ORIGIN suppression.

    The controller is intended for the coordinator's serialized control path.
    Measurement reads are batched into one SQLite query so thousands of sibling
    candidates do not create a per-source query storm.
    """

    def __init__(
        self,
        registry: SourceDiscoveryRegistry,
        *,
        policy: SaturationPolicy | None = None,
    ) -> None:
        self.registry = registry
        self.policy = policy or SaturationPolicy()

    def _current_measurements(self) -> tuple[_MeasuredSource, ...]:
        """Read current-authority completed scout measurements in one query.

        If no scout authority marker exists, legacy measurements are considered
        current, matching SourceDiscoveryRegistry.get_scout_measurement().
        Once authority is installed, stale rows remain durable for audit but are
        excluded from scheduling saturation.
        """

        authority = self.registry.current_scout_authority
        sql = """
            SELECT
                c.canonical_entrypoint,
                m.measurement_mode,
                m.unique_hosts,
                m.observed_host_year_pairs,
                m.novel_eed,
                m.novel_pair_eed
            FROM source_candidates AS c
            JOIN source_scout_metrics AS m
              ON m.source_key = c.source_key
        """
        params: tuple[object, ...] = ()
        if authority is not None:
            sql += """
                WHERE m.baseline_signature = ?
                  AND m.model_signature = ?
            """
            params = authority
        sql += " ORDER BY c.source_key"

        measured: list[_MeasuredSource] = []
        for row in self.registry.connection.execute(sql, params):
            mode = MeasurementMode(str(row["measurement_mode"]))
            observed_count = (
                int(row["observed_host_year_pairs"])
                if mode is MeasurementMode.HOST_YEAR
                else int(row["unique_hosts"])
            )
            novel_eed = (
                float(row["novel_pair_eed"])
                if mode is MeasurementMode.HOST_YEAR
                else float(row["novel_eed"])
            )
            measured.append(
                _MeasuredSource(
                    origin=source_origin(str(row["canonical_entrypoint"])),
                    observed_count=observed_count,
                    novel_eed=novel_eed,
                )
            )
        return tuple(measured)

    @staticmethod
    def _canonical_origin(origin: str) -> str:
        try:
            return source_origin(origin)
        except ValueError as exc:
            raise ValueError("origin must be an absolute http/https URL") from exc

    def _decision(
        self,
        origin: str,
        measured: tuple[_MeasuredSource, ...],
    ) -> SaturationDecision:
        measured_sources = len(measured)
        positive_sources = sum(item.novel_eed > 0.0 for item in measured)
        total_observations = sum(item.observed_count for item in measured)
        total_novel_eed = sum(item.novel_eed for item in measured)

        enough_sources = measured_sources >= self.policy.min_measured_siblings
        enough_observations = (
            total_observations >= self.policy.min_total_observations
        )
        zero_class = (
            positive_sources == 0
            and total_novel_eed
            <= self.policy.max_total_novel_eed_for_zero_class
        )
        should_suppress = enough_sources and enough_observations and zero_class

        if should_suppress:
            reason = (
                "origin saturation: "
                f"measured={measured_sources} "
                f"positive={positive_sources} "
                f"observations={total_observations} "
                f"novel_eed={total_novel_eed:g}"
            )
        elif not enough_sources:
            reason = (
                "origin saturation not reached: "
                f"measured={measured_sources} "
                f"required={self.policy.min_measured_siblings}"
            )
        elif positive_sources:
            reason = (
                "origin saturation blocked by positive yield: "
                f"measured={measured_sources} "
                f"positive={positive_sources} "
                f"novel_eed={total_novel_eed:g}"
            )
        elif not enough_observations:
            reason = (
                "origin saturation not reached: "
                f"observations={total_observations} "
                f"required={self.policy.min_total_observations}"
            )
        else:
            reason = (
                "origin saturation not reached: "
                f"novel_eed={total_novel_eed:g} "
                "exceeds zero-class threshold="
                f"{self.policy.max_total_novel_eed_for_zero_class:g}"
            )

        return SaturationDecision(
            origin=origin,
            measured_sources=measured_sources,
            positive_sources=positive_sources,
            total_observations=total_observations,
            total_novel_eed=total_novel_eed,
            should_suppress=should_suppress,
            reason=reason,
        )

    def evaluate(self, origin: str) -> SaturationDecision:
        """Evaluate one canonical origin without mutating suppression state."""

        canonical = self._canonical_origin(origin)
        measured = tuple(
            item
            for item in self._current_measurements()
            if item.origin == canonical
        )
        return self._decision(canonical, measured)

    def evaluate_all(self) -> tuple[SaturationDecision, ...]:
        """Evaluate every origin having at least one current scout measurement."""

        grouped: dict[str, list[_MeasuredSource]] = defaultdict(list)
        for item in self._current_measurements():
            grouped[item.origin].append(item)
        return tuple(
            self._decision(origin, tuple(grouped[origin]))
            for origin in sorted(grouped)
        )

    def _same_active_origin_suppression(
        self,
        decision: SaturationDecision,
    ) -> bool:
        """Avoid refreshing TTL/created_at on an identical active decision."""

        now = float(self.registry.clock())
        row = self.registry.connection.execute(
            """
            SELECT reason
            FROM source_suppressions
            WHERE scope_type = ?
              AND scope_key = ?
              AND (expires_at IS NULL OR expires_at > ?)
            """,
            (
                SuppressionScope.ORIGIN.value,
                decision.origin,
                now,
            ),
        ).fetchone()
        return row is not None and str(row["reason"]) == decision.reason

    def apply(self, decision: SaturationDecision) -> bool:
        """Persist one saturated-origin scheduling suppression.

        Returns True only when durable suppression state is created or changed.
        Re-applying the same active decision is a read-only no-op, so finite TTL
        is not accidentally extended on every coordinator cycle.
        """

        if not decision.should_suppress:
            return False
        if self._same_active_origin_suppression(decision):
            return False
        self.registry.suppress(
            SuppressionScope.ORIGIN,
            decision.origin,
            reason=decision.reason,
            ttl_seconds=self.policy.suppression_ttl_seconds,
        )
        return True

    def run(self) -> tuple[SaturationDecision, ...]:
        """Evaluate all measured origins and apply only saturated decisions."""

        decisions = self.evaluate_all()
        for decision in decisions:
            self.apply(decision)
        return decisions
