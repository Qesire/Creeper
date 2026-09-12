"""Interpretable online source-value estimates.

Measured scout yield is calibrated by observed family-level final/scout
conversion. Graph descendants contribute gateway value. An overlap penalty can
discount redundant mirrors without giving a learned model evidence authority.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from creeper.source_discovery.models import SourceCandidate
from creeper.source_discovery.registry import SourceDiscoveryRegistry


@dataclass(frozen=True)
class SourceValueEstimate:
    expected_final_eed: float
    expected_cost_seconds: float
    direct_value: float
    descendant_value: float
    uncertainty_bonus: float
    overlap_penalty: float

    @property
    def score(self) -> float:
        value = max(
            0.0,
            self.direct_value
            + self.descendant_value
            + self.uncertainty_bonus,
        )
        value *= max(0.0, 1.0 - self.overlap_penalty)
        return value / max(1e-6, self.expected_cost_seconds)


class InterpretableSourceValueModel:
    """Auditable empirical model suitable for online scheduling."""

    def __init__(
        self,
        registry: SourceDiscoveryRegistry,
        *,
        exploration_weight: float = 0.25,
        descendant_discount: float = 0.5,
    ) -> None:
        if exploration_weight < 0 or not 0 <= descendant_discount <= 1:
            raise ValueError("invalid source value model weights")
        self.registry = registry
        self.exploration_weight = float(exploration_weight)
        self.descendant_discount = float(descendant_discount)

    def _family_conversion(self, family: str) -> tuple[float, int]:
        row = self.registry.connection.execute(
            """
            SELECT
                COUNT(*) AS n,
                SUM(
                    CASE
                        WHEN m.measurement_mode = 'HOST_YEAR'
                            THEN m.novel_pair_eed
                        ELSE m.novel_eed
                    END
                ) AS scout_sum,
                SUM(f.final_accepted_eed) AS final_sum
            FROM source_candidates c
            JOIN source_scout_metrics m ON m.source_key = c.source_key
            JOIN source_final_rewards f ON f.source_key = c.source_key
            WHERE c.source_family = ?
            """,
            (family,),
        ).fetchone()
        n = int(row["n"] or 0)
        scout_sum = float(row["scout_sum"] or 0.0)
        final_sum = float(row["final_sum"] or 0.0)
        if n == 0 or scout_sum <= 0:
            return 1.0, n
        return max(0.0, final_sum / scout_sum), n

    def _descendant_reward(self, source_key: str) -> float:
        rows = self.registry.connection.execute(
            """
            WITH RECURSIVE descendants(source_key, depth) AS (
                SELECT child_key, 1
                FROM source_edges
                WHERE parent_key = ?
                UNION
                SELECT e.child_key, d.depth + 1
                FROM source_edges e
                JOIN descendants d ON e.parent_key = d.source_key
                WHERE d.depth < ?
            )
            SELECT d.depth, COALESCE(f.final_accepted_eed, 0) AS reward
            FROM descendants d
            LEFT JOIN source_final_rewards f ON f.source_key = d.source_key
            """,
            (source_key, self.registry.max_graph_hops),
        ).fetchall()
        return sum(
            float(row["reward"])
            * (self.descendant_discount ** int(row["depth"]))
            for row in rows
        )

    def estimate(
        self,
        candidate: SourceCandidate,
        *,
        overlap_penalty: float = 0.0,
    ) -> SourceValueEstimate:
        if not 0 <= overlap_penalty <= 1:
            raise ValueError("overlap_penalty must be within [0, 1]")
        measurement = self.registry.get_scout_measurement(
            candidate.source_key
        )
        conversion, family_n = self._family_conversion(
            candidate.source_family
        )

        if measurement is None:
            direct_eed = max(0.01, candidate.scout_priority)
            elapsed = max(
                1.0,
                1.0
                + candidate.access_cost_prior
                + candidate.adapter_cost_prior,
            )
        else:
            direct_eed = (
                measurement.novel_eed_for_ranking * conversion
            )
            elapsed = max(1e-3, measurement.elapsed_seconds)

        descendant = self._descendant_reward(candidate.source_key)
        uncertainty = (
            self.exploration_weight
            * max(1.0, direct_eed)
            / math.sqrt(family_n + 1.0)
        )
        return SourceValueEstimate(
            expected_final_eed=direct_eed + descendant,
            expected_cost_seconds=elapsed,
            direct_value=direct_eed,
            descendant_value=descendant,
            uncertainty_bonus=uncertainty,
            overlap_penalty=overlap_penalty,
        )
