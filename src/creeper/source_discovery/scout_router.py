"""Typed routing between structural expansion and measured-yield scouting.

Structural discovery and source-value measurement are intentionally different
operations. A catalog that reveals many child resources is not thereby a high
novel-EED source, so this router never manufactures a measurement from link
counts.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from creeper.source_discovery.coordinator import ScoutDisposition, ScoutResult
from creeper.source_discovery.models import SourceCandidate, SourceLevel


ScoutCallable = Callable[[SourceCandidate], Awaitable[ScoutResult]]


@dataclass(frozen=True)
class SourceScoutRouterPolicy:
    """Deterministic source-class routing policy.

    Collection and MetaSource nodes are structural by default. SOURCE nodes are
    expected to produce a measured source-yield result through a family-specific
    or generic measured executor before they may become WARM.
    """

    structural_families: frozenset[str] = field(
        default_factory=lambda: frozenset({"RESOURCE_CATALOG", "RESOURCE_DIRECTORY"})
    )
    measured_families: frozenset[str] = field(
        default_factory=lambda: frozenset({"BULK_ARTIFACT"})
    )

    def __post_init__(self) -> None:
        if any(not item.strip() for item in self.structural_families):
            raise ValueError("structural family names must be non-empty")
        if any(not item.strip() for item in self.measured_families):
            raise ValueError("measured family names must be non-empty")
        overlap = self.structural_families & self.measured_families
        if overlap:
            raise ValueError(f"source families cannot have conflicting scout routes: {overlap}")

    def requires_structural_scout(self, candidate: SourceCandidate) -> bool:
        if candidate.source_family in self.measured_families:
            return False
        return (
            candidate.source_family in self.structural_families
            or candidate.level in {SourceLevel.COLLECTION, SourceLevel.METASOURCE}
        )


class SourceScoutRouter:
    """Dispatch one candidate without granting executors durable authority."""

    def __init__(
        self,
        *,
        structural_executor: ScoutCallable,
        measured_executor: ScoutCallable | None = None,
        policy: SourceScoutRouterPolicy | None = None,
    ) -> None:
        self.structural_executor = structural_executor
        self.measured_executor = measured_executor
        self.policy = policy or SourceScoutRouterPolicy()

    async def __call__(self, candidate: SourceCandidate) -> ScoutResult:
        if self.policy.requires_structural_scout(candidate):
            return await self.structural_executor(candidate)

        if self.measured_executor is not None:
            return await self.measured_executor(candidate)

        # No adapter is a deterministic implementation gap, not evidence that a
        # source is bad. HOLD preserves the candidate without inventing yield or
        # creating a transient retry loop that can never resolve by itself.
        return ScoutResult(
            ScoutDisposition.HOLD,
            reason=(
                "no measured-yield scout adapter configured for "
                f"family={candidate.source_family} level={candidate.level.value}"
            ),
        )
