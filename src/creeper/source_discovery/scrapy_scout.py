"""Pure Scrapy structural scouting for source-graph expansion.

This adapter performs only subprocess/file I/O and deterministic promotion. It
never receives a registry or SQLite connection. The resulting child candidates
are committed later by ``SourceDiscoveryCoordinator`` on its serialized authority
path.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from creeper.source_discovery.coordinator import ScoutDisposition, ScoutResult
from creeper.source_discovery.models import SourceCandidate
from creeper.source_discovery.promotion import LinkPromotionAccumulator, PromotionPolicy
from creeper.source_discovery.scrapy_sidecar import (
    ScrapyScoutLauncher,
    ScrapyScoutSpec,
    iter_scrapy_link_discoveries,
)


@dataclass(frozen=True)
class ScrapyStructuralScoutPolicy:
    max_pages: int = 100
    max_depth: int = 2
    max_seconds: int = 120
    max_memory_mb: int = 512
    follow_query: bool = False

    def __post_init__(self) -> None:
        for name in ("max_pages", "max_seconds", "max_memory_mb"):
            value = getattr(self, name)
            if not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if not isinstance(self.max_depth, int) or self.max_depth < 0:
            raise ValueError("max_depth must be a non-negative integer")


class ScrapyStructuralScoutExecutor:
    """Turn one bounded Scrapy crawl into deterministic child source proposals.

    A successful retry parses the complete committed spool prefix, not only the
    latest append range. This is intentional: resource promotion may require
    corroboration from links emitted before a previous crash. Failed/hung runs
    raise so the coordinator leaves the parent retryable and commits no partial
    child set.
    """

    def __init__(
        self,
        launcher: ScrapyScoutLauncher,
        work_root: Path,
        *,
        policy: ScrapyStructuralScoutPolicy | None = None,
        promotion_policy: PromotionPolicy | None = None,
    ) -> None:
        self.launcher = launcher
        self.work_root = Path(work_root).resolve()
        self.policy = policy or ScrapyStructuralScoutPolicy()
        self.promotion_policy = promotion_policy or PromotionPolicy()

    def _spec(self, candidate: SourceCandidate) -> ScrapyScoutSpec:
        digest = candidate.source_key.removeprefix("src:")
        return ScrapyScoutSpec(
            source_key=candidate.source_key,
            start_url=candidate.canonical_entrypoint,
            jobdir=self.work_root / "jobdir" / digest,
            spool_path=self.work_root / "spool" / f"{digest}.jsonl",
            max_pages=self.policy.max_pages,
            max_depth=self.policy.max_depth,
            max_seconds=self.policy.max_seconds,
            max_memory_mb=self.policy.max_memory_mb,
            follow_query=self.policy.follow_query,
        )

    async def __call__(self, candidate: SourceCandidate) -> ScoutResult:
        spec = self._spec(candidate)
        run = await self.launcher.run_async(spec)
        if run.timed_out:
            raise TimeoutError(
                f"Scrapy structural scout exceeded hard timeout for {candidate.source_key}"
            )
        if run.returncode != 0:
            raise RuntimeError(
                f"Scrapy structural scout failed rc={run.returncode} for {candidate.source_key}"
            )

        accumulator = LinkPromotionAccumulator(policy=self.promotion_policy)
        if run.spool_end_offset > 0 and run.spool_path.exists():
            for link in iter_scrapy_link_discoveries(
                run.spool_path,
                expected_source_key=candidate.source_key,
                start_offset=0,
                end_offset=run.spool_end_offset,
            ):
                if accumulator.input_links >= self.promotion_policy.max_input_links:
                    break
                accumulator.add(link)

        promotions = accumulator.promoted(
            discovered_by="scrapy_sidecar",
            discovery_strategy="DETERMINISTIC_LINK_EXPANSION",
        )
        return ScoutResult(
            ScoutDisposition.HOLD,
            reason=(
                f"structural scout inspected {accumulator.input_links} committed links; "
                f"promoted {len(promotions)} child sources"
            ),
            discovered_candidates=tuple(item.candidate for item in promotions),
            edge_relation="links_to_resource",
        )
