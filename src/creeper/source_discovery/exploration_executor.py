"""Fast, checkpoint-safe execution of compiled deterministic regions."""

from __future__ import annotations

import inspect
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from creeper.source_discovery.enumerators import (
    CursorApiEnumerator,
    FilenamePatternEnumerator,
    HtmlCatalogEnumerator,
    IntegerPaginationEnumerator,
    StaticListEnumerator,
)
from creeper.source_discovery.models import (
    SourceCandidate,
    SourceLevel,
    SourceState,
    is_common_crawl_provenance,
)
from creeper.source_discovery.region_compilation import CompiledScoutPlan


@dataclass(frozen=True)
class RegionExecutionCheckpoint:
    """Durable resume point.

    page is the next enumerator page/ordinal to execute for query_index.
    cursor is the next cursor to issue. Counters are cumulative for the region.
    """

    query_index: int = 0
    cursor: object | None = None
    page: int | None = 0
    requests: int = 0
    bytes_read: int = 0
    results_seen: int = 0
    new_candidates: int = 0
    duplicate_candidates: int = 0

    def __post_init__(self) -> None:
        for name in (
            "query_index",
            "requests",
            "bytes_read",
            "results_seen",
            "new_candidates",
            "duplicate_candidates",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.page is not None and (
            isinstance(self.page, bool)
            or not isinstance(self.page, int)
            or self.page < 0
        ):
            raise ValueError("page must be None or a non-negative integer")

    def as_dict(self) -> dict[str, object]:
        return {
            "query_index": self.query_index,
            "cursor": self.cursor,
            "page": self.page,
            "requests": self.requests,
            "bytes_read": self.bytes_read,
            "results_seen": self.results_seen,
            "new_candidates": self.new_candidates,
            "duplicate_candidates": self.duplicate_candidates,
        }


@dataclass(frozen=True)
class NegativeObservation:
    code: str
    subject: str
    detail: str = ""


@dataclass(frozen=True)
class RegionExecutionResult:
    region_id: str
    terminal: bool
    checkpoint: RegionExecutionCheckpoint
    candidates: tuple[SourceCandidate, ...]
    negative_observations: tuple[NegativeObservation, ...] = ()
    requests: int = 0
    bytes_read: int = 0


CommitBatch = Callable[
    [tuple[SourceCandidate, ...], RegionExecutionCheckpoint],
    Awaitable[None] | None,
]


class ExplorationExecutor:
    """Execute one finite region with no persistence or LLM dependency."""

    def __init__(
        self,
        *,
        html_fetcher: Callable[..., Any] | None = None,
        api_fetcher: Callable[..., Any] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.html_fetcher = html_fetcher
        self.api_fetcher = api_fetcher
        self.clock = clock

    async def execute(
        self,
        plan: CompiledScoutPlan,
        *,
        checkpoint: RegionExecutionCheckpoint | None = None,
        commit_batch: CommitBatch | None = None,
        stop_after_batches: int | None = None,
        stop_after_pages: int | None = None,
    ) -> RegionExecutionResult:
        cp = checkpoint or RegionExecutionCheckpoint()
        queries = (
            plan.query_family.expand(max_queries=plan.hard_bounds.max_queries)
            if plan.query_family is not None
            else (plan.root,)
        )
        if cp.query_index > len(queries):
            raise ValueError("checkpoint query_index exceeds compiled query family")
        if cp.new_candidates > plan.hard_bounds.max_artifacts:
            raise ValueError("checkpoint exceeds max_artifacts")
        if cp.requests > plan.hard_bounds.max_requests:
            raise ValueError("checkpoint exceeds max_requests")
        if cp.bytes_read > plan.hard_bounds.max_bytes:
            raise ValueError("checkpoint exceeds max_bytes")

        candidates: list[SourceCandidate] = []
        negatives: list[NegativeObservation] = []
        seen: set[str] = set()
        started = self.clock()
        batches_this_run = 0
        pages_this_run = 0

        for index in range(cp.query_index, len(queries)):
            if self._budget_exhausted(plan, cp, started):
                return self._result(plan, False, cp, candidates, negatives)

            query = queries[index]
            async for batch in self._batches(plan, query=query, checkpoint=cp):
                batches_this_run += 1
                if batch.requests:
                    pages_this_run += 1

                batch_candidates, batch_duplicates = self._candidates(
                    plan, batch.artifacts, seen
                )
                remaining_artifacts = (
                    plan.hard_bounds.max_artifacts - cp.new_candidates
                )
                if len(batch_candidates) > remaining_artifacts:
                    negatives.append(
                        NegativeObservation(
                            "BATCH_EXCEEDS_ARTIFACT_BUDGET",
                            query,
                            (
                                f"batch_new={len(batch_candidates)} "
                                f"remaining={remaining_artifacts}"
                            ),
                        )
                    )
                    return self._result(plan, False, cp, candidates, negatives)

                next_requests = cp.requests + batch.requests
                next_bytes = cp.bytes_read + batch.bytes_read
                if (
                    next_requests > plan.hard_bounds.max_requests
                    or next_bytes > plan.hard_bounds.max_bytes
                ):
                    negatives.append(
                        NegativeObservation(
                            "FETCH_EXCEEDED_HARD_BUDGET",
                            query,
                            (
                                f"requests={next_requests}/"
                                f"{plan.hard_bounds.max_requests}; "
                                f"bytes={next_bytes}/{plan.hard_bounds.max_bytes}"
                            ),
                        )
                    )
                    # The fetch happened, but no candidate/checkpoint is committed.
                    # Replaying is safe; skipping the batch is not.
                    return self._result(plan, False, cp, candidates, negatives)

                next_cp = RegionExecutionCheckpoint(
                    query_index=index + (1 if batch.terminal else 0),
                    cursor=None if batch.terminal else batch.next_cursor,
                    page=0 if batch.terminal else (batch.next_page or 0),
                    requests=next_requests,
                    bytes_read=next_bytes,
                    results_seen=cp.results_seen + len(batch.artifacts),
                    new_candidates=cp.new_candidates + len(batch_candidates),
                    duplicate_candidates=(
                        cp.duplicate_candidates + batch_duplicates
                    ),
                )

                # Checkpoint progress advances only after the parent has accepted
                # every candidate emitted by the fetched batch.
                if commit_batch is not None:
                    outcome = commit_batch(tuple(batch_candidates), next_cp)
                    if inspect.isawaitable(outcome):
                        await outcome

                candidates.extend(batch_candidates)
                cp = next_cp

                if (
                    (stop_after_batches is not None and batches_this_run >= stop_after_batches)
                    or (stop_after_pages is not None and pages_this_run >= stop_after_pages)
                    or self._budget_exhausted(plan, cp, started)
                ):
                    terminal = cp.query_index >= len(queries)
                    return self._result(
                        plan, terminal, cp, candidates, negatives
                    )

                if batch.terminal:
                    break

            if cp.query_index <= index:
                return self._result(plan, False, cp, candidates, negatives)

        return self._result(
            plan,
            cp.query_index >= len(queries),
            cp,
            candidates,
            negatives,
        )

    def _budget_exhausted(
        self,
        plan: CompiledScoutPlan,
        checkpoint: RegionExecutionCheckpoint,
        started: float,
    ) -> bool:
        current = self.clock()
        return (
            checkpoint.new_candidates >= plan.hard_bounds.max_artifacts
            or checkpoint.requests >= plan.hard_bounds.max_requests
            or checkpoint.bytes_read >= plan.hard_bounds.max_bytes
            or current - started >= plan.hard_bounds.max_wall_seconds
        )

    def _batches(
        self,
        plan: CompiledScoutPlan,
        *,
        query: str,
        checkpoint: RegionExecutionCheckpoint,
    ):
        spec = plan.enumerator
        config = dict(spec.config)
        start = checkpoint.page or 0
        if spec.kind == "STATIC_LIST":
            if plan.query_family is not None:
                config = {"urls": (query,)}
                start = 0
            return StaticListEnumerator().enumerate(config=config, start=start)
        if spec.kind == "FILENAME_PATTERN":
            if plan.query_family is not None:
                config = {"template": query, "dimensions": {}}
                start = 0
            return FilenamePatternEnumerator().enumerate(
                config=config, start=start
            )
        if spec.kind == "HTML_CATALOG":
            return HtmlCatalogEnumerator().enumerate(
                config=config,
                start=start,
                max_pages=plan.hard_bounds.max_pages,
                fetcher=self.html_fetcher,
            )
        if spec.kind == "INTEGER_PAGINATION":
            return IntegerPaginationEnumerator().enumerate(
                config=config,
                start=start,
                max_pages=plan.hard_bounds.max_pages,
                fetcher=self.api_fetcher,
            )
        if spec.kind == "CURSOR_API":
            return CursorApiEnumerator().enumerate(
                config=config,
                cursor=checkpoint.cursor,
                start=start,
                max_pages=plan.hard_bounds.max_pages,
                fetcher=self.api_fetcher,
            )
        raise ValueError(f"unsupported enumerator {spec.kind}")

    @staticmethod
    def _candidates(
        plan: CompiledScoutPlan,
        urls: tuple[str, ...],
        seen: set[str],
    ) -> tuple[tuple[SourceCandidate, ...], int]:
        result: list[SourceCandidate] = []
        duplicates = 0
        predicate = plan.artifact_predicate or (lambda _: True)
        for url in urls:
            if not predicate(url):
                continue
            if is_common_crawl_provenance(
                url,
                plan.source_family,
                plan.region_id,
                plan.region_key,
            ):
                continue
            candidate = SourceCandidate(
                canonical_entrypoint=url,
                source_family=plan.source_family,
                level=SourceLevel.SOURCE,
                discovered_by=f"region:{plan.region_id}",
                discovery_strategy=(
                    f"DETERMINISTIC_REGION:{plan.region_id}:{plan.region_key}"
                ),
                confidence=1.0,
                state=SourceState.DISCOVERED,
            )
            if candidate.source_key in seen:
                duplicates += 1
                continue
            seen.add(candidate.source_key)
            result.append(candidate)
        return tuple(result), duplicates

    @staticmethod
    def _result(
        plan: CompiledScoutPlan,
        terminal: bool,
        checkpoint: RegionExecutionCheckpoint,
        candidates: list[SourceCandidate],
        negatives: list[NegativeObservation],
    ) -> RegionExecutionResult:
        return RegionExecutionResult(
            region_id=plan.region_id,
            terminal=terminal,
            checkpoint=checkpoint,
            candidates=tuple(candidates),
            negative_observations=tuple(negatives),
            requests=checkpoint.requests,
            bytes_read=checkpoint.bytes_read,
        )
