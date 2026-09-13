"""Fast, checkpoint-safe execution of compiled V7 exploration regions."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from creeper.source_discovery.enumerators import (
    CursorApiEnumerator,
    EnumeratedBatch,
    FilenamePatternEnumerator,
    HtmlCatalogEnumerator,
    IntegerPaginationEnumerator,
    StaticListEnumerator,
)
from creeper.source_discovery.models import SourceCandidate, SourceLevel, SourceState
from creeper.source_discovery.region_compilation import CompiledScoutPlan


@dataclass(frozen=True)
class RegionExecutionCheckpoint:
    query_index: int = 0
    cursor: object | None = None
    page: int | None = None
    requests: int = 0
    bytes_read: int = 0
    results_seen: int = 0


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


CommitBatch = Callable[[tuple[SourceCandidate, ...], RegionExecutionCheckpoint], Awaitable[None] | None]


class ExplorationExecutor:
    """Execute one finite region; parent callbacks own persistence and global dedup."""

    def __init__(self, *, html_fetcher: Callable[..., Any] | None = None, api_fetcher: Callable[..., Any] | None = None, clock: Callable[[], float] = time.monotonic) -> None:
        self.html_fetcher = html_fetcher
        self.api_fetcher = api_fetcher
        self.clock = clock

    async def execute(
        self,
        plan: CompiledScoutPlan,
        *,
        checkpoint: RegionExecutionCheckpoint | None = None,
        commit_batch: CommitBatch | None = None,
        stop_after_pages: int | None = None,
    ) -> RegionExecutionResult:
        cp = checkpoint or RegionExecutionCheckpoint()
        if cp.query_index < 0 or cp.requests < 0 or cp.bytes_read < 0 or cp.results_seen < 0:
            raise ValueError("checkpoint counters must be non-negative")
        if plan.query_family is not None:
            queries = plan.query_family.expand(max_queries=plan.hard_bounds.max_queries)
        else:
            queries = (plan.root,)
        if cp.query_index > len(queries):
            raise ValueError("checkpoint query_index exceeds compiled query family")

        candidates: list[SourceCandidate] = []
        negatives: list[NegativeObservation] = []
        seen: set[str] = set()
        started = self.clock()
        pages_this_run = 0
        for index in range(cp.query_index, len(queries)):
            query = queries[index]
            batches = self._batches(plan, query=query, checkpoint=cp)
            async for batch in batches:
                pages_this_run += 1
                next_cp = RegionExecutionCheckpoint(
                    query_index=index + (1 if batch.terminal else 0),
                    cursor=batch.next_cursor if batch.next_cursor is not None else batch.cursor,
                    page=batch.page,
                    requests=cp.requests + batch.requests,
                    bytes_read=cp.bytes_read + batch.bytes_read,
                    results_seen=cp.results_seen + len(batch.artifacts),
                )
                if next_cp.requests > plan.hard_bounds.max_requests or next_cp.bytes_read > plan.hard_bounds.max_bytes:
                    return RegionExecutionResult(plan.region_id, False, next_cp, tuple(candidates), tuple(negatives), next_cp.requests, next_cp.bytes_read)
                batch_candidates = self._candidates(plan, batch.artifacts, seen)
                if batch_candidates and commit_batch is not None:
                    outcome = commit_batch(tuple(batch_candidates), next_cp)
                    if inspect.isawaitable(outcome):
                        await outcome
                candidates.extend(batch_candidates)
                cp = next_cp
                if (
                    pages_this_run >= stop_after_pages
                    or cp.requests >= plan.hard_bounds.max_requests
                    or self.clock() - started >= plan.hard_bounds.max_wall_seconds
                ):
                    return RegionExecutionResult(plan.region_id, False, cp, tuple(candidates), tuple(negatives), cp.requests, cp.bytes_read)
                if batch.terminal:
                    break
            if cp.query_index <= index:
                break

        terminal = cp.query_index >= len(queries) and (plan.query_family is not None or plan.enumerator.kind in {"STATIC_LIST", "FILENAME_PATTERN"})
        return RegionExecutionResult(plan.region_id, terminal, cp, tuple(candidates), tuple(negatives), cp.requests, cp.bytes_read)

    def _batches(self, plan: CompiledScoutPlan, *, query: str, checkpoint: RegionExecutionCheckpoint):
        spec = plan.enumerator
        config = dict(spec.config)
        if spec.kind == "STATIC_LIST":
            config.setdefault("urls", (query,) if plan.query_family else config.get("urls", ()))
            return StaticListEnumerator().enumerate(config=config, start=checkpoint.page or 0)
        if spec.kind == "FILENAME_PATTERN":
            if plan.query_family is not None:
                config = {"template": query, "dimensions": {}}
            return FilenamePatternEnumerator().enumerate(config=config, start=checkpoint.page or 0)
        if spec.kind == "HTML_CATALOG":
            return HtmlCatalogEnumerator().enumerate(config=config, fetcher=self.html_fetcher, max_pages=plan.hard_bounds.max_pages)
        if spec.kind == "INTEGER_PAGINATION":
            return IntegerPaginationEnumerator().enumerate(config=config, start=checkpoint.page or 0, max_pages=plan.hard_bounds.max_pages, fetcher=self.api_fetcher)
        if spec.kind == "CURSOR_API":
            return CursorApiEnumerator().enumerate(config=config, cursor=checkpoint.cursor, start=checkpoint.page or 0, max_pages=plan.hard_bounds.max_pages, fetcher=self.api_fetcher)
        raise ValueError(f"unsupported enumerator {spec.kind}")

    @staticmethod
    def _candidates(plan: CompiledScoutPlan, urls: tuple[str, ...], seen: set[str]) -> tuple[SourceCandidate, ...]:
        result: list[SourceCandidate] = []
        predicate = plan.artifact_predicate or (lambda _: True)
        for url in urls:
            if not predicate(url):
                continue
            candidate = SourceCandidate(
                canonical_entrypoint=url,
                source_family=plan.source_family,
                level=SourceLevel.SOURCE,
                discovered_by=f"region:{plan.region_id}",
                discovery_strategy=f"DETERMINISTIC_REGION:{plan.region_key}",
                expected_year_from=None,
                expected_year_to=None,
                confidence=1.0,
                state=SourceState.DISCOVERED,
            )
            if candidate.source_key in seen:
                continue
            seen.add(candidate.source_key)
            result.append(candidate)
        return tuple(result)
