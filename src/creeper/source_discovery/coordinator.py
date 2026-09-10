"""Single-host source-discovery coordinator with bounded async I/O.

The coordinator deliberately keeps durable authority in ``SourceDiscoveryRegistry``.
Search, triage, and scout executors may perform network/subprocess I/O concurrently,
but every SQLite mutation is applied serially by the coordinator task.  A POSIX
``flock`` prevents multiple local coordinators from making competing plans.
"""

from __future__ import annotations

import asyncio
import fcntl
import os
import time
from collections.abc import Awaitable, Callable
from contextlib import contextmanager
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path
from typing import Generic, TypeVar

from creeper.source_discovery.manager import SearchDirective, SourceReservoirManager
from creeper.source_discovery.models import ScoutMeasurement, SourceCandidate, SourceState
from creeper.source_discovery.registry import SourceDiscoveryRegistry


class CoordinatorBusyError(RuntimeError):
    """Raised when another local source-discovery coordinator owns the lock."""


class TriageDisposition(StrEnum):
    SCOUT = "SCOUT"
    HOLD = "HOLD"
    REJECT = "REJECT"


class ScoutDisposition(StrEnum):
    WARM = "WARM"
    HOLD = "HOLD"
    REJECT = "REJECT"


@dataclass(frozen=True)
class TriageResult:
    disposition: TriageDisposition
    reason: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "disposition", TriageDisposition(self.disposition))


@dataclass(frozen=True)
class ScoutResult:
    disposition: ScoutDisposition
    measurement: ScoutMeasurement | None = None
    reason: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "disposition", ScoutDisposition(self.disposition))
        if self.disposition is ScoutDisposition.WARM and self.measurement is None:
            raise ValueError("WARM scout result requires a deterministic measurement")


@dataclass(frozen=True)
class SearchBatch:
    """One completed search episode returned by an injected search backend.

    Ordinary no-result/provider failures should be returned as an empty batch so
    their cost is still attributed to the strategy.  Unexpected executor crashes
    are isolated by the coordinator and reported separately.
    """

    backend: str
    query: str
    actor: str
    candidates: tuple[SourceCandidate, ...] = ()
    search_cost_seconds: float | None = None

    def __post_init__(self) -> None:
        if not self.backend.strip() or not self.query.strip() or not self.actor.strip():
            raise ValueError("search batch attribution fields are required")
        if self.search_cost_seconds is not None and self.search_cost_seconds < 0:
            raise ValueError("search_cost_seconds must be non-negative")


@dataclass(frozen=True)
class CoordinatorCycleReport:
    recovered_scouts: int = 0
    activated: int = 0
    triaged_to_scout: int = 0
    triaged_hold: int = 0
    triaged_rejected: int = 0
    triage_failures: int = 0
    scouted_warm: int = 0
    scouted_hold: int = 0
    scouted_rejected: int = 0
    scout_failures: int = 0
    search_episodes: int = 0
    search_candidates_registered: int = 0
    search_candidates_dropped: int = 0
    search_failures: int = 0


T = TypeVar("T")


@dataclass(frozen=True)
class _Outcome(Generic[T]):
    value: T | None = None
    error: Exception | None = None
    elapsed_seconds: float = 0.0


TriageExecutor = Callable[[SourceCandidate], Awaitable[TriageResult]]
ScoutExecutor = Callable[[SourceCandidate], Awaitable[ScoutResult]]
SearchExecutor = Callable[[SearchDirective], Awaitable[SearchBatch]]


@contextmanager
def _coordinator_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise CoordinatorBusyError(
                f"source discovery coordinator is already running: {path}"
            ) from exc
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


async def _capture(awaitable: Awaitable[T]) -> _Outcome[T]:
    started = time.perf_counter()
    try:
        value = await awaitable
    except Exception as exc:  # independent work item failure must not cancel siblings
        return _Outcome(error=exc, elapsed_seconds=time.perf_counter() - started)
    return _Outcome(value=value, elapsed_seconds=time.perf_counter() - started)


class SourceDiscoveryCoordinator:
    """Execute one bounded discovery cycle while preserving one SQLite authority."""

    def __init__(
        self,
        registry: SourceDiscoveryRegistry,
        manager: SourceReservoirManager,
        *,
        lock_path: Path,
        triage_executor: TriageExecutor,
        scout_executor: ScoutExecutor,
        search_executor: SearchExecutor,
        triage_parallelism: int = 4,
        scout_parallelism: int | None = None,
        search_parallelism: int = 3,
        failure_retry_seconds: float = 30.0,
    ) -> None:
        if triage_parallelism < 1 or search_parallelism < 1:
            raise ValueError("coordinator parallelism must be positive")
        if scout_parallelism is None:
            scout_parallelism = manager.targets.scout_parallelism
        if scout_parallelism < 1:
            raise ValueError("scout_parallelism must be positive")
        if failure_retry_seconds <= 0:
            raise ValueError("failure_retry_seconds must be positive")
        self.registry = registry
        self.manager = manager
        self.lock_path = Path(lock_path)
        self.triage_executor = triage_executor
        self.scout_executor = scout_executor
        self.search_executor = search_executor
        self.triage_parallelism = triage_parallelism
        self.scout_parallelism = min(scout_parallelism, manager.targets.scout_parallelism)
        self.search_parallelism = search_parallelism
        self.failure_retry_seconds = float(failure_retry_seconds)
        self._startup_recovered = False

    @staticmethod
    def _failure_reason(stage: str, error: Exception) -> str:
        detail = str(error).strip().replace("\n", " ")[:400]
        return f"{stage} transient failure: {type(error).__name__}: {detail}".rstrip(": ")

    def _recover_stranded_scouts(self) -> int:
        """Recover SCOUTING rows left by a previously crashed coordinator."""
        recovered = 0
        for candidate in self.registry.list_candidates(state=SourceState.SCOUTING):
            self.registry.transition(candidate.source_key, SourceState.HOLD)
            self.registry.suppress_candidate(
                candidate,
                reason="recovered stranded SCOUTING state after coordinator restart",
                ttl_seconds=self.failure_retry_seconds,
            )
            self.registry.transition(candidate.source_key, SourceState.SCOUT_READY)
            recovered += 1
        return recovered

    async def _bounded_batch(self, items, executor, parallelism: int):
        semaphore = asyncio.Semaphore(parallelism)

        async def one(item):
            async with semaphore:
                return await _capture(executor(item))

        return await asyncio.gather(*(one(item) for item in items))

    def _claim_scouts(self, source_keys: tuple[str, ...]) -> list[SourceCandidate]:
        claimed: list[SourceCandidate] = []
        for source_key in source_keys[: self.scout_parallelism]:
            candidate = self.registry.get_candidate(source_key)
            if candidate is None or candidate.state is not SourceState.SCOUT_READY:
                continue
            if self.registry.suppression_reason(candidate) is not None:
                continue
            self.registry.transition(source_key, SourceState.SCOUTING)
            claimed.append(candidate)
        return claimed

    def _retry_failed_scout(self, candidate: SourceCandidate, error: Exception) -> None:
        self.registry.transition(candidate.source_key, SourceState.HOLD)
        self.registry.suppress_candidate(
            candidate,
            reason=self._failure_reason("scout", error),
            ttl_seconds=self.failure_retry_seconds,
        )
        self.registry.transition(candidate.source_key, SourceState.SCOUT_READY)

    def _commit_triage(
        self,
        candidates: list[SourceCandidate],
        outcomes: list[_Outcome[TriageResult]],
        counts: dict[str, int],
    ) -> None:
        for candidate, outcome in zip(candidates, outcomes, strict=True):
            current = self.registry.get_candidate(candidate.source_key)
            if current is None or current.state is not SourceState.DISCOVERED:
                continue
            if outcome.error is not None:
                self.registry.suppress_candidate(
                    current,
                    reason=self._failure_reason("triage", outcome.error),
                    ttl_seconds=self.failure_retry_seconds,
                )
                counts["triage_failures"] += 1
                continue
            result = outcome.value
            assert result is not None
            self.registry.transition(candidate.source_key, SourceState.TRIAGED)
            if result.disposition is TriageDisposition.SCOUT:
                self.registry.transition(candidate.source_key, SourceState.SCOUT_READY)
                counts["triaged_to_scout"] += 1
            elif result.disposition is TriageDisposition.HOLD:
                self.registry.transition(candidate.source_key, SourceState.HOLD)
                counts["triaged_hold"] += 1
            else:
                self.registry.transition(candidate.source_key, SourceState.REJECTED)
                counts["triaged_rejected"] += 1

    def _commit_scouts(
        self,
        candidates: list[SourceCandidate],
        outcomes: list[_Outcome[ScoutResult]],
        counts: dict[str, int],
    ) -> None:
        for candidate, outcome in zip(candidates, outcomes, strict=True):
            current = self.registry.get_candidate(candidate.source_key)
            if current is None or current.state is not SourceState.SCOUTING:
                continue
            if outcome.error is not None:
                self._retry_failed_scout(current, outcome.error)
                counts["scout_failures"] += 1
                continue
            result = outcome.value
            assert result is not None
            if result.measurement is not None:
                self.registry.record_scout_measurement(candidate.source_key, result.measurement)
            if result.disposition is ScoutDisposition.WARM:
                self.registry.transition(candidate.source_key, SourceState.WARM)
                counts["scouted_warm"] += 1
            elif result.disposition is ScoutDisposition.HOLD:
                self.registry.transition(candidate.source_key, SourceState.HOLD)
                counts["scouted_hold"] += 1
            else:
                self.registry.transition(candidate.source_key, SourceState.REJECTED)
                counts["scouted_rejected"] += 1

    def _commit_searches(
        self,
        directives: tuple[SearchDirective, ...],
        outcomes: list[_Outcome[SearchBatch]],
        counts: dict[str, int],
    ) -> None:
        for directive, outcome in zip(directives, outcomes, strict=True):
            if outcome.error is not None:
                counts["search_failures"] += 1
                continue
            batch = outcome.value
            assert batch is not None
            cost = (
                outcome.elapsed_seconds
                if batch.search_cost_seconds is None
                else batch.search_cost_seconds
            )
            episode = self.registry.begin_search_episode(
                strategy=directive.strategy,
                backend=batch.backend,
                query=batch.query,
                actor=batch.actor,
            )
            seen: set[str] = set()
            accepted: list[SourceCandidate] = []
            dropped = 0
            for candidate in batch.candidates:
                normalized = replace(
                    candidate,
                    discovered_by=batch.actor,
                    discovery_strategy=directive.strategy,
                )
                if normalized.source_key in seen:
                    dropped += 1
                    continue
                seen.add(normalized.source_key)
                if self.registry.suppression_reason(normalized) is not None:
                    dropped += 1
                    continue
                if len(accepted) >= directive.desired_candidates:
                    dropped += 1
                    continue
                accepted.append(normalized)
            for candidate in accepted:
                self.registry.register_proposal(candidate, episode_id=episode.episode_id)
            self.registry.finish_search_episode(
                episode.episode_id,
                search_cost_seconds=cost,
            )
            counts["search_episodes"] += 1
            counts["search_candidates_registered"] += len(accepted)
            counts["search_candidates_dropped"] += dropped

    async def run_once(self) -> CoordinatorCycleReport:
        """Run one finite cycle. Independent external work overlaps; commits do not."""
        with _coordinator_lock(self.lock_path):
            recovered = 0
            if not self._startup_recovered:
                recovered = self._recover_stranded_scouts()
                self._startup_recovered = True

            plan = self.manager.plan()
            counts = {
                "recovered_scouts": recovered,
                "activated": 0,
                "triaged_to_scout": 0,
                "triaged_hold": 0,
                "triaged_rejected": 0,
                "triage_failures": 0,
                "scouted_warm": 0,
                "scouted_hold": 0,
                "scouted_rejected": 0,
                "scout_failures": 0,
                "search_episodes": 0,
                "search_candidates_registered": 0,
                "search_candidates_dropped": 0,
                "search_failures": 0,
            }

            for source_key in plan.activate_source_keys:
                candidate = self.registry.get_candidate(source_key)
                if candidate is not None and candidate.state is SourceState.WARM:
                    self.registry.transition(source_key, SourceState.ACTIVE)
                    counts["activated"] += 1

            triage_candidates = [
                candidate
                for key in plan.triage_source_keys
                if (candidate := self.registry.get_candidate(key)) is not None
                and candidate.state is SourceState.DISCOVERED
                and self.registry.suppression_reason(candidate) is None
            ]
            scout_candidates = self._claim_scouts(plan.scout_source_keys)

            # Search, triage, and scout I/O are independent pipeline stages and
            # run concurrently.  No executor receives the SQLite connection.
            triage_task = self._bounded_batch(
                triage_candidates,
                self.triage_executor,
                self.triage_parallelism,
            )
            scout_task = self._bounded_batch(
                scout_candidates,
                self.scout_executor,
                self.scout_parallelism,
            )
            search_task = self._bounded_batch(
                plan.search_directives,
                self.search_executor,
                self.search_parallelism,
            )
            triage_outcomes, scout_outcomes, search_outcomes = await asyncio.gather(
                triage_task,
                scout_task,
                search_task,
            )

            # All durable mutations return to this coordinator task.  This keeps
            # the sqlite3 connection thread-confined while external I/O remains concurrent.
            self._commit_triage(triage_candidates, triage_outcomes, counts)
            self._commit_scouts(scout_candidates, scout_outcomes, counts)
            self._commit_searches(plan.search_directives, search_outcomes, counts)

            return CoordinatorCycleReport(**counts)
