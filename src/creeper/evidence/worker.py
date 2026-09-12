"""Durable asynchronous evidence consumer.

The queue authority remains Creeper's ControlStore because EvidenceQueryKey and
its terminal states are competition semantics, not generic job metadata. The
worker follows the mature ACK/visibility-timeout pattern used by persistent
queues: claim a bounded supported-provider batch, renew visibility while slow
network work is in flight, then either commit a terminal result or release
retryable work with a durable retry time.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass
from typing import Protocol

from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceQueryKey,
    EvidenceQueryResult,
    RangeEvidenceQueryResult,
    TemporalScope,
)
from creeper.storage.commit_writer import CommitWriter
from creeper.storage.control_store import ControlStore, EvidenceTask
from creeper.storage.evidence_queue import DurableEvidenceQueue
from creeper.storage.evidence_store import EvidenceStore


class AsyncEvidenceProvider(Protocol):
    async def query_key(self, key: EvidenceQueryKey) -> EvidenceQueryResult:
        """Execute one already-claimed durable query key."""

    async def query_range(self, key: EvidenceQueryKey) -> RangeEvidenceQueryResult:
        """Probe a multi-year scope without committing annual evidence."""


@dataclass(frozen=True)
class EvidenceWorkerReport:
    claimed: int = 0
    terminal: int = 0
    retryable: int = 0
    inserted_capsules: int = 0
    unknown_provider: int = 0
    pass_count: int = 0
    empty_exhaustive_count: int = 0
    decomposed_count: int = 0
    invalid_count: int = 0
    incomplete_count: int = 0
    transient_error_count: int = 0
    provider_http_requests_total: int = 0
    provider_throttle_responses_total: int = 0


class AsyncEvidenceWorker:
    """Pull durable evidence work and execute it with bounded concurrency."""

    def __init__(
        self,
        *,
        control_store: ControlStore,
        evidence_store: EvidenceStore,
        providers: Mapping[str, AsyncEvidenceProvider],
        owner: str,
        claim_batch_size: int = 16,
        lease_seconds: float = 300.0,
        provider_inflight: Mapping[str, int] | None = None,
        retry_base_seconds: float = 30.0,
        retry_max_seconds: float = 3_600.0,
        heartbeat_interval: float | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not owner:
            raise ValueError("owner is required")
        if claim_batch_size < 1:
            raise ValueError("claim_batch_size must be positive")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if retry_base_seconds < 0 or retry_max_seconds < retry_base_seconds:
            raise ValueError("invalid persistent retry bounds")
        if not providers:
            raise ValueError("at least one evidence provider is required")
        if heartbeat_interval is None:
            heartbeat_interval = max(1.0, min(60.0, lease_seconds / 3.0))
        if heartbeat_interval <= 0 or heartbeat_interval >= lease_seconds:
            raise ValueError("heartbeat_interval must be positive and below lease_seconds")

        self.control_store = control_store
        self.evidence_store = evidence_store
        self.providers = dict(providers)
        self.owner = owner
        self.claim_batch_size = claim_batch_size
        self.lease_seconds = lease_seconds
        self.retry_base_seconds = retry_base_seconds
        self.retry_max_seconds = retry_max_seconds
        self.heartbeat_interval = float(heartbeat_interval)
        self.clock = clock
        self.queue = DurableEvidenceQueue(control_store)
        # Cumulative operational wait-state counters. The service snapshots
        # deltas into RuntimeTelemetryStore; they never affect evidence state.
        self.host_lock_wait_milliseconds = 0
        self.provider_inflight_wait_milliseconds = 0
        self.claim_wait_milliseconds = 0
        # Streaming-pump telemetry. These counters describe queue refill
        # mechanics only and never participate in evidence authority.
        self.stream_refill_claims = 0
        self.stream_refill_tasks = 0
        self.stream_refill_empty_claims = 0

        limits = dict(provider_inflight or {})
        unknown_limits = set(limits) - set(self.providers)
        if unknown_limits:
            raise KeyError(f"inflight configured for unknown providers: {sorted(unknown_limits)}")
        self._semaphores: dict[str, asyncio.Semaphore] = {}
        # Bounded striped locks prevent overlapping queries for the same host
        # without retaining one Lock per hostname across a multi-million task run.
        self._host_lock_stripes = tuple(asyncio.Lock() for _ in range(4096))
        for provider in self.providers:
            limit = limits.get(provider, 4)
            if not isinstance(limit, int) or limit < 1:
                raise ValueError("provider inflight limits must be positive integers")
            self._semaphores[provider] = asyncio.Semaphore(limit)

    def _retry_at(self, attempt: int) -> float:
        exponent = max(0, int(attempt) - 1)
        delay = min(
            self.retry_max_seconds,
            self.retry_base_seconds * (2**exponent),
        )
        return float(self.clock()) + delay

    @staticmethod
    def _transient_for(
        task: EvidenceTask, error: str
    ) -> EvidenceQueryResult | RangeEvidenceQueryResult:
        scope = task.key.temporal_scope
        if scope.year_from != scope.year_to:
            return RangeEvidenceQueryResult(
                hostname=task.key.hostname,
                key=task.key,
                state=CDXQueryState.TRANSIENT_ERROR,
                error=error,
            )
        return EvidenceQueryResult(
            hostname=task.key.hostname,
            year=scope.year_from,
            state=CDXQueryState.TRANSIENT_ERROR,
            error=error,
            key=task.key,
        )

    def _record_attempt_metric(
        self,
        task: EvidenceTask,
        result: EvidenceQueryResult | RangeEvidenceQueryResult,
    ) -> None:
        self.control_store.record_evidence_task_attempt_metric(
            result.key or task.key,
            attempt=task.attempt,
            state=result.state,
            provider_requests=result.provider_requests,
            provider_elapsed_milliseconds=result.provider_elapsed_milliseconds,
            pages_seen=result.pages_seen,
            records_seen=result.records_seen,
        )

    async def _execute(self, task: EvidenceTask) -> EvidenceQueryResult:
        provider = self.providers[task.key.provider]
        semaphore = self._semaphores[task.key.provider]
        host_identity = (
            task.key.provider + "\0" + task.key.hostname
        ).encode("utf-8")
        stripe = int.from_bytes(
            hashlib.blake2s(host_identity, digest_size=4).digest(),
            "big",
        ) % len(self._host_lock_stripes)
        host_lock = self._host_lock_stripes[stripe]
        # Same-host serialization is a semantic guard, not provider capacity.
        # Acquire it before the provider semaphore so a duplicate/same-stripe
        # waiter cannot consume an inflight slot while doing no network work.
        loop = asyncio.get_running_loop()
        host_started = loop.time()
        await host_lock.acquire()
        self.host_lock_wait_milliseconds += max(
            0,
            int(round((loop.time() - host_started) * 1000.0)),
        )
        try:
            inflight_started = loop.time()
            await semaphore.acquire()
            self.provider_inflight_wait_milliseconds += max(
                0,
                int(round((loop.time() - inflight_started) * 1000.0)),
            )
            try:
                scope = task.key.temporal_scope
                if scope.year_from != scope.year_to:
                    query_range = getattr(provider, "query_range", None)
                    if query_range is None:
                        raise ValueError(
                            f"provider {task.key.provider!r} does not support range probes"
                        )
                    return await query_range(task.key)
                return await provider.query_key(task.key)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # operational failure, never evidence INVALID
                return self._transient_for(task, str(exc) or type(exc).__name__)
            finally:
                semaphore.release()
        finally:
            host_lock.release()

    async def _heartbeat(
        self,
        active_keys: set[EvidenceQueryKey],
        stop: asyncio.Event,
    ) -> None:
        """Keep visibility alive only for results that are still in flight.

        Completed tasks are removed from the active set immediately after their
        durable state transition, so terminal work is not renewed again.
        """
        renewal_seconds = self.lease_seconds + self.heartbeat_interval
        while True:
            if stop.is_set():
                return
            keys = tuple(active_keys)
            if keys:
                renewed = self.queue.renew(
                    keys,
                    owner=self.owner,
                    lease_seconds=renewal_seconds,
                )
                if renewed != len(keys):
                    raise RuntimeError(
                        "lost ownership while renewing evidence visibility"
                    )
            try:
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=self.heartbeat_interval,
                )
                return
            except TimeoutError:
                continue

    async def run_once(self) -> EvidenceWorkerReport:
        """Claim and process one bounded durable batch.

        Provider work remains bounded by the per-provider semaphore, but durable
        completion is incremental: as soon as one task finishes it is committed
        and its queue slot is released. A slow tail request therefore no longer
        holds the rest of the batch before SourceProducer can observe headroom.
        """
        loop = asyncio.get_running_loop()
        claim_started = loop.time()
        tasks = self.queue.claim(
            owner=self.owner,
            limit=self.claim_batch_size,
            providers=self.providers,
            lease_seconds=self.lease_seconds,
        )
        self.claim_wait_milliseconds += max(
            0,
            int(round((loop.time() - claim_started) * 1000.0)),
        )
        if not tasks:
            return EvidenceWorkerReport()

        task_by_key = {task.key: task for task in tasks}
        active_keys = set(task_by_key)
        stop_heartbeat = asyncio.Event()
        heartbeat = asyncio.create_task(
            self._heartbeat(active_keys, stop_heartbeat)
        )
        executions = [
            asyncio.create_task(self._execute(task))
            for task in tasks
        ]
        writer = CommitWriter(
            self.evidence_store,
            self.control_store,
            owner=self.owner,
            flush_count=1,
        )
        terminal = retryable = range_inserted_capsules = 0
        state_counts = {
            CDXQueryState.PASS: 0,
            CDXQueryState.EMPTY_EXHAUSTIVE: 0,
            CDXQueryState.DECOMPOSED: 0,
            CDXQueryState.INVALID: 0,
            CDXQueryState.INCOMPLETE: 0,
            CDXQueryState.TRANSIENT_ERROR: 0,
        }

        try:
            for completed in asyncio.as_completed(executions):
                result = await completed
                if result.key is None:
                    raise ValueError(
                        "provider result must preserve EvidenceQueryKey"
                    )
                state_counts[result.state] += 1
                task = task_by_key[result.key]
                self._record_attempt_metric(task, result)

                if isinstance(result, RangeEvidenceQueryResult):
                    if result.capsules:
                        self.control_store.attribute_task_host_years(
                            result.key,
                            (capsule.year for capsule in result.capsules),
                        )
                        range_inserted_capsules += self.evidence_store.put_many(
                            result.capsules
                        )
                    capsule_years = {
                        capsule.year for capsule in result.capsules
                    }
                    if result.state in {
                        CDXQueryState.PASS,
                        CDXQueryState.EMPTY_EXHAUSTIVE,
                        CDXQueryState.DECOMPOSED,
                        CDXQueryState.INVALID,
                    }:
                        followups = ()
                        if result.state is CDXQueryState.DECOMPOSED:
                            followups = tuple(
                                EvidenceQueryKey(
                                    result.hostname,
                                    TemporalScope(year, year),
                                    result.key.provider,
                                    result.key.policy_version,
                                )
                                for year in result.followup_years
                            )
                        elif result.state is CDXQueryState.PASS:
                            followups = tuple(
                                EvidenceQueryKey(
                                    result.hostname,
                                    TemporalScope(year, year),
                                    result.key.provider,
                                    result.key.policy_version,
                                )
                                for year in result.candidate_years
                                if year not in capsule_years
                            )
                        self.control_store.finish_range_task(
                            result.key,
                            result.state,
                            followup_keys=followups,
                            owner=self.owner,
                        )
                        terminal += 1
                    elif result.state in {
                        CDXQueryState.INCOMPLETE,
                        CDXQueryState.TRANSIENT_ERROR,
                    }:
                        self.control_store.finish_evidence_task(
                            result.key,
                            result.state,
                            owner=self.owner,
                            retry_at=self._retry_at(task.attempt),
                        )
                        retryable += 1
                    else:
                        raise ValueError(
                            f"unsupported provider state: {result.state}"
                        )
                elif result.state in {
                    CDXQueryState.PASS,
                    CDXQueryState.EMPTY_EXHAUSTIVE,
                    CDXQueryState.INVALID,
                }:
                    writer.submit(result.capsule, result)
                    terminal += 1
                elif result.state in {
                    CDXQueryState.INCOMPLETE,
                    CDXQueryState.TRANSIENT_ERROR,
                }:
                    self.control_store.finish_evidence_task(
                        result.key,
                        result.state,
                        owner=self.owner,
                        retry_at=self._retry_at(task.attempt),
                    )
                    retryable += 1
                else:
                    raise ValueError(
                        f"unsupported provider state: {result.state}"
                    )

                active_keys.discard(result.key)
        finally:
            for execution in executions:
                if not execution.done():
                    execution.cancel()
            await asyncio.gather(*executions, return_exceptions=True)
            writer.close()
            stop_heartbeat.set()
            await heartbeat

        return EvidenceWorkerReport(
            claimed=len(tasks),
            terminal=terminal,
            retryable=retryable,
            inserted_capsules=(
                writer.inserted_capsules + range_inserted_capsules
            ),
            unknown_provider=0,
            pass_count=state_counts[CDXQueryState.PASS],
            empty_exhaustive_count=state_counts[
                CDXQueryState.EMPTY_EXHAUSTIVE
            ],
            decomposed_count=state_counts[CDXQueryState.DECOMPOSED],
            invalid_count=state_counts[CDXQueryState.INVALID],
            incomplete_count=state_counts[CDXQueryState.INCOMPLETE],
            transient_error_count=state_counts[
                CDXQueryState.TRANSIENT_ERROR
            ],
        )


    async def run_streaming(
        self,
        *,
        stop_event: asyncio.Event,
        refill_batch_size: int | None = None,
    ) -> AsyncIterator[EvidenceWorkerReport]:
        """Continuously refill one claimed work window as tasks complete.

        ``run_once`` intentionally retains bounded batch semantics for tests and
        one-shot callers. Production services should use this method instead:
        the durable claim window is replenished before the current window drains,
        eliminating the 64 -> 0 -> 64 barrier that otherwise starves the provider
        rate limiter at every slow batch tail.

        A stop request is graceful: no new durable work is claimed after the
        event is set, while already-owned tasks keep their visibility heartbeat
        and drain to a durable state before the generator returns.
        """
        if refill_batch_size is None:
            refill_batch_size = max(1, min(16, self.claim_batch_size))
        if refill_batch_size < 1 or refill_batch_size > self.claim_batch_size:
            raise ValueError(
                "refill_batch_size must be between 1 and claim_batch_size"
            )
        low_watermark = max(0, self.claim_batch_size - refill_batch_size)
        loop = asyncio.get_running_loop()

        def claim(limit: int, *, refill: bool) -> list[EvidenceTask]:
            if limit < 1:
                return []
            started = loop.time()
            tasks = self.queue.claim(
                owner=self.owner,
                limit=limit,
                providers=self.providers,
                lease_seconds=self.lease_seconds,
            )
            self.claim_wait_milliseconds += max(
                0,
                int(round((loop.time() - started) * 1000.0)),
            )
            if refill:
                self.stream_refill_claims += 1
                self.stream_refill_tasks += len(tasks)
                if not tasks:
                    self.stream_refill_empty_claims += 1
            return tasks

        initial = claim(self.claim_batch_size, refill=False)
        if not initial:
            return

        active_keys: set[EvidenceQueryKey] = set()
        executions: dict[asyncio.Task, EvidenceTask] = {}

        def launch(tasks: list[EvidenceTask]) -> None:
            for task in tasks:
                active_keys.add(task.key)
                execution = asyncio.create_task(self._execute(task))
                executions[execution] = task

        launch(initial)
        claimed = len(initial)
        terminal = retryable = inserted_capsules = 0
        state_counts = {
            CDXQueryState.PASS: 0,
            CDXQueryState.EMPTY_EXHAUSTIVE: 0,
            CDXQueryState.DECOMPOSED: 0,
            CDXQueryState.INVALID: 0,
            CDXQueryState.INCOMPLETE: 0,
            CDXQueryState.TRANSIENT_ERROR: 0,
        }
        completed_since_yield = 0
        writer = CommitWriter(
            self.evidence_store,
            self.control_store,
            owner=self.owner,
            flush_count=1,
        )
        stop_heartbeat = asyncio.Event()
        heartbeat = asyncio.create_task(
            self._heartbeat(active_keys, stop_heartbeat)
        )

        def snapshot_and_reset() -> EvidenceWorkerReport:
            nonlocal claimed, terminal, retryable, inserted_capsules
            nonlocal completed_since_yield, state_counts
            report = EvidenceWorkerReport(
                claimed=claimed,
                terminal=terminal,
                retryable=retryable,
                inserted_capsules=inserted_capsules,
                unknown_provider=0,
                pass_count=state_counts[CDXQueryState.PASS],
                empty_exhaustive_count=state_counts[
                    CDXQueryState.EMPTY_EXHAUSTIVE
                ],
                decomposed_count=state_counts[CDXQueryState.DECOMPOSED],
                invalid_count=state_counts[CDXQueryState.INVALID],
                incomplete_count=state_counts[CDXQueryState.INCOMPLETE],
                transient_error_count=state_counts[
                    CDXQueryState.TRANSIENT_ERROR
                ],
            )
            claimed = terminal = retryable = inserted_capsules = 0
            completed_since_yield = 0
            state_counts = {
                CDXQueryState.PASS: 0,
                CDXQueryState.EMPTY_EXHAUSTIVE: 0,
                CDXQueryState.DECOMPOSED: 0,
                CDXQueryState.INVALID: 0,
                CDXQueryState.INCOMPLETE: 0,
                CDXQueryState.TRANSIENT_ERROR: 0,
            }
            return report

        try:
            while executions:
                done, _pending = await asyncio.wait(
                    tuple(executions),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for execution in done:
                    task = executions.pop(execution)
                    result = await execution
                    if result.key is None:
                        raise ValueError(
                            "provider result must preserve EvidenceQueryKey"
                        )
                    state_counts[result.state] += 1
                    self._record_attempt_metric(task, result)

                    if isinstance(result, RangeEvidenceQueryResult):
                        if result.capsules:
                            self.control_store.attribute_task_host_years(
                                result.key,
                                (capsule.year for capsule in result.capsules),
                            )
                            inserted_capsules += self.evidence_store.put_many(
                                result.capsules
                            )
                        capsule_years = {
                            capsule.year for capsule in result.capsules
                        }
                        if result.state in {
                            CDXQueryState.PASS,
                            CDXQueryState.EMPTY_EXHAUSTIVE,
                            CDXQueryState.DECOMPOSED,
                            CDXQueryState.INVALID,
                        }:
                            followups = ()
                            if result.state is CDXQueryState.DECOMPOSED:
                                followups = tuple(
                                    EvidenceQueryKey(
                                        result.hostname,
                                        TemporalScope(year, year),
                                        result.key.provider,
                                        result.key.policy_version,
                                    )
                                    for year in result.followup_years
                                )
                            elif result.state is CDXQueryState.PASS:
                                followups = tuple(
                                    EvidenceQueryKey(
                                        result.hostname,
                                        TemporalScope(year, year),
                                        result.key.provider,
                                        result.key.policy_version,
                                    )
                                    for year in result.candidate_years
                                    if year not in capsule_years
                                )
                            self.control_store.finish_range_task(
                                result.key,
                                result.state,
                                followup_keys=followups,
                                owner=self.owner,
                            )
                            terminal += 1
                        elif result.state in {
                            CDXQueryState.INCOMPLETE,
                            CDXQueryState.TRANSIENT_ERROR,
                        }:
                            self.control_store.finish_evidence_task(
                                result.key,
                                result.state,
                                owner=self.owner,
                                retry_at=self._retry_at(task.attempt),
                            )
                            retryable += 1
                        else:
                            raise ValueError(
                                f"unsupported provider state: {result.state}"
                            )
                    elif result.state in {
                        CDXQueryState.PASS,
                        CDXQueryState.EMPTY_EXHAUSTIVE,
                        CDXQueryState.INVALID,
                    }:
                        before_inserted = writer.inserted_capsules
                        writer.submit(result.capsule, result)
                        inserted_capsules += (
                            writer.inserted_capsules - before_inserted
                        )
                        terminal += 1
                    elif result.state in {
                        CDXQueryState.INCOMPLETE,
                        CDXQueryState.TRANSIENT_ERROR,
                    }:
                        self.control_store.finish_evidence_task(
                            result.key,
                            result.state,
                            owner=self.owner,
                            retry_at=self._retry_at(task.attempt),
                        )
                        retryable += 1
                    else:
                        raise ValueError(
                            f"unsupported provider state: {result.state}"
                        )

                    active_keys.discard(result.key)
                    completed_since_yield += 1

                if (
                    not stop_event.is_set()
                    and len(executions) <= low_watermark
                ):
                    capacity = self.claim_batch_size - len(executions)
                    # Refill all the way to the durable claim-window high
                    # watermark. The refill_batch_size is a hysteresis/reporting
                    # threshold, not a cap: multiple tasks can complete in one
                    # event-loop turn, and replacing only one fixed chunk would
                    # recreate a smaller batch-tail starvation window.
                    refill = claim(capacity, refill=True)
                    launch(refill)
                    claimed += len(refill)

                if (
                    completed_since_yield >= refill_batch_size
                    or not executions
                ):
                    report = snapshot_and_reset()
                    if report.claimed or report.terminal or report.retryable:
                        yield report
        finally:
            for execution in executions:
                if not execution.done():
                    execution.cancel()
            await asyncio.gather(*executions, return_exceptions=True)
            writer.close()
            stop_heartbeat.set()
            await heartbeat

    async def run_until_idle(self, *, max_batches: int | None = None) -> EvidenceWorkerReport:
        """Drain currently claimable work without busy-polling future retries."""
        if max_batches is not None and max_batches < 1:
            raise ValueError("max_batches must be positive")
        total = EvidenceWorkerReport()
        batches = 0
        while max_batches is None or batches < max_batches:
            report = await self.run_once()
            if report.claimed == 0:
                break
            total = EvidenceWorkerReport(
                claimed=total.claimed + report.claimed,
                terminal=total.terminal + report.terminal,
                retryable=total.retryable + report.retryable,
                inserted_capsules=total.inserted_capsules + report.inserted_capsules,
                unknown_provider=total.unknown_provider + report.unknown_provider,
                pass_count=total.pass_count + report.pass_count,
                empty_exhaustive_count=(
                    total.empty_exhaustive_count + report.empty_exhaustive_count
                ),
                decomposed_count=total.decomposed_count + report.decomposed_count,
                invalid_count=total.invalid_count + report.invalid_count,
                incomplete_count=total.incomplete_count + report.incomplete_count,
                transient_error_count=(
                    total.transient_error_count + report.transient_error_count
                ),
            )
            batches += 1
        return total
