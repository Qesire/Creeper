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
from collections.abc import Callable, Mapping
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

        limits = dict(provider_inflight or {})
        unknown_limits = set(limits) - set(self.providers)
        if unknown_limits:
            raise KeyError(f"inflight configured for unknown providers: {sorted(unknown_limits)}")
        self._semaphores: dict[str, asyncio.Semaphore] = {}
        # Bounded striped locks prevent overlapping queries for the same host
        # without retaining one Lock per hostname across a multi-million task run.
        self._host_lock_stripes = tuple(asyncio.Lock() for _ in range(256))
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
        async with semaphore:
            async with host_lock:
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

    async def _heartbeat(
        self,
        keys: tuple[EvidenceQueryKey, ...],
        stop: asyncio.Event,
    ) -> None:
        """Keep visibility alive while a claimed network batch is running."""
        renewal_seconds = self.lease_seconds + self.heartbeat_interval
        while True:
            # Renew before sleeping so newly claimed work immediately gains one
            # heartbeat interval of scheduler-jitter headroom. This keeps slow
            # network tasks invisible even if the event loop is briefly delayed
            # by synchronous SQLite commits elsewhere in the process.
            renewed = self.queue.renew(
                keys,
                owner=self.owner,
                lease_seconds=renewal_seconds,
            )
            if renewed != len(keys):
                raise RuntimeError("lost ownership while renewing evidence visibility")
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

        Existing PENDING/INCOMPLETE/TRANSIENT_ERROR rows are eligible, so this
        worker can resume backlog created by an earlier process or SourceLease.
        Tasks for unconfigured providers remain untouched for the appropriate
        provider worker instead of being claimed and churned through retries.
        """
        tasks = self.queue.claim(
            owner=self.owner,
            limit=self.claim_batch_size,
            providers=self.providers,
            lease_seconds=self.lease_seconds,
        )
        if not tasks:
            return EvidenceWorkerReport()

        keys = tuple(task.key for task in tasks)
        stop_heartbeat = asyncio.Event()
        heartbeat = asyncio.create_task(self._heartbeat(keys, stop_heartbeat))
        try:
            results = await asyncio.gather(*(self._execute(task) for task in tasks))
        finally:
            stop_heartbeat.set()
            await heartbeat

        writer = CommitWriter(
            self.evidence_store,
            self.control_store,
            owner=self.owner,
            flush_count=max(1, min(self.claim_batch_size, 128)),
        )
        terminal = retryable = 0
        range_capsules = [
            capsule
            for result in results
            if isinstance(result, RangeEvidenceQueryResult)
            for capsule in result.capsules
        ]
        # Commit all observed range positives in one transaction before any
        # parent range task is made terminal. A crash after this point is safe:
        # range retries merely hit EvidenceStore's idempotent primary key.
        range_inserted_capsules = self.evidence_store.put_many(range_capsules)
        task_by_key = {task.key: task for task in tasks}
        try:
            for result in results:
                if result.key is None:
                    raise ValueError("provider result must preserve EvidenceQueryKey")
                if isinstance(result, RangeEvidenceQueryResult):
                    # Positive CDX rows are individually valid evidence even if
                    # a later page makes the parent range retryable. Persist
                    # them first; duplicate retries are idempotent in
                    # EvidenceStore. Only candidate years not backed by a
                    # capsule fall back to exact-year provider tasks.
                    capsule_years = {capsule.year for capsule in result.capsules}
                    if result.state in {
                        CDXQueryState.PASS,
                        CDXQueryState.EMPTY_EXHAUSTIVE,
                        CDXQueryState.INVALID,
                    }:
                        followups = ()
                        if result.state is CDXQueryState.PASS:
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
                        continue
                    if result.state not in {
                        CDXQueryState.INCOMPLETE,
                        CDXQueryState.TRANSIENT_ERROR,
                    }:
                        raise ValueError(f"unsupported provider state: {result.state}")
                    task = task_by_key[result.key]
                    self.control_store.finish_evidence_task(
                        result.key,
                        result.state,
                        owner=self.owner,
                        retry_at=self._retry_at(task.attempt),
                    )
                    retryable += 1
                    continue
                if result.state in {
                    CDXQueryState.PASS,
                    CDXQueryState.EMPTY_EXHAUSTIVE,
                    CDXQueryState.INVALID,
                }:
                    writer.submit(result.capsule, result)
                    terminal += 1
                    continue
                if result.state not in {
                    CDXQueryState.INCOMPLETE,
                    CDXQueryState.TRANSIENT_ERROR,
                }:
                    raise ValueError(f"unsupported provider state: {result.state}")
                task = task_by_key[result.key]
                self.control_store.finish_evidence_task(
                    result.key,
                    result.state,
                    owner=self.owner,
                    retry_at=self._retry_at(task.attempt),
                )
                retryable += 1
        finally:
            writer.close()

        return EvidenceWorkerReport(
            claimed=len(tasks),
            terminal=terminal,
            retryable=retryable,
            inserted_capsules=writer.inserted_capsules + range_inserted_capsules,
            unknown_provider=0,
        )

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
            )
            batches += 1
        return total
