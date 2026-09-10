"""Durable asynchronous evidence consumer.

The queue authority remains Creeper's ControlStore because EvidenceQueryKey and
its terminal states are competition semantics, not generic job metadata. The
worker follows the mature ACK/visibility-timeout pattern used by persistent
queues: claim a bounded batch, execute provider work concurrently, then either
commit a terminal result or release retryable work with a durable retry time.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Protocol

from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceQueryKey,
    EvidenceQueryResult,
)
from creeper.storage.commit_writer import CommitWriter
from creeper.storage.control_store import ControlStore, EvidenceTask
from creeper.storage.evidence_store import EvidenceStore


class AsyncEvidenceProvider(Protocol):
    async def query_key(self, key: EvidenceQueryKey) -> EvidenceQueryResult:
        """Execute one already-claimed durable query key."""


@dataclass(frozen=True)
class EvidenceWorkerReport:
    claimed: int = 0
    terminal: int = 0
    retryable: int = 0
    inserted_capsules: int = 0
    unknown_provider: int = 0


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
        self.control_store = control_store
        self.evidence_store = evidence_store
        self.providers = dict(providers)
        self.owner = owner
        self.claim_batch_size = claim_batch_size
        self.lease_seconds = lease_seconds
        self.retry_base_seconds = retry_base_seconds
        self.retry_max_seconds = retry_max_seconds
        self.clock = clock

        limits = dict(provider_inflight or {})
        unknown_limits = set(limits) - set(self.providers)
        if unknown_limits:
            raise KeyError(f"inflight configured for unknown providers: {sorted(unknown_limits)}")
        self._semaphores: dict[str, asyncio.Semaphore] = {}
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
    def _transient_for(task: EvidenceTask, error: str) -> EvidenceQueryResult:
        scope = task.key.temporal_scope
        return EvidenceQueryResult(
            hostname=task.key.hostname,
            year=scope.year_from,
            state=CDXQueryState.TRANSIENT_ERROR,
            error=error,
            key=task.key,
        )

    async def _execute(self, task: EvidenceTask) -> EvidenceQueryResult:
        provider = self.providers.get(task.key.provider)
        if provider is None:
            return self._transient_for(
                task,
                f"provider is not configured: {task.key.provider}",
            )
        semaphore = self._semaphores[task.key.provider]
        async with semaphore:
            try:
                return await provider.query_key(task.key)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # operational failure, never evidence INVALID
                return self._transient_for(task, str(exc) or type(exc).__name__)

    async def run_once(self) -> EvidenceWorkerReport:
        """Claim and process one bounded durable batch.

        Existing PENDING/INCOMPLETE/TRANSIENT_ERROR rows are eligible, so this
        worker can resume backlog created by an earlier process or SourceLease.
        """
        tasks = self.control_store.claim_evidence_tasks(
            owner=self.owner,
            limit=self.claim_batch_size,
            lease_seconds=self.lease_seconds,
        )
        if not tasks:
            return EvidenceWorkerReport()

        results = await asyncio.gather(*(self._execute(task) for task in tasks))
        writer = CommitWriter(
            self.evidence_store,
            self.control_store,
            owner=self.owner,
            flush_count=max(1, min(self.claim_batch_size, 128)),
        )
        terminal = retryable = unknown_provider = 0
        task_by_key = {task.key: task for task in tasks}
        try:
            for result in results:
                if result.key is None:
                    raise ValueError("provider result must preserve EvidenceQueryKey")
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
                if result.error and result.error.startswith("provider is not configured:"):
                    unknown_provider += 1
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
            inserted_capsules=writer.inserted_capsules,
            unknown_provider=unknown_provider,
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
