"""Replaceable outbound-only Fabric v2 worker."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Protocol

from creeper.distributed.coordinator_client import (
    CoordinatorClient,
    CoordinatorError,
    CoordinatorTransportError,
    CoordinatorUploadBudgetExceededError,
    StaleLeaseCoordinatorError,
)
from creeper.distributed.lease_keeper import LeaseKeeper, LeaseLostError
from creeper.distributed.models import ResultBatch, TaskLease, WorkerDescriptor
from creeper.distributed.worker_spool import WorkerResultSpool


@dataclass(frozen=True, slots=True)
class ProducerContext:
    client: CoordinatorClient
    keeper: LeaseKeeper
    descriptor: WorkerDescriptor


class DistributedProducer(Protocol):
    async def run(
        self,
        lease: TaskLease,
        context: ProducerContext,
    ) -> AsyncIterator[ResultBatch]: ...


class DistributedWorker:
    def __init__(
        self,
        client: CoordinatorClient,
        descriptor: WorkerDescriptor,
        producers: Mapping[str, DistributedProducer],
        spool: WorkerResultSpool,
        *,
        poll_seconds: float = 1.0,
        claim_wait_seconds: float = 10.0,
        lease_seconds: float = 300.0,
        heartbeat_seconds: float = 30.0,
    ) -> None:
        if (
            poll_seconds <= 0
            or not 0 <= claim_wait_seconds <= 25
            or lease_seconds <= 0
            or heartbeat_seconds <= 0
        ):
            raise ValueError("invalid worker timing configuration")
        self.client = client
        self.descriptor = descriptor
        self.producers = dict(producers)
        self.spool = spool
        self.poll_seconds = float(poll_seconds)
        self.claim_wait_seconds = float(claim_wait_seconds)
        self.lease_seconds = float(lease_seconds)
        self.heartbeat_seconds = float(heartbeat_seconds)
        unknown = set(self.descriptor.producers) - set(self.producers)
        if unknown:
            raise ValueError(
                "descriptor advertises unregistered producers: "
                + ",".join(sorted(unknown))
            )

    async def initialize(self) -> None:
        await self.client.register(self.descriptor)
        await self._replay_spool()

    async def _replay_spool(self) -> None:
        """Replay durable batches before claiming new work.

        The spool may survive a worker crash after provider I/O or after an
        Authority commit whose ACK was lost. Replaying the same BatchID is
        idempotent. If the task lease was fenced or expired, Authority returns
        STALE_LEASE and only that obsolete generation is discarded.
        """

        discarded: set[tuple[str, int]] = set()
        for batch in self.spool.pending():
            generation_key=(batch.task_id,batch.generation)
            if generation_key in discarded:
                continue
            try:
                await self.client.commit_batch(batch)
            except StaleLeaseCoordinatorError:
                self.spool.discard_generation(
                    batch.task_id,
                    batch.generation,
                )
                discarded.add(generation_key)
                continue
            self.spool.ack(batch.batch_id)

    async def _deliver(
        self,
        batch: ResultBatch,
        keeper: LeaseKeeper,
    ) -> None:
        self.spool.put(batch)
        while True:
            keeper.assert_owned()
            try:
                await self.client.commit_batch(batch)
            except CoordinatorTransportError:
                await asyncio.sleep(min(1.0, self.poll_seconds))
                continue
            except StaleLeaseCoordinatorError as exc:
                self.spool.discard_generation(batch.task_id, batch.generation)
                raise LeaseLostError(str(exc)) from exc
            self.spool.ack(batch.batch_id)
            return

    async def run_once(self) -> bool:
        lease = await self.client.claim(
            lease_seconds=self.lease_seconds,
            wait_seconds=self.claim_wait_seconds,
        )
        if lease is None:
            return False
        producer = self.producers.get(lease.work.producer)
        if producer is None:
            await self.client.fail(
                lease,
                f"worker has no producer: {lease.work.producer}",
                retryable=False,
            )
            return True

        keeper = LeaseKeeper(
            self.client,
            lease,
            lease_seconds=self.lease_seconds,
        )
        sequence = lease.next_sequence_no
        cursor = lease.cursor
        final_sent = False
        try:
            async with keeper:
                context = ProducerContext(
                    client=self.client,
                    keeper=keeper,
                    descriptor=self.descriptor,
                )
                async for batch in producer.run(lease, context):
                    keeper.assert_owned()
                    if (
                        batch.task_id != lease.task_id
                        or batch.generation != lease.generation
                        or batch.sequence_no != sequence
                    ):
                        raise ValueError(
                            "producer emitted incompatible result-batch identity"
                        )
                    await self._deliver(batch, keeper)
                    sequence += 1
                    cursor = batch.cursor_after
                    if batch.final:
                        final_sent = True
                        break
                if not final_sent:
                    await self._deliver(
                        ResultBatch(
                            task_id=lease.task_id,
                            generation=lease.generation,
                            sequence_no=sequence,
                            results=(),
                            cursor_after=cursor,
                            final=True,
                        ),
                        keeper,
                    )
            return True
        except LeaseLostError:
            return True
        except CoordinatorUploadBudgetExceededError:
            raise
        except BaseException as exc:
            try:
                keeper.assert_owned()
                await self.client.fail(
                    lease,
                    f"{type(exc).__name__}: {exc}",
                    retryable=True,
                )
            except (LeaseLostError, CoordinatorError):
                pass
            return True

    async def _heartbeat_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.client.heartbeat()
            except CoordinatorError:
                pass
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.heartbeat_seconds)
            except TimeoutError:
                continue

    async def run_forever(self, stop: asyncio.Event | None = None) -> None:
        stop = stop or asyncio.Event()
        while not stop.is_set():
            try:
                await self.initialize()
                break
            except CoordinatorUploadBudgetExceededError:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=60.0)
                except TimeoutError:
                    pass
            except CoordinatorTransportError:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self.poll_seconds)
                except TimeoutError:
                    pass
        if stop.is_set():
            return
        heartbeat = asyncio.create_task(self._heartbeat_loop(stop))
        try:
            while not stop.is_set():
                quota_wait = False
                try:
                    worked = await self.run_once()
                except CoordinatorUploadBudgetExceededError:
                    worked = False
                    quota_wait = True
                except CoordinatorTransportError:
                    worked = False
                if not worked:
                    try:
                        await asyncio.wait_for(
                            stop.wait(),
                            timeout=60.0 if quota_wait else self.poll_seconds,
                        )
                    except TimeoutError:
                        pass
        finally:
            stop.set()
            await heartbeat
