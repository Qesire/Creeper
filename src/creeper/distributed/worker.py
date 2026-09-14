"""Generic pull-based VM worker runtime for distributed Creeper."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Awaitable, Callable

from creeper.distributed.coordinator_client import (
    CoordinatorClient,
    CoordinatorError,
    StaleLeaseCoordinatorError,
)
from creeper.distributed.lease_keeper import LeaseKeeper, LeaseLostError
from creeper.distributed.models import TaskLease, WorkerDescriptor


ProducerExecutor = Callable[
    [TaskLease, CoordinatorClient, LeaseKeeper],
    Awaitable[None],
]


@dataclass(frozen=True)
class WorkerRunReport:
    claimed: bool
    task_id: str | None = None
    producer: str | None = None
    completed: bool = False
    lost_lease: bool = False
    failed: bool = False
    error: str | None = None


class DistributedWorker:
    """One replaceable worker that pulls work from Local Authority."""

    def __init__(
        self,
        client: CoordinatorClient,
        descriptor: WorkerDescriptor,
        producers: dict[str, ProducerExecutor],
        *,
        lease_seconds: float = 300.0,
    ) -> None:
        if client.worker_id != descriptor.worker_id:
            raise ValueError("worker descriptor/client identity mismatch")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.client = client
        self.producers = dict(producers)
        advertised = tuple(sorted(self.producers))
        if descriptor.producers and set(descriptor.producers) != set(advertised):
            raise ValueError(
                "worker descriptor producers do not match runtime producer registry"
            )
        self.descriptor = replace(descriptor, producers=advertised)
        self.lease_seconds = float(lease_seconds)
        self._registered = False

    async def register(self) -> None:
        await self.client.register(self.descriptor)
        self._registered = True

    async def run_once(self) -> WorkerRunReport:
        if not self._registered:
            await self.register()
        await self.client.heartbeat()
        lease = await self.client.claim(lease_seconds=self.lease_seconds)
        if lease is None:
            return WorkerRunReport(claimed=False)

        executor = self.producers.get(lease.work.producer)
        if executor is None:
            try:
                await self.client.fail(
                    lease,
                    f"unsupported producer: {lease.work.producer}",
                    retryable=True,
                )
            except CoordinatorError:
                pass
            return WorkerRunReport(
                claimed=True,
                task_id=lease.task_id,
                producer=lease.work.producer,
                failed=True,
                error="unsupported producer",
            )

        keeper = LeaseKeeper(
            self.client,
            lease,
            lease_seconds=self.lease_seconds,
        )
        try:
            async with keeper:
                await executor(lease, self.client, keeper)
                keeper.assert_owned()
            await self.client.finish(keeper.lease)
            return WorkerRunReport(
                claimed=True,
                task_id=lease.task_id,
                producer=lease.work.producer,
                completed=True,
            )
        except (LeaseLostError, StaleLeaseCoordinatorError) as exc:
            # Do not attempt fail/finish through a lease that is already fenced.
            return WorkerRunReport(
                claimed=True,
                task_id=lease.task_id,
                producer=lease.work.producer,
                lost_lease=True,
                error=str(exc),
            )
        except Exception as exc:
            try:
                keeper.assert_owned()
            except LeaseLostError:
                return WorkerRunReport(
                    claimed=True,
                    task_id=lease.task_id,
                    producer=lease.work.producer,
                    lost_lease=True,
                    error=str(exc),
                )
            try:
                await self.client.fail(
                    keeper.lease,
                    f"{type(exc).__name__}: {exc}",
                    retryable=True,
                )
            except CoordinatorError:
                pass
            return WorkerRunReport(
                claimed=True,
                task_id=lease.task_id,
                producer=lease.work.producer,
                failed=True,
                error=str(exc),
            )
