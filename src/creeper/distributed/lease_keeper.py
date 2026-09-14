"""Lease maintenance for remote distributed workers."""

from __future__ import annotations

import asyncio
import time

from creeper.distributed.coordinator_client import (
    CoordinatorClient,
    CoordinatorError,
    StaleLeaseCoordinatorError,
)
from creeper.distributed.models import TaskLease


class LeaseLostError(RuntimeError):
    """Worker no longer has authority to create new external side effects."""


class LeaseKeeper:
    """Renew one task lease while a producer is executing.

    Producers call :meth:`assert_owned` before every new expensive external
    operation. If renewal fails or the Authority fences the generation, the
    keeper flips permanently to lost and the producer must stop.
    """

    def __init__(
        self,
        client: CoordinatorClient,
        lease: TaskLease,
        *,
        lease_seconds: float = 300.0,
        renew_fraction: float = 0.5,
        min_renew_interval: float = 1.0,
        clock=time.time,
    ) -> None:
        if (
            lease_seconds <= 0
            or not 0 < renew_fraction < 1
            or min_renew_interval <= 0
        ):
            raise ValueError("invalid lease keeper configuration")
        self.client = client
        self.lease = lease
        self.lease_seconds = float(lease_seconds)
        self.renew_fraction = float(renew_fraction)
        self.min_renew_interval = float(min_renew_interval)
        self.clock = clock
        self._lost: BaseException | None = None
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    @property
    def lost(self) -> bool:
        return self._lost is not None

    @property
    def loss_reason(self) -> BaseException | None:
        return self._lost

    def assert_owned(self) -> None:
        if self._lost is not None:
            raise LeaseLostError(str(self._lost))
        if float(self.clock()) >= float(self.lease.lease_deadline):
            raise LeaseLostError("lease deadline has passed")

    async def __aenter__(self) -> "LeaseKeeper":
        if self._task is not None:
            raise RuntimeError("lease keeper is already running")
        self._task = asyncio.create_task(
            self._run(),
            name=f"creeper-lease-{self.lease.task_id[:12]}",
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            now = float(self.clock())
            remaining = float(self.lease.lease_deadline) - now
            if remaining <= 0:
                self._lost = LeaseLostError("lease expired before renewal")
                return
            target_wait = max(
                self.min_renew_interval,
                min(
                    remaining * self.renew_fraction,
                    max(0.0, remaining - self.min_renew_interval),
                ),
            )
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=target_wait,
                )
                return
            except TimeoutError:
                pass

            try:
                renewed = await self.client.renew(
                    self.lease,
                    lease_seconds=self.lease_seconds,
                )
            except (StaleLeaseCoordinatorError, CoordinatorError) as exc:
                self._lost = exc
                return
            if (
                renewed.task_id != self.lease.task_id
                or renewed.generation != self.lease.generation
                or renewed.worker_id != self.lease.worker_id
            ):
                self._lost = LeaseLostError(
                    "Authority returned incompatible renewed lease"
                )
                return
            self.lease = renewed
