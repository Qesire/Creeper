"""Lease renewal and fencing for distributed workers."""

from __future__ import annotations

import asyncio
import time

from creeper.distributed.coordinator_client import (
    CoordinatorClient,
    CoordinatorError,
    CoordinatorTransportError,
    StaleLeaseCoordinatorError,
)
from creeper.distributed.models import TaskLease


class LeaseLostError(RuntimeError):
    pass


class LeaseKeeper:
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
        if lease_seconds <= 0 or not 0 < renew_fraction < 1 or min_renew_interval <= 0:
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
            name=f"creeper-fabric-lease-{self.lease.task_id[:12]}",
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
            wait = max(
                0.01,
                min(
                    remaining * self.renew_fraction,
                    max(0.01, remaining - self.min_renew_interval),
                ),
            )
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=wait)
                return
            except TimeoutError:
                pass
            transient_failures = 0
            while not self._stop.is_set():
                try:
                    renewed = await self.client.renew(
                        self.lease,
                        lease_seconds=self.lease_seconds,
                    )
                    break
                except StaleLeaseCoordinatorError as exc:
                    self._lost = exc
                    return
                except CoordinatorTransportError as exc:
                    transient_failures += 1
                    remaining = float(self.lease.lease_deadline) - float(
                        self.clock()
                    )
                    if remaining <= 0:
                        self._lost = LeaseLostError(
                            f"lease expired during transient renewal failure: {exc}"
                        )
                        return
                    retry_delay = min(
                        5.0,
                        max(
                            0.25,
                            self.min_renew_interval
                            * (2 ** min(transient_failures - 1, 3)),
                        ),
                        max(0.01, remaining),
                    )
                    try:
                        await asyncio.wait_for(
                            self._stop.wait(),
                            timeout=retry_delay,
                        )
                        return
                    except TimeoutError:
                        continue
                except CoordinatorError as exc:
                    self._lost = exc
                    return
            else:
                return
            if (
                renewed.task_id != self.lease.task_id
                or renewed.generation != self.lease.generation
                or renewed.worker_id != self.lease.worker_id
                or renewed.worker_instance_id != self.lease.worker_instance_id
            ):
                self._lost = LeaseLostError("authority returned incompatible lease")
                return
            self.lease = renewed
