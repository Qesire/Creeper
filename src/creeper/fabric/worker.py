"""Generic Fabric v2 worker runtime with automatic lease renewal."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from typing import Awaitable, Callable, Mapping

from .client import (
    FabricClient,
    FabricClientError,
    FabricTransportError,
    StaleLeaseClientError,
)
from .models import (
    FabricWorkClass,
    LeaseToken,
    WorkerDescriptor,
)


class PermanentTaskError(RuntimeError):
    """Handler failure that should not be retried."""


class LeaseLostError(RuntimeError):
    """The worker no longer owns the task epoch."""


TaskHandler = Callable[["FabricTaskSession"], Awaitable[None]]


@dataclass
class FabricTaskSession:
    client: FabricClient
    lease: LeaseToken

    def __post_init__(self) -> None:
        self._lock = asyncio.Lock()

    async def snapshot(self) -> LeaseToken:
        async with self._lock:
            return self.lease

    async def replace_lease(self, lease: LeaseToken) -> None:
        async with self._lock:
            if (
                lease.task_id != self.lease.task_id
                or lease.lease_epoch != self.lease.lease_epoch
            ):
                raise LeaseLostError("renewed lease identity changed")
            self.lease = lease

    async def commit_batch(
        self,
        results: list[Mapping[str, object]],
        *,
        cursor_after: Mapping[str, object] | None = None,
    ) -> str:
        async with self._lock:
            current = self.lease
            sequence = current.next_sequence_no
        status = await self.client.commit_batch(
            current,
            sequence_no=sequence,
            results=results,
            cursor_after=cursor_after,
        )
        async with self._lock:
            # The authority accepted this sequence (or confirmed an idempotent
            # replay), so advance only local session metadata. The authority
            # remains the source of truth.
            self.lease = replace(
                self.lease,
                cursor=(
                    None if cursor_after is None else dict(cursor_after)
                ),
                next_sequence_no=sequence + 1,
            )
        return status

    async def provider_permit(
        self,
        provider: str,
        *,
        allowed_requests: int = 1,
        ttl_seconds: float = 30.0,
        retry_after_seconds: float | None = 0.25,
    ):
        lease = await self.snapshot()
        return await self.client.provider_permit(
            lease,
            provider,
            allowed_requests=allowed_requests,
            ttl_seconds=ttl_seconds,
            retry_after_seconds=retry_after_seconds,
        )


class LeaseKeeper:
    """Renew one task lease and fail before its fencing window expires."""

    def __init__(
        self,
        session: FabricTaskSession,
        *,
        lease_seconds: float,
        renew_fraction: float = 0.40,
        retry_floor_seconds: float = 0.25,
    ) -> None:
        if lease_seconds <= 2:
            raise ValueError("lease_seconds must exceed two seconds")
        if not 0.1 <= renew_fraction <= 0.8:
            raise ValueError("renew_fraction must be within [0.1, 0.8]")
        if retry_floor_seconds <= 0:
            raise ValueError("retry_floor_seconds must be positive")
        self.session = session
        self.lease_seconds = float(lease_seconds)
        self.renew_fraction = float(renew_fraction)
        self.retry_floor_seconds = float(retry_floor_seconds)

    async def run(self) -> None:
        delay = max(1.0, self.lease_seconds * self.renew_fraction)
        while True:
            await asyncio.sleep(delay)
            current = await self.session.snapshot()
            try:
                renewed = await self.session.client.renew(
                    current,
                    lease_seconds=self.lease_seconds,
                )
            except StaleLeaseClientError as exc:
                raise LeaseLostError(str(exc)) from exc
            except FabricTransportError:
                # A transport outage is not proof that ownership was lost.
                # Retry quickly, but never manufacture a local epoch.
                await asyncio.sleep(self.retry_floor_seconds)
                current = await self.session.snapshot()
                try:
                    renewed = await self.session.client.renew(
                        current,
                        lease_seconds=self.lease_seconds,
                    )
                except (StaleLeaseClientError, FabricTransportError) as exc:
                    raise LeaseLostError(
                        "could not safely renew lease before continuing"
                    ) from exc
            await self.session.replace_lease(renewed)


class FabricWorker:
    """Pull-worker runtime shared by local and remote deployments."""

    def __init__(
        self,
        client: FabricClient,
        descriptor: WorkerDescriptor,
        handlers: Mapping[FabricWorkClass, TaskHandler],
        *,
        queue: str = "default",
        lease_seconds: float = 60.0,
        wait_seconds: float = 20.0,
        idle_sleep_seconds: float = 0.5,
    ) -> None:
        if descriptor.worker_id != client.worker_id:
            raise ValueError("worker descriptor and client identity disagree")
        if not handlers:
            raise ValueError("at least one task handler is required")
        self.client = client
        self.descriptor = descriptor
        self.handlers = {
            FabricWorkClass(key): value for key, value in handlers.items()
        }
        self.queue = queue
        self.lease_seconds = float(lease_seconds)
        self.wait_seconds = float(wait_seconds)
        self.idle_sleep_seconds = float(idle_sleep_seconds)

    async def run_once(self) -> bool:
        lease = await self.client.claim(
            queue=self.queue,
            lease_seconds=self.lease_seconds,
            wait_seconds=self.wait_seconds,
        )
        if lease is None:
            return False

        handler = self.handlers.get(lease.work.work_class)
        if handler is None:
            await self.client.fail(
                lease,
                error=(
                    "worker has no handler for work class "
                    f"{lease.work.work_class.value}"
                ),
                retryable=False,
            )
            return True

        session = FabricTaskSession(self.client, lease)
        keeper = LeaseKeeper(
            session,
            lease_seconds=self.lease_seconds,
        )
        handler_task = asyncio.create_task(handler(session))
        keeper_task = asyncio.create_task(keeper.run())
        done, pending = await asyncio.wait(
            {handler_task, keeper_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if keeper_task in done:
            error = keeper_task.exception()
            if error is not None:
                handler_task.cancel()
                await asyncio.gather(handler_task, return_exceptions=True)
                if isinstance(error, LeaseLostError):
                    return True
                raise error

        if handler_task in done:
            keeper_task.cancel()
            await asyncio.gather(keeper_task, return_exceptions=True)
            try:
                handler_task.result()
            except PermanentTaskError as exc:
                current = await session.snapshot()
                try:
                    await self.client.fail(
                        current,
                        error=str(exc),
                        retryable=False,
                    )
                except StaleLeaseClientError:
                    pass
                return True
            except (LeaseLostError, StaleLeaseClientError):
                return True
            except Exception as exc:
                current = await session.snapshot()
                try:
                    await self.client.fail(
                        current,
                        error=f"{type(exc).__name__}: {exc}",
                        retryable=True,
                    )
                except StaleLeaseClientError:
                    pass
                return True

            current = await session.snapshot()
            try:
                await self.client.complete(current)
            except StaleLeaseClientError:
                pass
            return True

        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        return True

    async def run_forever(self) -> None:
        await self.client.register(self.descriptor)
        while True:
            try:
                worked = await self.run_once()
            except FabricClientError:
                await asyncio.sleep(max(1.0, self.idle_sleep_seconds))
                continue
            if not worked:
                await asyncio.sleep(self.idle_sleep_seconds)
