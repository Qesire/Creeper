"""Authority-backed permit gate for real provider HTTP requests."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from uuid import uuid4

import httpx

from creeper.distributed.coordinator_client import (
    CoordinatorClient,
    CoordinatorTransportError,
)
from creeper.distributed.lease_keeper import LeaseKeeper
from creeper.distributed.models import ProviderPermit


class DistributedProviderGate:
    """Acquire/release one global permit per actual external HTTP request."""

    def __init__(
        self,
        client: CoordinatorClient,
        keeper: LeaseKeeper,
        provider: str,
        *,
        permit_ttl_seconds: float = 60.0,
        budget_poll_seconds: float = 0.1,
        throttle_floor_seconds: float = 2.0,
    ) -> None:
        if (
            not provider.strip()
            or permit_ttl_seconds <= 0
            or budget_poll_seconds <= 0
            or throttle_floor_seconds < 0
        ):
            raise ValueError("invalid distributed provider gate configuration")
        self.client = client
        self.keeper = keeper
        self.provider = provider
        self.permit_ttl_seconds = float(permit_ttl_seconds)
        self.budget_poll_seconds = float(budget_poll_seconds)
        self.throttle_floor_seconds = float(throttle_floor_seconds)

    async def acquire(self) -> object:
        # One idempotency key per actual external HTTP attempt. If the
        # Authority accepted the permit but the HTTPS response is lost, retry
        # the same logical request instead of consuming a second global slot.
        request_id = uuid4().hex
        while True:
            self.keeper.assert_owned()
            try:
                permit = await self.client.provider_permit(
                    self.keeper.lease,
                    self.provider,
                    request_id=request_id,
                    ttl_seconds=self.permit_ttl_seconds,
                )
            except CoordinatorTransportError:
                self.keeper.assert_owned()
                await asyncio.sleep(self.budget_poll_seconds)
                continue
            if permit is not None:
                return permit
            await asyncio.sleep(self.budget_poll_seconds)

    @staticmethod
    def _retry_after_seconds(headers: httpx.Headers | None) -> float | None:
        if headers is None:
            return None
        raw = headers.get("Retry-After")
        if raw is None:
            return None
        value = raw.strip()
        if not value:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            pass
        try:
            target = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if target.tzinfo is None:
            target = target.replace(tzinfo=timezone.utc)
        return max(
            0.0,
            (
                target.astimezone(timezone.utc)
                - datetime.now(timezone.utc)
            ).total_seconds(),
        )

    async def report(
        self,
        token: object,
        status_code: int | None,
        headers: httpx.Headers | None,
        response_bytes: int,
    ) -> None:
        if not isinstance(token, ProviderPermit):
            raise TypeError("distributed provider gate received invalid permit token")
        cooldown = 0.0
        if status_code in {429, 503}:
            retry_after = self._retry_after_seconds(headers)
            cooldown = max(
                self.throttle_floor_seconds,
                0.0 if retry_after is None else retry_after,
            )
        for attempt in range(5):
            try:
                await self.client.provider_report(
                    token.permit_id,
                    status_code=status_code,
                    cooldown_seconds=cooldown,
                    response_bytes=int(response_bytes),
                )
                return
            except CoordinatorTransportError:
                if attempt == 4:
                    raise
                await asyncio.sleep(
                    min(1.0, self.budget_poll_seconds * (2**attempt))
                )
