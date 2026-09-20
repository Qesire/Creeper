"""Authority-backed provider rate/inflight gate."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import time
from email.utils import parsedate_to_datetime
from uuid import uuid4

import httpx

from creeper.distributed.coordinator_client import (
    CoordinatorClient,
    CoordinatorError,
    CoordinatorTransportError,
)
from creeper.distributed.lease_keeper import LeaseKeeper
from creeper.distributed.models import ProviderPermit


class DistributedProviderGate:
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
            raise ValueError("invalid provider gate configuration")
        self.client = client
        self.keeper = keeper
        self.provider = provider
        self.permit_ttl_seconds = float(permit_ttl_seconds)
        self.budget_poll_seconds = float(budget_poll_seconds)
        self.throttle_floor_seconds = float(throttle_floor_seconds)
        self._started_at: dict[str, float] = {}

    async def acquire(self) -> ProviderPermit:
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
                self._started_at[permit.permit_id] = time.monotonic()
                return permit
            await asyncio.sleep(self.budget_poll_seconds)

    @staticmethod
    def _retry_after_seconds(headers: httpx.Headers | None) -> float | None:
        if headers is None:
            return None
        raw = headers.get("Retry-After")
        if raw is None or not raw.strip():
            return None
        try:
            return max(0.0, float(raw.strip()))
        except ValueError:
            pass
        try:
            target = parsedate_to_datetime(raw.strip())
        except (TypeError, ValueError, OverflowError):
            return None
        if target.tzinfo is None:
            target = target.replace(tzinfo=timezone.utc)
        return max(
            0.0,
            (target.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds(),
        )

    async def report(
        self,
        token: object,
        status_code: int | None,
        headers: httpx.Headers | None,
        response_bytes: int,
    ) -> None:
        if not isinstance(token, ProviderPermit):
            raise TypeError("invalid provider permit token")
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
                break
            except CoordinatorTransportError:
                if attempt == 4:
                    raise
                await asyncio.sleep(min(1.0, self.budget_poll_seconds * (2**attempt)))

        started = self._started_at.pop(token.permit_id, None)
        latency_ms = (
            0.0
            if started is None
            else max(0.0, (time.monotonic() - started) * 1000.0)
        )
        try:
            await self.client.provider_observation(
                self.keeper.lease,
                provider=self.provider,
                connect_success=status_code is not None,
                status_code=status_code,
                latency_ms=latency_ms,
                response_bytes=int(response_bytes),
                timeout=status_code is None,
                policy_block=status_code in {403, 451},
            )
        except CoordinatorError:
            # Permit settlement is authoritative. Region qualification is
            # routing telemetry and must not turn a completed provider request
            # into a duplicate retry if this best-effort observation is lost.
            pass
