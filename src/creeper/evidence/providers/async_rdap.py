"""Asynchronous RDAP registration-year evidence provider.

RDAP is an independent evidence lane: a standards-defined registration event can
prove the registrable domain's creation year without consuming Wayback quota.
The provider is deliberately conservative: missing/ambiguous registration events
never become negative evidence.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote

import httpx

from creeper.authority.normalizer import normalize_official
from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceCapsule,
    EvidenceQueryKey,
    EvidenceQueryResult,
    RangeEvidenceQueryResult,
)
from creeper.runtime.http import configured_http_proxy


class AsyncRDAPClient:
    provider = "rdap"

    def __init__(
        self,
        endpoint: str = "https://rdap.org/domain",
        *,
        timeout: float = 20.0,
        requests_per_second: float = 1.0,
        min_requests_per_second: float = 0.10,
        decrease_factor: float = 0.70,
        recovery_successes: int = 20,
        recovery_step_fraction: float = 0.10,
        throttle_floor_seconds: float = 2.0,
        max_connections: int = 4,
        max_keepalive_connections: int = 2,
        user_agent: str = "Creeper/2.2 (research; https://github.com/Qesire/Creeper)",
        client: httpx.AsyncClient | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if (
            not endpoint.strip()
            or timeout <= 0
            or requests_per_second < 0
            or min_requests_per_second <= 0
            or (
                requests_per_second > 0
                and min_requests_per_second > requests_per_second
            )
            or not math.isfinite(decrease_factor)
            or not 0 < decrease_factor < 1
            or not isinstance(recovery_successes, int)
            or isinstance(recovery_successes, bool)
            or recovery_successes < 1
            or not math.isfinite(recovery_step_fraction)
            or recovery_step_fraction <= 0
            or throttle_floor_seconds < 0
        ):
            raise ValueError("invalid RDAP client configuration")
        self.endpoint = endpoint.rstrip("/")
        self.timeout = float(timeout)
        self.requests_per_second = float(requests_per_second)
        self.min_requests_per_second = float(min_requests_per_second)
        self.decrease_factor = float(decrease_factor)
        self.recovery_successes = int(recovery_successes)
        self.recovery_step_fraction = float(recovery_step_fraction)
        self.throttle_floor_seconds = float(throttle_floor_seconds)
        self.effective_requests_per_second = float(requests_per_second)
        self.http_requests = 0
        self.transport_errors = 0
        self.http_status_counts: dict[int, int] = {}
        self.http_elapsed_milliseconds = 0
        self.throttle_events = 0
        self.cooldown_wait_milliseconds = 0
        self.rate_limit_wait_milliseconds = 0
        self.adaptive_rate_decreases = 0
        self.adaptive_rate_increases = 0
        self._pacing_lock = asyncio.Lock()
        self._health_lock = asyncio.Lock()
        self._next_request_start = 0.0
        self._cooldown_until = 0.0
        self._success_streak = 0
        self._owns_client = client is None
        if client is not None:
            self.client = client
        else:
            options = {
                "timeout": httpx.Timeout(timeout),
                "limits": httpx.Limits(
                    max_connections=max_connections,
                    max_keepalive_connections=max_keepalive_connections,
                ),
                "headers": {
                    "User-Agent": user_agent,
                    "Accept": "application/rdap+json, application/json",
                },
                "follow_redirects": True,
                "transport": transport,
                "trust_env": False,
            }
            if transport is None:
                options["proxy"] = configured_http_proxy()
            self.client = httpx.AsyncClient(**options)

    @staticmethod
    def _retry_after_seconds(response: httpx.Response) -> float | None:
        value = response.headers.get("retry-after")
        if not value:
            return None
        text = value.strip()
        if text.isdigit():
            return max(0.0, float(text))
        try:
            target = parsedate_to_datetime(text)
            if target.tzinfo is None:
                return None
            now = datetime.now(timezone.utc)
            return max(0.0, (target - now).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None

    async def _acquire_request_slot(self) -> None:
        if self.requests_per_second <= 0:
            return
        loop = asyncio.get_running_loop()
        async with self._pacing_lock:
            now = loop.time()
            wait_until = max(self._next_request_start, self._cooldown_until)
            delay = max(0.0, wait_until - now)
            if delay > 0:
                started = loop.time()
                await asyncio.sleep(delay)
                waited = max(0, int(round((loop.time() - started) * 1000.0)))
                if wait_until == self._cooldown_until and self._cooldown_until >= self._next_request_start:
                    self.cooldown_wait_milliseconds += waited
                else:
                    self.rate_limit_wait_milliseconds += waited
            now = loop.time()
            interval = 1.0 / max(
                self.min_requests_per_second,
                self.effective_requests_per_second,
            )
            self._next_request_start = max(now, self._next_request_start) + interval

    async def _register_throttle(self, response: httpx.Response) -> None:
        loop = asyncio.get_running_loop()
        retry_after = self._retry_after_seconds(response)
        cooldown = max(
            self.throttle_floor_seconds,
            0.0 if retry_after is None else retry_after,
        )
        async with self._health_lock:
            self.throttle_events += 1
            self._success_streak = 0
            if self.requests_per_second > 0:
                reduced = max(
                    self.min_requests_per_second,
                    self.effective_requests_per_second * self.decrease_factor,
                )
                if reduced < self.effective_requests_per_second:
                    self.effective_requests_per_second = reduced
                    self.adaptive_rate_decreases += 1
            self._cooldown_until = max(
                self._cooldown_until,
                loop.time() + cooldown,
            )

    async def _register_success(self) -> None:
        if self.requests_per_second <= 0:
            return
        async with self._health_lock:
            self._success_streak += 1
            if self._success_streak < self.recovery_successes:
                return
            self._success_streak = 0
            if self.effective_requests_per_second >= self.requests_per_second:
                return
            increased = min(
                self.requests_per_second,
                self.effective_requests_per_second
                + self.requests_per_second * self.recovery_step_fraction,
            )
            if increased > self.effective_requests_per_second:
                self.effective_requests_per_second = increased
                self.adaptive_rate_increases += 1

    async def __aenter__(self) -> "AsyncRDAPClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def _get(self, hostname: str) -> tuple[httpx.Response, int]:
        url = f"{self.endpoint}/{quote(hostname, safe='.-')}"
        loop = asyncio.get_running_loop()
        await self._acquire_request_slot()
        started = loop.time()
        self.http_requests += 1
        try:
            response = await self.client.get(url)
        except (httpx.TimeoutException, httpx.TransportError):
            self.transport_errors += 1
            raise
        finally:
            elapsed = max(0, int(round((loop.time() - started) * 1000.0)))
            self.http_elapsed_milliseconds += elapsed
        status = int(response.status_code)
        self.http_status_counts[status] = (
            self.http_status_counts.get(status, 0) + 1
        )
        if status in {429, 503}:
            await self._register_throttle(response)
        elif status < 500:
            await self._register_success()
        return response, elapsed

    @staticmethod
    def _registration_event(payload: dict[str, object]) -> str | None:
        events = payload.get("events")
        if not isinstance(events, list):
            return None
        dates = []
        for event in events:
            if not isinstance(event, dict):
                continue
            if str(event.get("eventAction", "")).strip().lower() != "registration":
                continue
            value = event.get("eventDate")
            if isinstance(value, str) and len(value) >= 4 and value[:4].isdigit():
                dates.append(value.strip())
        return min(dates) if dates else None

    async def query_range(self, key: EvidenceQueryKey) -> RangeEvidenceQueryResult:
        if key.provider != self.provider:
            raise ValueError("provider mismatch for RDAP query")
        scope = key.temporal_scope
        requests = elapsed_ms = 0
        try:
            response, elapsed_ms = await self._get(key.hostname)
            requests = 1
            if response.status_code == 429 or response.status_code >= 500:
                response.raise_for_status()
            if response.status_code >= 400:
                return RangeEvidenceQueryResult(
                    hostname=key.hostname,
                    key=key,
                    state=CDXQueryState.INVALID,
                    provider_requests=requests,
                    provider_elapsed_milliseconds=elapsed_ms,
                    error=f"RDAP HTTP {response.status_code}",
                )
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("RDAP response must be an object")
            returned = normalize_official(
                str(payload.get("ldhName") or payload.get("unicodeName") or "")
            )
            if returned != key.hostname:
                raise ValueError("RDAP response domain does not match query")
            event_date = self._registration_event(payload)
            if event_date is None:
                return RangeEvidenceQueryResult(
                    hostname=key.hostname,
                    key=key,
                    state=CDXQueryState.INVALID,
                    provider_requests=requests,
                    provider_elapsed_milliseconds=elapsed_ms,
                    error="RDAP domain has no registration event",
                )
            year = int(event_date[:4])
            if not scope.year_from <= year <= scope.year_to:
                return RangeEvidenceQueryResult(
                    hostname=key.hostname,
                    key=key,
                    state=CDXQueryState.EMPTY_EXHAUSTIVE,
                    provider_requests=requests,
                    provider_elapsed_milliseconds=elapsed_ms,
                )
            serialized = json.dumps(
                payload, ensure_ascii=False, sort_keys=True
            ).encode("utf-8")
            capsule = EvidenceCapsule(
                hostname=key.hostname,
                year=year,
                provider=key.provider,
                temporal_semantics="registration_event_year",
                evidence_timestamp=event_date,
                source_locator=str(response.url),
                payload_hash=hashlib.sha256(serialized).hexdigest(),
                policy_version=key.policy_version,
                evidence_type="rdap_registration_event",
                source_id="rdap",
                original_url=str(response.url),
                record_locator=f"rdap:{key.hostname}:registration",
                extraction_method="rdap_registration_event",
            )
            return RangeEvidenceQueryResult(
                hostname=key.hostname,
                key=key,
                state=CDXQueryState.PASS,
                candidate_years=(year,),
                capsules=(capsule,),
                pages_seen=1,
                records_seen=1,
                provider_requests=requests,
                provider_elapsed_milliseconds=elapsed_ms,
            )
        except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
            return RangeEvidenceQueryResult(
                hostname=key.hostname,
                key=key,
                state=CDXQueryState.TRANSIENT_ERROR,
                provider_requests=max(1, requests),
                provider_elapsed_milliseconds=elapsed_ms,
                error=str(exc) or type(exc).__name__,
            )
        except (ValueError, json.JSONDecodeError) as exc:
            return RangeEvidenceQueryResult(
                hostname=key.hostname,
                key=key,
                state=CDXQueryState.INVALID,
                provider_requests=max(1, requests),
                provider_elapsed_milliseconds=elapsed_ms,
                error=str(exc),
            )

    async def query_key(self, key: EvidenceQueryKey) -> EvidenceQueryResult:
        result = await self.query_range(key)
        year = key.temporal_scope.year_from
        capsule = next(
            (item for item in result.capsules if item.year == year),
            None,
        )
        return EvidenceQueryResult(
            hostname=key.hostname,
            year=year,
            state=(
                CDXQueryState.PASS
                if capsule is not None
                else result.state
            ),
            capsule=capsule,
            pages_seen=result.pages_seen,
            records_seen=result.records_seen,
            provider_requests=result.provider_requests,
            provider_elapsed_milliseconds=result.provider_elapsed_milliseconds,
            error=result.error,
            key=key,
        )
