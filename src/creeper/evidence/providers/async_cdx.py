"""Mature asynchronous Wayback CDX transport.

HTTP connection pooling and timeout handling are delegated to HTTPX, bounded
retry/backoff to Tenacity, and request-rate shaping to aiolimiter. Creeper
retains only the competition-specific exact-host/exact-year acceptance rules.
"""

from __future__ import annotations

import asyncio
from collections import Counter
import hashlib
import json
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from collections.abc import AsyncIterator

import httpx
from aiolimiter import AsyncLimiter
from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
    wait_none,
    wait_random_exponential,
)

from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceCapsule,
    EvidenceQueryKey,
    EvidenceQueryResult,
    RangeEvidenceQueryResult,
)
from creeper.evidence.providers.cdx import Page, WaybackCDXClient, _exact_hostname
from creeper.runtime.http import configured_http_proxy


def _retryable_http_error(exc: BaseException) -> bool:
    if isinstance(exc, (httpx.TimeoutException, httpx.TransportError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status == 429 or status >= 500
    return False


class AsyncWaybackCDXClient:
    """Reusable asynchronous CDX provider with bounded mature HTTP controls.

    One long-lived ``httpx.AsyncClient`` is intentionally shared across calls so
    HTTP keep-alive and connection pooling remain effective. Provider task
    concurrency belongs to the EvidenceWorker; this class only bounds the
    underlying HTTP connection pool and per-provider request rate.
    """

    def __init__(
        self,
        endpoint: str = "https://web.archive.org/cdx/search/cdx",
        *,
        provider: str = "wayback",
        limit: int = 1_000,
        timeout: float = 30.0,
        max_retries: int = 3,
        backoff: float = 0.5,
        max_backoff: float = 30.0,
        requests_per_second: float = 0.0,
        max_connections: int = 16,
        max_keepalive_connections: int = 8,
        keepalive_expiry_seconds: float = 30.0,
        throttle_floor_seconds: float = 2.0,
        user_agent: str = "Creeper/2.2 (research; https://github.com/Qesire/Creeper)",
        client: httpx.AsyncClient | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if (
            limit < 1
            or timeout <= 0
            or max_retries < 0
            or backoff < 0
            or max_backoff < 0
            or requests_per_second < 0
            or max_connections < 1
            or max_keepalive_connections < 0
            or max_keepalive_connections > max_connections
            or keepalive_expiry_seconds <= 0
            or throttle_floor_seconds < 0
        ):
            raise ValueError("invalid async CDX client limits")
        if client is not None and transport is not None:
            raise ValueError("pass either client or transport, not both")
        self.endpoint = endpoint
        self.provider = provider
        self.limit = limit
        self.max_retries = max_retries
        self.backoff = backoff
        self.max_backoff = max_backoff
        self.throttle_floor_seconds = float(throttle_floor_seconds)
        self.http_requests = 0
        self.throttle_responses = 0
        self.transport_errors = 0
        self.http_elapsed_milliseconds = 0
        self.http_latency_buckets: Counter[str] = Counter()
        self.http_status_counts: Counter[int] = Counter()
        self._cooldown_until = 0.0
        self._cooldown_lock = asyncio.Lock()
        if requests_per_second > 0:
            # Use one token per interval for strict pacing. A token bucket with
            # max_rate=N permits an N-request burst at the start of each
            # window, which is exactly the pattern that tends to trigger
            # public-CDX throttling. Concurrency remains useful for overlapping
            # request latency, but request *starts* are evenly spaced.
            self._limiter = AsyncLimiter(
                1,
                time_period=1.0 / requests_per_second,
            )
        else:
            self._limiter = None
        self._owns_client = client is None
        client_options = dict(
            timeout=httpx.Timeout(timeout),
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_keepalive_connections,
                keepalive_expiry=keepalive_expiry_seconds,
            ),
            headers={
                "User-Agent": user_agent,
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
            },
            follow_redirects=True,
            trust_env=False,
            transport=transport,
        )
        if transport is None:
            client_options["proxy"] = configured_http_proxy()
        self.client = client or httpx.AsyncClient(**client_options)

    async def __aenter__(self) -> "AsyncWaybackCDXClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    def _wait_policy(self):
        if self.backoff == 0 or self.max_backoff == 0:
            return wait_none()
        return wait_random_exponential(
            multiplier=self.backoff,
            max=max(self.backoff, self.max_backoff),
        )

    @staticmethod
    def _retry_after_seconds(response: httpx.Response) -> float | None:
        """Parse Retry-After as delta-seconds or an HTTP date."""
        raw = response.headers.get("Retry-After")
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
            (target.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds(),
        )

    async def _extend_cooldown(self, seconds: float) -> None:
        """Extend one provider-wide monotonic cooldown shared by all coroutines."""
        if seconds <= 0:
            return
        loop = asyncio.get_running_loop()
        deadline = loop.time() + float(seconds)
        async with self._cooldown_lock:
            self._cooldown_until = max(self._cooldown_until, deadline)

    async def _wait_for_cooldown(self) -> None:
        """Wait until provider-wide throttling has expired.

        This is deliberately shared across requests. A single 429/503 therefore
        slows every coroutine instead of allowing the rest of the batch to keep
        hammering the same CDX endpoint.
        """
        loop = asyncio.get_running_loop()
        while True:
            async with self._cooldown_lock:
                remaining = self._cooldown_until - loop.time()
            if remaining <= 0:
                return
            await asyncio.sleep(remaining)

    async def _register_throttle(self, response: httpx.Response) -> None:
        self.throttle_responses += 1
        delay = self._retry_after_seconds(response)
        if delay is None:
            delay = max(self.throttle_floor_seconds, self.backoff)
        await self._extend_cooldown(delay)

    async def _get(self, params: dict[str, str]) -> httpx.Response:
        retrying = AsyncRetrying(
            stop=stop_after_attempt(self.max_retries + 1),
            wait=self._wait_policy(),
            retry=retry_if_exception(_retryable_http_error),
            reraise=True,
        )
        async for attempt in retrying:
            with attempt:
                await self._wait_for_cooldown()

                async def request_once() -> httpx.Response:
                    loop = asyncio.get_running_loop()
                    started = loop.time()
                    self.http_requests += 1
                    try:
                        return await self.client.get(
                            self.endpoint,
                            params=params,
                        )
                    except (httpx.TimeoutException, httpx.TransportError):
                        self.transport_errors += 1
                        raise
                    finally:
                        elapsed_ms = max(
                            0,
                            int(round((loop.time() - started) * 1000.0)),
                        )
                        self.http_elapsed_milliseconds += elapsed_ms
                        if elapsed_ms <= 2_000:
                            bucket = "le_2s"
                        elif elapsed_ms <= 4_000:
                            bucket = "le_4s"
                        elif elapsed_ms <= 8_000:
                            bucket = "le_8s"
                        elif elapsed_ms <= 16_000:
                            bucket = "le_16s"
                        elif elapsed_ms <= 30_000:
                            bucket = "le_30s"
                        else:
                            bucket = "gt_30s"
                        self.http_latency_buckets[bucket] += 1

                if self._limiter is None:
                    response = await request_once()
                else:
                    async with self._limiter:
                        # Cooldown may have been extended while this coroutine
                        # was waiting for a rate token.
                        await self._wait_for_cooldown()
                        response = await request_once()
                self.http_status_counts[int(response.status_code)] += 1
                if response.status_code == 429 or response.status_code == 503:
                    await self._register_throttle(response)
                    response.raise_for_status()
                if response.status_code >= 500:
                    response.raise_for_status()
                if response.status_code >= 400:
                    raise ValueError(
                        f"CDX rejected request with HTTP {response.status_code}"
                    )
                return response
        raise AssertionError("unreachable")

    async def iter_range_pages(
        self,
        hostname: str,
        year_from: int,
        year_to: int,
        *,
        page_limit: int | None = None,
    ) -> AsyncIterator[Page]:
        if not 1996 <= year_from <= year_to <= 2001:
            raise ValueError("year range must be within 1996-2001")
        effective_limit = self.limit if page_limit is None else int(page_limit)
        if effective_limit < 1:
            raise ValueError("page_limit must be positive")
        query = {
            "url": f"http://{hostname}/",
            "matchType": "host",
            "from": f"{year_from}0101000000",
            "to": f"{year_to}1231235959",
            "output": "json",
            # urlkey is intentionally present: Wayback resume-key pagination
            # depends on the sort key being part of the selected CDX fields.
            "fl": "urlkey,timestamp,original,statuscode,digest,length",
            # Server-side filtering removes captures that can never satisfy
            # Creeper's acceptance predicate. Local validation remains the
            # final authority for every returned row.
            "filter": "statuscode:[23][0-9][0-9]",
            "gzip": "false",
            "showResumeKey": "true",
            "limit": str(effective_limit),
        }
        resume_key: str | None = None
        while True:
            params = dict(query)
            if resume_key is not None:
                params["resumeKey"] = resume_key
            response = await self._get(params)
            rows, next_key = WaybackCDXClient._parse_payload(response.content)
            if next_key:
                yield rows, False
                if next_key == resume_key:
                    raise ConnectionError("CDX returned a repeated resume key")
                resume_key = next_key
                continue
            yield rows, True
            return

    @staticmethod
    def _accepted_year(
        row: dict[str, object],
        *,
        hostname: str,
        year_from: int,
        year_to: int,
    ) -> int | None:
        timestamp = str(row.get("timestamp", ""))
        original = str(row.get("original", ""))
        status = str(row.get("status", row.get("statuscode", "")))
        if (
            len(timestamp) >= 4
            and timestamp[:4].isdigit()
            and year_from <= int(timestamp[:4]) <= year_to
            and _exact_hostname(original, hostname)
            and status[:1] in {"2", "3"}
        ):
            return int(timestamp[:4])
        return None

    @staticmethod
    def _capsule_from_row(
        key: EvidenceQueryKey,
        row: dict[str, object],
        *,
        year: int,
        page_no: int,
        record_no: int,
        extraction_method: str,
    ) -> EvidenceCapsule:
        original = str(row.get("original", ""))
        timestamp = str(row.get("timestamp", ""))
        payload = json.dumps(row, ensure_ascii=False, sort_keys=True).encode()
        return EvidenceCapsule(
            hostname=key.hostname,
            year=year,
            provider=key.provider,
            temporal_semantics="capture_timestamp_year",
            evidence_timestamp=timestamp,
            source_locator=original,
            payload_hash=hashlib.sha256(payload).hexdigest(),
            policy_version=key.policy_version,
            evidence_type="exact_host_cdx_capture",
            source_id=key.provider,
            original_url=original,
            record_locator=(
                f"{key.provider}:{key.hostname}:{year}:"
                f"page={page_no}:record={record_no}"
            ),
            extraction_method=extraction_method,
        )

    async def query_range(self, key: EvidenceQueryKey) -> RangeEvidenceQueryResult:
        """Probe a multi-year range and retain one accepted capture per year.

        Positive rows are individually authoritative even if a later page fails.
        Exhaustive negative conclusions are emitted only after the range has
        reached a complete final page.
        """
        if key.provider != self.provider:
            raise ValueError(
                f"provider mismatch: key={key.provider!r}, client={self.provider!r}"
            )
        scope = key.temporal_scope
        if scope.year_from == scope.year_to:
            raise ValueError("range provider requires a multi-year task")
        capsules_by_year: dict[int, EvidenceCapsule] = {}
        pages_seen = records_seen = 0
        last_page_complete: bool | None = None
        try:
            async for page, complete in self.iter_range_pages(
                key.hostname, scope.year_from, scope.year_to
            ):
                pages_seen += 1
                last_page_complete = complete
                for row in page:
                    records_seen += 1
                    year = self._accepted_year(
                        row,
                        hostname=key.hostname,
                        year_from=scope.year_from,
                        year_to=scope.year_to,
                    )
                    if year is None or year in capsules_by_year:
                        continue
                    capsules_by_year[year] = self._capsule_from_row(
                        key,
                        row,
                        year=year,
                        page_no=pages_seen,
                        record_no=records_seen,
                        extraction_method="cdx_query_range",
                    )
            complete_years = (
                tuple(sorted(capsules_by_year))
                if last_page_complete is True
                else ()
            )
            return RangeEvidenceQueryResult(
                hostname=key.hostname,
                key=key,
                state=(
                    CDXQueryState.PASS
                    if complete_years
                    else (
                        CDXQueryState.EMPTY_EXHAUSTIVE
                        if last_page_complete is True
                        else CDXQueryState.INCOMPLETE
                    )
                ),
                candidate_years=complete_years,
                capsules=tuple(capsules_by_year[year] for year in sorted(capsules_by_year)),
                pages_seen=pages_seen,
                records_seen=records_seen,
                error=None,
            )
        except ValueError as exc:
            return RangeEvidenceQueryResult(
                hostname=key.hostname,
                key=key,
                state=CDXQueryState.INVALID,
                capsules=tuple(capsules_by_year[year] for year in sorted(capsules_by_year)),
                pages_seen=pages_seen,
                records_seen=records_seen,
                error=str(exc),
            )
        except (
            httpx.TimeoutException,
            httpx.TransportError,
            httpx.HTTPStatusError,
            ConnectionError,
        ) as exc:
            return RangeEvidenceQueryResult(
                hostname=key.hostname,
                key=key,
                state=CDXQueryState.TRANSIENT_ERROR,
                capsules=tuple(capsules_by_year[year] for year in sorted(capsules_by_year)),
                pages_seen=pages_seen,
                records_seen=records_seen,
                error=str(exc) or type(exc).__name__,
            )

    async def query_key(self, key: EvidenceQueryKey) -> EvidenceQueryResult:
        """Execute one exact-year durable EvidenceQueryKey."""
        if key.provider != self.provider:
            raise ValueError(
                f"provider mismatch: key={key.provider!r}, client={self.provider!r}"
            )
        scope = key.temporal_scope
        if scope.year_from != scope.year_to:
            raise ValueError("exact-year provider received a range task")
        hostname = key.hostname
        year = scope.year_from
        pages_seen = records_seen = 0
        last_page_complete: bool | None = None
        try:
            async for page, complete in self.iter_range_pages(
                hostname,
                year,
                year,
                page_limit=1,
            ):
                pages_seen += 1
                last_page_complete = complete
                for row in page:
                    records_seen += 1
                    accepted_year = self._accepted_year(
                        row,
                        hostname=hostname,
                        year_from=year,
                        year_to=year,
                    )
                    if accepted_year is not None:
                        capsule = self._capsule_from_row(
                            key,
                            row,
                            year=accepted_year,
                            page_no=pages_seen,
                            record_no=records_seen,
                            extraction_method="cdx_query_year",
                        )
                        return EvidenceQueryResult(
                            hostname,
                            year,
                            CDXQueryState.PASS,
                            capsule=capsule,
                            pages_seen=pages_seen,
                            records_seen=records_seen,
                            key=key,
                        )
            return EvidenceQueryResult(
                hostname,
                year,
                CDXQueryState.EMPTY_EXHAUSTIVE
                if last_page_complete is True
                else CDXQueryState.INCOMPLETE,
                pages_seen=pages_seen,
                records_seen=records_seen,
                key=key,
            )
        except ValueError as exc:
            return EvidenceQueryResult(
                hostname,
                year,
                CDXQueryState.INVALID,
                pages_seen=pages_seen,
                records_seen=records_seen,
                error=str(exc),
                key=key,
            )
        except (
            httpx.TimeoutException,
            httpx.TransportError,
            httpx.HTTPStatusError,
            ConnectionError,
        ) as exc:
            return EvidenceQueryResult(
                hostname,
                year,
                CDXQueryState.TRANSIENT_ERROR,
                pages_seen=pages_seen,
                records_seen=records_seen,
                error=str(exc) or type(exc).__name__,
                key=key,
            )
