"""Mature asynchronous Wayback CDX transport.

HTTP connection pooling and timeout handling are delegated to HTTPX, bounded
retry/backoff to Tenacity, and request-rate shaping to aiolimiter. Creeper
retains only the competition-specific exact-host/exact-year acceptance rules.
"""

from __future__ import annotations

import hashlib
import json
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
    is_year_timestamp,
)
from creeper.evidence.providers.cdx import Page, WaybackCDXClient, _exact_hostname


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
        user_agent: str = "Creeper/2.2 (research; contact administrator)",
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
        self.http_requests = 0
        self._limiter = (
            AsyncLimiter(requests_per_second, time_period=1.0)
            if requests_per_second > 0
            else None
        )
        self._owns_client = client is None
        self.client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_keepalive_connections,
            ),
            headers={
                "User-Agent": user_agent,
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
            },
            follow_redirects=True,
            transport=transport,
        )

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

    async def _get(self, params: dict[str, str]) -> httpx.Response:
        retrying = AsyncRetrying(
            stop=stop_after_attempt(self.max_retries + 1),
            wait=self._wait_policy(),
            retry=retry_if_exception(_retryable_http_error),
            reraise=True,
        )
        async for attempt in retrying:
            with attempt:
                if self._limiter is None:
                    self.http_requests += 1
                    response = await self.client.get(self.endpoint, params=params)
                else:
                    async with self._limiter:
                        self.http_requests += 1
                        response = await self.client.get(self.endpoint, params=params)
                if response.status_code == 429 or response.status_code >= 500:
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
    ) -> AsyncIterator[Page]:
        if not 1996 <= year_from <= year_to <= 2001:
            raise ValueError("year range must be within 1996-2001")
        query = {
            "url": f"http://{hostname}/",
            "matchType": "host",
            "from": f"{year_from}0101000000",
            "to": f"{year_to}1231235959",
            "output": "json",
            "fl": "timestamp,original,statuscode,mimetype,digest,length",
            "gzip": "false",
            "showResumeKey": "true",
            "limit": str(self.limit),
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
            async for page, complete in self.iter_range_pages(hostname, year, year):
                pages_seen += 1
                records_seen += len(page)
                last_page_complete = complete
                for row in page:
                    timestamp = str(row.get("timestamp", ""))
                    original = str(row.get("original", ""))
                    status = str(row.get("status", row.get("statuscode", "")))
                    if (
                        is_year_timestamp(timestamp, year)
                        and _exact_hostname(original, hostname)
                        and status[:1] in {"2", "3"}
                    ):
                        payload = json.dumps(
                            row, ensure_ascii=False, sort_keys=True
                        ).encode()
                        capsule = EvidenceCapsule(
                            hostname=hostname,
                            year=year,
                            provider=key.provider,
                            temporal_semantics="capture_timestamp_year",
                            evidence_timestamp=timestamp,
                            source_locator=original,
                            payload_hash=hashlib.sha256(payload).hexdigest(),
                            policy_version=key.policy_version,
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
