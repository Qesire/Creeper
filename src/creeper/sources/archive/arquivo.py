"""Bounded Arquivo.pt CDX discovery adapter.

Arquivo.pt rows are discovery observations only. They are not annual authority
records and do not become accepted evidence without the existing evidence gate.

HTTP connection pooling, timeout handling, redirects, and transport errors are
delegated to HTTPX. Bounded retry/backoff is delegated to Tenacity. Creeper
retains only source-specific query construction and record semantics.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Iterator
from urllib.parse import urlencode, urlsplit

import httpx
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_none, wait_random_exponential

from creeper.authority.normalizer import normalize_official
from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import HostObservation, SourceRecord


Fetch = Callable[[str, float, dict[str, str]], bytes]


def _retryable_http_error(exc: BaseException) -> bool:
    if isinstance(exc, (httpx.TimeoutException, httpx.TransportError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status == 429 or status >= 500
    return False


class ArquivoCDXClient:
    """Query Arquivo.pt CDX through a reusable mature HTTP transport.

    ``fetch`` remains injectable for deterministic/offline tests. Production
    callers should reuse one client instance (or provide one shared
    ``httpx.Client``) so keep-alive and connection pooling remain effective.
    """

    def __init__(
        self,
        endpoint: str = "https://arquivo.pt/wayback/cdx",
        *,
        limit: int = 1_000,
        timeout: float = 30.0,
        max_retries: int = 3,
        backoff: float = 0.5,
        max_backoff: float = 30.0,
        max_connections: int = 8,
        max_keepalive_connections: int = 4,
        fetch: Fetch | None = None,
        client: httpx.Client | None = None,
        transport: httpx.BaseTransport | None = None,
        user_agent: str = "Creeper/2.2 (research; contact administrator)",
    ):
        if (
            not endpoint.strip()
            or limit < 1
            or timeout <= 0
            or max_retries < 0
            or backoff < 0
            or max_backoff < 0
            or max_connections < 1
            or max_keepalive_connections < 0
            or max_keepalive_connections > max_connections
        ):
            raise ValueError("invalid Arquivo.pt CDX client limits")
        if fetch is not None and (client is not None or transport is not None):
            raise ValueError("fetch cannot be combined with client/transport")
        if client is not None and transport is not None:
            raise ValueError("pass either client or transport, not both")
        self.endpoint = endpoint
        self.limit = limit
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff
        self.max_backoff = max_backoff
        self.user_agent = user_agent
        self.fetch = fetch
        self.last_request_url: str | None = None
        self.http_requests = 0
        self._owns_client = fetch is None and client is None
        self.client = None if fetch is not None else client or httpx.Client(
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

    def __enter__(self) -> "ArquivoCDXClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client and self.client is not None:
            self.client.close()

    def _wait_policy(self):
        if self.backoff == 0 or self.max_backoff == 0:
            return wait_none()
        return wait_random_exponential(
            multiplier=self.backoff,
            max=max(self.backoff, self.max_backoff),
        )

    def _request(self, request_url: str) -> bytes:
        headers = {"User-Agent": self.user_agent, "Accept": "application/json"}
        if self.fetch is not None:
            self.http_requests += 1
            return self.fetch(request_url, self.timeout, headers)

        assert self.client is not None
        retrying = Retrying(
            stop=stop_after_attempt(self.max_retries + 1),
            wait=self._wait_policy(),
            retry=retry_if_exception(_retryable_http_error),
            reraise=True,
        )
        for attempt in retrying:
            with attempt:
                self.http_requests += 1
                response = self.client.get(request_url)
                if response.status_code == 429 or response.status_code >= 500:
                    response.raise_for_status()
                if response.status_code >= 400:
                    raise ValueError(
                        f"Arquivo.pt CDX rejected request with HTTP {response.status_code}"
                    )
                return response.content
        raise AssertionError("unreachable")

    def query(
        self,
        url: str,
        *,
        from_year: int,
        to_year: int,
        match_type: str = "domain",
    ) -> list[dict[str, object]]:
        if from_year < 1 or to_year < from_year or to_year > 9999:
            raise ValueError("invalid Arquivo.pt CDX year range")
        if match_type not in {"exact", "prefix", "host", "domain"}:
            raise ValueError("invalid Arquivo.pt CDX match type")
        params = {
            "url": url,
            "matchType": match_type,
            "from": str(from_year),
            "to": str(to_year),
            "output": "json",
            "fields": "url,timestamp,status,mime,digest,length,offset,filename",
            "limit": str(self.limit),
        }
        request_url = f"{self.endpoint}?{urlencode(params)}"
        self.last_request_url = request_url
        return self._parse_payload(self._request(request_url))

    @staticmethod
    def _parse_payload(payload: bytes) -> list[dict[str, object]]:
        text = payload.decode("utf-8", errors="replace").strip()
        if not text:
            return []
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            rows: list[dict[str, object]] = []
            for line in text.splitlines():
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(item, dict):
                    rows.append(item)
            return rows
        if isinstance(value, dict):
            return [value]
        if not isinstance(value, list):
            return []
        return [item for item in value if isinstance(item, dict)]


class ArquivoCDXSource:
    """Enumerate bounded capture rows for explicit host/domain seeds."""

    scope = CandidateSourceScope.LOCAL_DISCOVERY

    def __init__(
        self,
        client: ArquivoCDXClient,
        *,
        seed_urls: Iterable[str],
        from_year: int,
        to_year: int,
        match_type: str = "domain",
    ):
        self.client = client
        self.seed_urls = tuple(seed.strip() for seed in seed_urls if seed.strip())
        if not self.seed_urls:
            raise ValueError("at least one Arquivo.pt seed URL is required")
        self.from_year = from_year
        self.to_year = to_year
        self.match_type = match_type

    def enumerate(
        self,
        *,
        limit_per_seed: int | None = None,
        total_limit: int | None = None,
    ) -> Iterator[SourceRecord]:
        if limit_per_seed is not None and limit_per_seed < 1:
            raise ValueError("limit_per_seed must be positive")
        if total_limit is not None and total_limit < 1:
            raise ValueError("total_limit must be positive")
        emitted = 0
        for seed in self.seed_urls:
            rows = self.client.query(
                seed,
                from_year=self.from_year,
                to_year=self.to_year,
                match_type=self.match_type,
            )
            for row_number, row in enumerate(rows, 1):
                if limit_per_seed is not None and row_number > limit_per_seed:
                    break
                if total_limit is not None and emitted >= total_limit:
                    return
                capture_url = str(row.get("url", "")).strip()
                if not capture_url:
                    continue
                timestamp = str(row.get("timestamp", ""))
                source_year = int(timestamp[:4]) if timestamp[:4].isdigit() else None
                locator = f"{self.client.last_request_url}#row={row_number}"
                yield SourceRecord(
                    source_id=f"arquivo_pt_cdx:{seed}",
                    locator=locator,
                    payload=capture_url,
                    scope=self.scope,
                    source_year=source_year,
                )
                emitted += 1

    def extract_hosts(self, record: SourceRecord) -> Iterable[HostObservation]:
        try:
            hostname = urlsplit(record.payload).hostname
        except ValueError:
            hostname = None
        normalized = normalize_official(hostname or "")
        if normalized:
            yield HostObservation(
                hostname=normalized,
                source_id=record.source_id,
                locator=record.locator,
                scope=record.scope,
                source_year=record.source_year,
            )
