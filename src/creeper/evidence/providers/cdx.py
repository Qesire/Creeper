"""Deterministic CDX acceptance state machine.

The transport is injected so tests and local replay runs do not need network
access. A transport returns pages of dictionaries and a completion flag.
"""

from __future__ import annotations

import hashlib
import json
import gzip
import time
from collections.abc import Callable, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from creeper.authority.normalizer import normalize_official
from creeper.evidence.limits import RequestRateLimiter
from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceCapsule,
    EvidenceQueryKey,
    EvidenceQueryResult,
    TemporalScope,
    is_year_timestamp,
)


Page = tuple[list[dict[str, object]], bool]
Transport = Callable[[str, int], Iterable[Page]]


class WaybackCDXClient:
    """Rate-limited Wayback CDX transport using bounded resume-key retries."""

    def __init__(
        self,
        endpoint: str = "https://web.archive.org/cdx/search/cdx",
        *,
        limit: int = 1_000,
        timeout: float = 30.0,
        max_retries: int = 3,
        backoff: float = 1.0,
        requests_per_second: float = 0.0,
        user_agent: str = "Creeper/2.1 (research; contact administrator)",
        fetch: Callable[[str, float, dict[str, str]], bytes] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if limit < 1 or timeout <= 0 or max_retries < 0 or backoff < 0 or requests_per_second < 0:
            raise ValueError("invalid CDX client limits")
        self.endpoint = endpoint
        self.limit = limit
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff
        self.user_agent = user_agent
        self.fetch = fetch or self._fetch
        self.sleep = sleep
        self.rate_limiter = RequestRateLimiter(
            requests_per_second,
            sleep=sleep,
        )
        self.http_requests = 0

    @staticmethod
    def _fetch(url: str, timeout: float, headers: dict[str, str]) -> bytes:
        request = Request(url, headers=headers)
        with urlopen(request, timeout=timeout) as response:
            return response.read()

    def _request(self, url: str) -> bytes:
        for attempt in range(self.max_retries + 1):
            try:
                self.rate_limiter.acquire()
                self.http_requests += 1
                return self.fetch(
                    url,
                    self.timeout,
                    {"User-Agent": self.user_agent, "Accept-Encoding": "gzip"},
                )
            except HTTPError as exc:
                if exc.code < 500 and exc.code != 429:
                    raise ValueError(f"CDX rejected request with HTTP {exc.code}") from exc
                if attempt >= self.max_retries:
                    raise ConnectionError(f"CDX HTTP {exc.code}") from exc
            except (TimeoutError, URLError, OSError) as exc:
                if attempt >= self.max_retries:
                    raise ConnectionError(str(exc) or type(exc).__name__) from exc
            self.sleep(self.backoff * (2**attempt))
        raise AssertionError("unreachable")

    def __call__(self, hostname: str, year: int) -> Iterable[Page]:
        query = {
            "url": f"http://{hostname}/",
            "matchType": "host",
            "from": f"{year}0101000000",
            "to": f"{year}1231235959",
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
            url = f"{self.endpoint}?{urlencode(params)}"
            payload = self._request(url)
            rows, next_key = self._parse_payload(payload)
            if next_key:
                yield rows, False
                if next_key == resume_key:
                    raise ConnectionError("CDX returned a repeated resume key")
                resume_key = next_key
                continue
            yield rows, True
            return

    @staticmethod
    def _parse_payload(payload: bytes) -> tuple[list[dict[str, object]], str | None]:
        if payload[:2] == b"\x1f\x8b":
            payload = gzip.decompress(payload)
        value = json.loads(payload.decode("utf-8"))
        if not isinstance(value, list) or not value:
            return [], None
        header = value[0]
        if not isinstance(header, list) or not all(isinstance(item, str) for item in header):
            raise ValueError("CDX JSON response has no field header")
        rows: list[dict[str, object]] = []
        resume_key: str | None = None
        for item in value[1:]:
            if not isinstance(item, list):
                continue
            if len(item) == 0:
                continue
            if len(item) == 1 and isinstance(item[0], str):
                resume_key = item[0]
                continue
            if len(item) != len(header):
                continue
            row = dict(zip(header, item, strict=True))
            if "statuscode" in row and "status" not in row:
                row["status"] = row["statuscode"]
            rows.append(row)
        return rows, resume_key


def _exact_hostname(original: str, hostname: str) -> bool:
    try:
        parsed = urlsplit(original)
    except ValueError:
        return False
    return normalize_official(parsed.hostname or "") == hostname


def query_year(
    hostname: str,
    year: int,
    transport: Transport,
    *,
    provider: str = "cdx",
    policy_version: str = "cdx-v1",
) -> EvidenceQueryResult:
    normalized = normalize_official(hostname)
    if normalized is None or year < 1996 or year > 2001:
        return EvidenceQueryResult(hostname, year, CDXQueryState.INVALID)
    key = EvidenceQueryKey(
        normalized,
        TemporalScope(year, year),
        provider,
        policy_version,
    )
    pages_seen = records_seen = 0
    last_page_complete: bool | None = None
    try:
        for page, complete in transport(normalized, year):
            pages_seen += 1
            records_seen += len(page)
            last_page_complete = complete
            for row in page:
                timestamp = str(row.get("timestamp", ""))
                original = str(row.get("original", ""))
                status = str(row.get("status", ""))
                if (
                    is_year_timestamp(timestamp, year)
                    and _exact_hostname(original, normalized)
                    and status[:1] in {"2", "3"}
                ):
                    payload = json.dumps(row, ensure_ascii=False, sort_keys=True).encode()
                    capsule = EvidenceCapsule(
                        hostname=normalized,
                        year=year,
                        provider=provider,
                        temporal_semantics="capture_timestamp_year",
                        evidence_timestamp=timestamp,
                        source_locator=original,
                        payload_hash=hashlib.sha256(payload).hexdigest(),
                        policy_version=policy_version,
                    )
                    return EvidenceQueryResult(
                        normalized,
                        year,
                        CDXQueryState.PASS,
                        key=key,
                        capsule=capsule,
                        pages_seen=pages_seen,
                        records_seen=records_seen,
                    )
        return EvidenceQueryResult(
            normalized,
            year,
            CDXQueryState.EMPTY_EXHAUSTIVE if last_page_complete is True else CDXQueryState.INCOMPLETE,
            key=key,
            pages_seen=pages_seen,
            records_seen=records_seen,
        )
    except (TimeoutError, ConnectionError) as exc:
        return EvidenceQueryResult(
            normalized,
            year,
            CDXQueryState.TRANSIENT_ERROR,
            key=key,
            pages_seen=pages_seen,
            records_seen=records_seen,
            error=str(exc) or type(exc).__name__,
        )
    except ValueError as exc:
        return EvidenceQueryResult(
            normalized,
            year,
            CDXQueryState.INVALID,
            key=key,
            pages_seen=pages_seen,
            records_seen=records_seen,
            error=str(exc),
        )


def query_missing_years(
    hostname: str,
    missing_years: Iterable[int],
    transport: Transport,
    *,
    provider: str = "cdx",
    policy_version: str = "cdx-v1",
) -> list[EvidenceQueryResult]:
    """Run one exact-year state machine per missing year."""
    return [
        query_year(
            hostname,
            year,
            transport,
            provider=provider,
            policy_version=policy_version,
        )
        for year in missing_years
    ]
