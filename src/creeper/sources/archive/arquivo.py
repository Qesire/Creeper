"""Bounded Arquivo.pt CDX discovery adapter.

Arquivo.pt rows are discovery observations only. They are not annual authority
records and do not become accepted evidence without the existing evidence gate.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Iterator
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

from creeper.authority.normalizer import normalize_official
from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import HostObservation, SourceRecord


Fetch = Callable[[str, float, dict[str, str]], bytes]


class ArquivoCDXClient:
    """Query the public Arquivo.pt CDX endpoint with an explicit row limit."""

    def __init__(
        self,
        endpoint: str = "https://arquivo.pt/wayback/cdx",
        *,
        limit: int = 1_000,
        timeout: float = 30.0,
        fetch: Fetch | None = None,
        user_agent: str = "Creeper/2.1 (research; contact administrator)",
    ):
        if not endpoint.strip() or limit < 1 or timeout <= 0:
            raise ValueError("invalid Arquivo.pt CDX client limits")
        self.endpoint = endpoint
        self.limit = limit
        self.timeout = timeout
        self.fetch = fetch or self._fetch
        self.user_agent = user_agent
        self.last_request_url: str | None = None

    @staticmethod
    def _fetch(url: str, timeout: float, headers: dict[str, str]) -> bytes:
        request = Request(url, headers=headers)
        with urlopen(request, timeout=timeout) as response:
            return response.read()

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
        payload = self.fetch(
            request_url,
            self.timeout,
            {"User-Agent": self.user_agent, "Accept": "application/json"},
        )
        return self._parse_payload(payload)

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

