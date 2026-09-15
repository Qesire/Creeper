"""Deterministic seed-driven web search execution for Creeper Fabric."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit, urlunsplit

import httpx
from selectolax.parser import HTMLParser

from creeper.authority.normalizer import normalize_official
from creeper.distributed.coordinator_client import CoordinatorClient
from creeper.distributed.http_transport import build_authority_transport
from creeper.distributed.lease_keeper import LeaseKeeper
from creeper.distributed.models import TaskClass, TaskLease
from creeper.distributed.provider_gate import DistributedProviderGate
from creeper.distributed.search_campaign import SearchCampaign
from creeper.distributed.source_discovery import _classify_candidate
from creeper.distributed.urlcanon import canonical_http_url


@dataclass(frozen=True)
class SeededSearchLimits:
    max_queries: int = 16
    max_response_bytes: int = 1024 * 1024
    max_links_per_query: int = 128
    timeout_seconds: float = 20.0

    def __post_init__(self) -> None:
        if (
            self.max_queries < 1
            or self.max_response_bytes < 4096
            or self.max_links_per_query < 1
            or self.timeout_seconds <= 0
        ):
            raise ValueError("invalid seeded search limits")


class SeededSearchProducer:
    """Execute one frozen campaign slice without any remote LLM dependency."""

    EOF_CURSOR = "EOF"

    def __init__(
        self,
        *,
        limits: SeededSearchLimits | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.limits = limits or SeededSearchLimits()
        self.transport = transport

    @staticmethod
    def _unwrap_result_url(base_url: str, href: str) -> str | None:
        absolute = urljoin(base_url, href)
        parsed = urlsplit(absolute)
        query = parse_qs(parsed.query)
        for key in ("url", "u", "target", "uddg"):
            values = query.get(key)
            if values:
                candidate = values[0].strip()
                if candidate.startswith(("http://", "https://")):
                    absolute = candidate
                    break
        try:
            return canonical_http_url(absolute)
        except ValueError:
            return None

    @staticmethod
    def _query_url(endpoint: str, query_param: str, query: str) -> str:
        parsed = urlsplit(endpoint)
        params = parse_qs(parsed.query, keep_blank_values=True)
        params[query_param] = [query]
        query_string = urlencode(params, doseq=True)
        return urlunsplit(
            (parsed.scheme, parsed.netloc, parsed.path, query_string, "")
        )

    async def __call__(
        self,
        lease: TaskLease,
        coordinator: CoordinatorClient,
        keeper: LeaseKeeper,
    ) -> None:
        if lease.work.task_class is not TaskClass.SEARCH_SLICE:
            raise ValueError("SeededSearchProducer requires SEARCH_SLICE")
        if lease.cursor == self.EOF_CURSOR:
            return

        coverage = dict(lease.work.coverage)
        campaign_raw = coverage.get("campaign")
        if not isinstance(campaign_raw, dict):
            raise ValueError("SEARCH_SLICE requires frozen campaign mapping")
        campaign = SearchCampaign.from_mapping(campaign_raw)
        if str(coverage.get("campaign_id", "")) != campaign.campaign_id:
            raise ValueError("SEARCH_SLICE campaign identity mismatch")

        seed = int(coverage.get("seed", -1))
        slot_start = int(coverage.get("slot_start", -1))
        slot_count = int(coverage.get("slot_count", 0))
        if (
            seed < 0
            or slot_start < 0
            or not 1 <= slot_count <= self.limits.max_queries
        ):
            raise ValueError("invalid SEARCH_SLICE seed/slot bounds")

        endpoint = canonical_http_url(str(coverage.get("search_endpoint", "")))
        query_param = str(coverage.get("query_param", "q")).strip()
        provider = str(coverage.get("provider", "web_search")).strip()
        max_bytes = min(
            self.limits.max_response_bytes,
            int(
                coverage.get(
                    "max_response_bytes",
                    self.limits.max_response_bytes,
                )
            ),
        )
        max_links = min(
            self.limits.max_links_per_query,
            int(
                coverage.get(
                    "max_links_per_query",
                    self.limits.max_links_per_query,
                )
            ),
        )
        if (
            not query_param
            or not provider
            or max_bytes < 4096
            or max_links < 1
        ):
            raise ValueError("invalid SEARCH_SLICE provider limits")

        endpoint_host = normalize_official(urlsplit(endpoint).hostname or "")
        gate = DistributedProviderGate(
            coordinator,
            keeper,
            provider,
            throttle_floor_seconds=1.0,
        )
        transport = build_authority_transport(
            acquire=gate.acquire,
            report=gate.report,
            max_connections=2,
            max_keepalive_connections=1,
            keepalive_expiry_seconds=20.0,
            inner=self.transport,
        )

        source_results: dict[str, dict[str, object]] = {}
        host_results: dict[str, dict[str, object]] = {}
        queries = campaign.render_slice(
            seed=seed,
            slot_start=slot_start,
            slot_count=slot_count,
        )

        async with httpx.AsyncClient(
            transport=transport,
            timeout=httpx.Timeout(self.limits.timeout_seconds),
            follow_redirects=True,
            trust_env=False,
            headers={
                "User-Agent": (
                    "Creeper-Fabric/0.1 seeded-search "
                    "(research; https://github.com/Qesire/Creeper)"
                ),
                "Accept": "text/html,application/xhtml+xml;q=0.9,text/plain;q=0.5",
                "Accept-Encoding": "gzip, deflate",
            },
        ) as client:
            for offset, query in enumerate(queries):
                keeper.assert_owned()
                request_url = self._query_url(endpoint, query_param, query)
                async with client.stream("GET", request_url) as response:
                    status = int(response.status_code)
                    if status >= 500:
                        raise RuntimeError(
                            f"seeded search provider HTTP {status}"
                        )
                    if status >= 400:
                        continue
                    content_type = response.headers.get(
                        "Content-Type",
                        "",
                    ).split(";", 1)[0].strip().lower()
                    if content_type not in {
                        "",
                        "text/html",
                        "application/xhtml+xml",
                        "text/plain",
                    }:
                        continue

                    chunks: list[bytes] = []
                    seen = 0
                    async for chunk in response.aiter_bytes():
                        keeper.assert_owned()
                        if seen + len(chunk) > max_bytes:
                            remaining = max_bytes - seen
                            if remaining > 0:
                                chunks.append(chunk[:remaining])
                            break
                        chunks.append(chunk)
                        seen += len(chunk)
                    payload = b"".join(chunks)
                    final_url = canonical_http_url(str(response.url))

                links: list[str] = []
                if content_type == "text/plain":
                    for raw_line in payload.decode(
                        "utf-8",
                        errors="replace",
                    ).splitlines():
                        value = raw_line.strip()
                        if not value.startswith(("http://", "https://")):
                            continue
                        try:
                            links.append(canonical_http_url(value))
                        except ValueError:
                            continue
                else:
                    tree = HTMLParser(payload)
                    for node in tree.css("a[href]"):
                        href = node.attributes.get("href", "").strip()
                        if not href:
                            continue
                        candidate = self._unwrap_result_url(final_url, href)
                        if candidate is not None:
                            links.append(candidate)

                for url in sorted(set(links))[:max_links]:
                    hostname = normalize_official(urlsplit(url).hostname or "")
                    if hostname is None or hostname == endpoint_host:
                        continue
                    candidate_type, parser_kind = _classify_candidate(url)
                    source_results.setdefault(
                        url,
                        {
                            "kind": "SOURCE_CANDIDATE",
                            "url": url,
                            "candidate_type": candidate_type,
                            "parser_kind": parser_kind,
                            "referrer_url": final_url,
                        },
                    )
                    host_results.setdefault(
                        hostname,
                        {
                            "kind": "HOST_CANDIDATE",
                            "hostname": hostname,
                            "source": "seeded_search",
                            "locator": url,
                            "campaign_id": campaign.campaign_id,
                            "seed": seed,
                            "slot": slot_start + offset,
                            "query": query,
                        },
                    )

        keeper.assert_owned()
        results = list(source_results.values()) + list(host_results.values())
        await coordinator.commit_batch(
            keeper.lease,
            sequence_no=lease.next_sequence_no,
            results=results,
            cursor_after=self.EOF_CURSOR,
        )
