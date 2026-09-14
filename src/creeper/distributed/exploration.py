"""Evidence-only historical exploration executed entirely on remote workers."""

from __future__ import annotations

import hashlib
import heapq
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
from creeper.distributed.remote_resolver import RemoteHostnameResolver
from creeper.distributed.search_campaign import SearchCampaign
from creeper.distributed.urlcanon import canonical_http_url
from creeper.evidence.providers.multi_cdx import CDXProviderConfig


_STRONG_PATH_TOKENS = (
    "links",
    "link",
    "webring",
    "guestbook",
    "member",
    "members",
    "user",
    "users",
    "people",
    "person",
    "home",
    "homepage",
    "personal",
    "directory",
    "resource",
    "resources",
    "friend",
    "friends",
    "site",
    "sites",
    "archive",
    "index",
)

_SKIP_SUFFIXES = (
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".webp",
    ".svg",
    ".css",
    ".js",
    ".ico",
    ".pdf",
    ".zip",
    ".tar",
    ".gz",
    ".mp3",
    ".mp4",
    ".avi",
    ".mov",
)


@dataclass(frozen=True)
class ExplorationLimits:
    max_pages: int = 32
    max_hosts: int = 128
    max_depth: int = 3
    max_links_per_page: int = 128
    max_response_bytes: int = 2 * 1024 * 1024
    timeout_seconds: float = 20.0

    def __post_init__(self) -> None:
        if (
            self.max_pages < 1
            or self.max_hosts < 1
            or self.max_depth < 0
            or self.max_links_per_page < 1
            or self.max_response_bytes < 4096
            or self.timeout_seconds <= 0
        ):
            raise ValueError("invalid historical exploration limits")


@dataclass(frozen=True)
class SeededExplorationLimits:
    max_queries: int = 8
    max_search_response_bytes: int = 1024 * 1024
    max_results_per_query: int = 64

    def __post_init__(self) -> None:
        if (
            self.max_queries < 1
            or self.max_search_response_bytes < 4096
            or self.max_results_per_query < 1
        ):
            raise ValueError("invalid seeded exploration limits")


class HistoricalCrawlerEngine:
    """Bounded local frontier whose hostname outputs are consumed immediately."""

    def __init__(
        self,
        resolver: RemoteHostnameResolver,
        coordinator: CoordinatorClient,
        keeper: LeaseKeeper,
        *,
        limits: ExplorationLimits | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        provider: str = "web_discovery",
    ) -> None:
        self.resolver = resolver
        self.coordinator = coordinator
        self.keeper = keeper
        self.limits = limits or ExplorationLimits()
        self.transport = transport
        self.provider = provider

    @staticmethod
    def _path_score(url: str) -> int:
        parsed = urlsplit(url)
        text = f"{parsed.path} {parsed.query}".lower()
        score = sum(2 for token in _STRONG_PATH_TOKENS if token in text)
        if parsed.path in {"", "/"}:
            score += 1
        if parsed.query:
            score -= 1
        if parsed.path.lower().endswith(_SKIP_SUFFIXES):
            score -= 100
        return score

    @staticmethod
    def _tie(seed: int, url: str) -> int:
        digest = hashlib.sha256(
            f"{int(seed)}\0{url}".encode("utf-8")
        ).digest()
        return int.from_bytes(digest[:8], "big")

    @staticmethod
    def _extract_links(base_url: str, payload: bytes) -> list[str]:
        tree = HTMLParser(payload)
        links: set[str] = set()
        for node in tree.css("a[href]"):
            href = node.attributes.get("href", "").strip()
            if not href:
                continue
            try:
                url = canonical_http_url(urljoin(base_url, href))
            except ValueError:
                continue
            if urlsplit(url).path.lower().endswith(_SKIP_SUFFIXES):
                continue
            links.add(url)
        return sorted(links)

    async def crawl(
        self,
        start_urls: list[str] | tuple[str, ...],
        *,
        seed: int,
    ) -> None:
        gate = DistributedProviderGate(
            self.coordinator,
            self.keeper,
            self.provider,
            throttle_floor_seconds=1.0,
        )
        transport = build_authority_transport(
            acquire=gate.acquire,
            report=gate.report,
            max_connections=4,
            max_keepalive_connections=2,
            keepalive_expiry_seconds=20.0,
            inner=self.transport,
        )
        frontier: list[tuple[int, int, int, str]] = []
        seen_urls: set[str] = set()
        seen_hosts: set[str] = set()
        historical_hosts: set[str] = set()
        pages = 0

        for raw in start_urls:
            try:
                url = canonical_http_url(raw)
            except ValueError:
                continue
            heapq.heappush(
                frontier,
                (
                    -self._path_score(url),
                    self._tie(seed, url),
                    0,
                    url,
                ),
            )

        async with httpx.AsyncClient(
            transport=transport,
            timeout=httpx.Timeout(self.limits.timeout_seconds),
            follow_redirects=True,
            trust_env=False,
            headers={
                "User-Agent": (
                    "Creeper-Fabric/0.1 historical-crawler "
                    "(research; https://github.com/Qesire/Creeper)"
                ),
                "Accept": "text/html,application/xhtml+xml;q=0.9",
                "Accept-Encoding": "gzip, deflate",
            },
        ) as client:
            while frontier and pages < self.limits.max_pages:
                self.keeper.assert_owned()
                _neg_score, _tie, depth, url = heapq.heappop(frontier)
                if url in seen_urls or depth > self.limits.max_depth:
                    continue
                seen_urls.add(url)

                host = normalize_official(urlsplit(url).hostname or "")
                if host is None:
                    continue
                if host not in seen_hosts:
                    if len(seen_hosts) >= self.limits.max_hosts:
                        continue
                    outcome = await self.resolver.consume(host)
                    seen_hosts.add(host)
                    if outcome.historical:
                        historical_hosts.add(host)

                try:
                    async with client.stream("GET", url) as response:
                        status = int(response.status_code)
                        if status >= 500:
                            continue
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
                        }:
                            continue
                        raw_length = response.headers.get("Content-Length")
                        if raw_length is not None:
                            try:
                                declared = int(raw_length)
                            except ValueError:
                                declared = -1
                            if declared > self.limits.max_response_bytes:
                                continue

                        chunks: list[bytes] = []
                        size = 0
                        async for chunk in response.aiter_bytes():
                            self.keeper.assert_owned()
                            if size + len(chunk) > self.limits.max_response_bytes:
                                break
                            chunks.append(chunk)
                            size += len(chunk)
                        payload = b"".join(chunks)
                        final_url = canonical_http_url(str(response.url))
                except (httpx.TimeoutException, httpx.TransportError):
                    continue

                pages += 1
                links = self._extract_links(final_url, payload)
                current_host = normalize_official(
                    urlsplit(final_url).hostname or ""
                )
                for child in links[: self.limits.max_links_per_page]:
                    child_host = normalize_official(urlsplit(child).hostname or "")
                    if child_host is None:
                        continue

                    historical = child_host in historical_hosts
                    if child_host not in seen_hosts:
                        if len(seen_hosts) >= self.limits.max_hosts:
                            continue
                        outcome = await self.resolver.consume(child_host)
                        seen_hosts.add(child_host)
                        historical = outcome.historical
                        if historical:
                            historical_hosts.add(child_host)

                    if depth >= self.limits.max_depth:
                        continue
                    score = self._path_score(child)
                    same_host = child_host == current_host
                    current_historical = current_host in historical_hosts
                    # Continue within a site only after the current host is
                    # proven target-era, unless the URL itself looks like a
                    # strong historical topology hub. External continuation
                    # likewise requires target-era evidence or strong topology.
                    if not (
                        historical
                        or (same_host and current_historical)
                        or score >= 4
                    ):
                        continue
                    if child in seen_urls:
                        continue
                    priority = score + (4 if historical else 0) + (
                        2 if same_host and current_historical else 0
                    )
                    heapq.heappush(
                        frontier,
                        (
                            -priority,
                            self._tie(seed, child),
                            depth + 1,
                            child,
                        ),
                    )


class HistoricalCrawlerProducer:
    """Explore one historical root and export only novel HY evidence."""

    EOF_CURSOR = "EOF"

    def __init__(
        self,
        configs: tuple[CDXProviderConfig, ...],
        *,
        limits: ExplorationLimits | None = None,
        web_transport: httpx.AsyncBaseTransport | None = None,
        cdx_transports: dict[str, httpx.AsyncBaseTransport] | None = None,
    ) -> None:
        if not configs:
            raise ValueError("historical crawler requires CDX providers")
        self.configs = configs
        self.limits = limits or ExplorationLimits()
        self.web_transport = web_transport
        self.cdx_transports = dict(cdx_transports or {})

    async def __call__(
        self,
        lease: TaskLease,
        coordinator: CoordinatorClient,
        keeper: LeaseKeeper,
    ) -> None:
        if lease.work.task_class is not TaskClass.SOURCE_PAGE:
            raise ValueError("HistoricalCrawlerProducer requires SOURCE_PAGE")
        if lease.cursor == self.EOF_CURSOR:
            return
        coverage = dict(lease.work.coverage)
        root_url = canonical_http_url(
            str(coverage.get("url", lease.work.input_identity))
        )
        seed = int(coverage.get("seed", 0))

        async with RemoteHostnameResolver(
            self.configs,
            coordinator,
            keeper,
            transports=self.cdx_transports,
        ) as resolver:
            engine = HistoricalCrawlerEngine(
                resolver,
                coordinator,
                keeper,
                limits=self.limits,
                transport=self.web_transport,
            )
            await engine.crawl([root_url], seed=seed)

        # Raw frontier/hostnames never cross the worker boundary.
        await coordinator.commit_batch(
            keeper.lease,
            sequence_no=lease.next_sequence_no,
            results=[],
            cursor_after=self.EOF_CURSOR,
        )


class SeededExplorationProducer:
    """Search with a frozen campaign, then resolve/crawl results locally."""

    EOF_CURSOR = "EOF"

    def __init__(
        self,
        configs: tuple[CDXProviderConfig, ...],
        *,
        exploration_limits: ExplorationLimits | None = None,
        search_limits: SeededExplorationLimits | None = None,
        search_transport: httpx.AsyncBaseTransport | None = None,
        web_transport: httpx.AsyncBaseTransport | None = None,
        cdx_transports: dict[str, httpx.AsyncBaseTransport] | None = None,
    ) -> None:
        if not configs:
            raise ValueError("seeded exploration requires CDX providers")
        self.configs = configs
        self.exploration_limits = exploration_limits or ExplorationLimits()
        self.search_limits = search_limits or SeededExplorationLimits()
        self.search_transport = search_transport
        self.web_transport = web_transport
        self.cdx_transports = dict(cdx_transports or {})

    @staticmethod
    def _query_url(endpoint: str, query_param: str, query: str) -> str:
        parsed = urlsplit(endpoint)
        params = parse_qs(parsed.query, keep_blank_values=True)
        params[query_param] = [query]
        return urlunsplit(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                urlencode(params, doseq=True),
                "",
            )
        )

    @staticmethod
    def _unwrap(base_url: str, href: str) -> str | None:
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

    async def __call__(
        self,
        lease: TaskLease,
        coordinator: CoordinatorClient,
        keeper: LeaseKeeper,
    ) -> None:
        if lease.work.task_class is not TaskClass.SEARCH_SLICE:
            raise ValueError("SeededExplorationProducer requires SEARCH_SLICE")
        if lease.cursor == self.EOF_CURSOR:
            return

        coverage = dict(lease.work.coverage)
        raw_campaign = coverage.get("campaign")
        if not isinstance(raw_campaign, dict):
            raise ValueError("seeded exploration requires frozen campaign")
        campaign = SearchCampaign.from_mapping(raw_campaign)
        if campaign.campaign_id != str(coverage.get("campaign_id", "")):
            raise ValueError("seeded exploration campaign identity mismatch")

        seed = int(coverage.get("seed", -1))
        slot_start = int(coverage.get("slot_start", -1))
        slot_count = int(coverage.get("slot_count", 0))
        if (
            seed < 0
            or slot_start < 0
            or not 1 <= slot_count <= self.search_limits.max_queries
        ):
            raise ValueError("invalid seeded exploration slice")

        endpoint = canonical_http_url(str(coverage.get("search_endpoint", "")))
        query_param = str(coverage.get("query_param", "q")).strip()
        search_provider = str(coverage.get("search_provider", "web_search")).strip()
        endpoint_host = normalize_official(urlsplit(endpoint).hostname or "")
        gate = DistributedProviderGate(
            coordinator,
            keeper,
            search_provider,
            throttle_floor_seconds=1.0,
        )
        transport = build_authority_transport(
            acquire=gate.acquire,
            report=gate.report,
            max_connections=2,
            max_keepalive_connections=1,
            keepalive_expiry_seconds=20.0,
            inner=self.search_transport,
        )

        roots: list[str] = []
        queries = campaign.render_slice(
            seed=seed,
            slot_start=slot_start,
            slot_count=slot_count,
        )
        async with httpx.AsyncClient(
            transport=transport,
            timeout=httpx.Timeout(20.0),
            follow_redirects=True,
            trust_env=False,
            headers={
                "User-Agent": (
                    "Creeper-Fabric/0.1 seeded-exploration "
                    "(research; https://github.com/Qesire/Creeper)"
                ),
                "Accept": "text/html,application/xhtml+xml;q=0.9",
                "Accept-Encoding": "gzip, deflate",
            },
        ) as client:
            for query in queries:
                keeper.assert_owned()
                request_url = self._query_url(endpoint, query_param, query)
                async with client.stream("GET", request_url) as response:
                    if int(response.status_code) >= 500:
                        raise RuntimeError(
                            f"search provider HTTP {response.status_code}"
                        )
                    if int(response.status_code) >= 400:
                        continue
                    chunks: list[bytes] = []
                    size = 0
                    async for chunk in response.aiter_bytes():
                        if (
                            size + len(chunk)
                            > self.search_limits.max_search_response_bytes
                        ):
                            break
                        chunks.append(chunk)
                        size += len(chunk)
                    payload = b"".join(chunks)
                    final_url = canonical_http_url(str(response.url))

                tree = HTMLParser(payload)
                query_roots: list[str] = []
                for node in tree.css("a[href]"):
                    href = node.attributes.get("href", "").strip()
                    if not href:
                        continue
                    candidate = self._unwrap(final_url, href)
                    if candidate is None:
                        continue
                    host = normalize_official(urlsplit(candidate).hostname or "")
                    if host is None or host == endpoint_host:
                        continue
                    query_roots.append(candidate)
                roots.extend(
                    sorted(set(query_roots))[
                        : self.search_limits.max_results_per_query
                    ]
                )

        roots = list(dict.fromkeys(roots))
        async with RemoteHostnameResolver(
            self.configs,
            coordinator,
            keeper,
            transports=self.cdx_transports,
        ) as resolver:
            engine = HistoricalCrawlerEngine(
                resolver,
                coordinator,
                keeper,
                limits=self.exploration_limits,
                transport=self.web_transport,
            )
            await engine.crawl(roots, seed=seed)

        await coordinator.commit_batch(
            keeper.lease,
            sequence_no=lease.next_sequence_no,
            results=[],
            cursor_after=self.EOF_CURSOR,
        )
