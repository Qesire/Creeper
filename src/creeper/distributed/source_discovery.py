"""Bounded deterministic web source discovery for the Fabric derivative."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urljoin, urlsplit

import httpx
from selectolax.parser import HTMLParser

from creeper.distributed.coordinator_client import CoordinatorClient
from creeper.distributed.http_transport import build_authority_transport
from creeper.distributed.lease_keeper import LeaseKeeper
from creeper.distributed.models import TaskClass, TaskLease
from creeper.distributed.provider_gate import DistributedProviderGate
from creeper.distributed.urlcanon import canonical_http_url


class SourceDiscoveryError(RuntimeError):
    """One source page could not be safely interpreted."""


@dataclass(frozen=True)
class SourceDiscoveryLimits:
    max_bytes: int = 2 * 1024 * 1024
    max_links: int = 512
    timeout_seconds: float = 20.0

    def __post_init__(self) -> None:
        if self.max_bytes < 1024 or self.max_links < 1 or self.timeout_seconds <= 0:
            raise ValueError("invalid source discovery limits")


def _classify_candidate(url: str) -> tuple[str, str]:
    path = urlsplit(url).path.lower()
    if path.endswith((".cdxj", ".cdxj.gz")):
        return "bulk_artifact", "cdxj"
    if path.endswith((".cdx", ".cdx.gz")):
        return "bulk_artifact", "cdx"
    if path.endswith((".warc", ".warc.gz", ".arc", ".arc.gz")):
        return "bulk_artifact", "warc_arc"
    if path.endswith((".jsonl", ".jsonl.gz")):
        return "bulk_artifact", "jsonl"
    if path.endswith((".csv", ".csv.gz", ".tsv", ".tsv.gz")):
        return "bulk_artifact", "delimited"
    return "source_page", ""


class SourceDiscoveryProducer:
    """Fetch one bounded page and emit candidate source URLs only."""

    EOF_CURSOR = "EOF"
    PROVIDER = "web_discovery"

    def __init__(
        self,
        *,
        limits: SourceDiscoveryLimits | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.limits = limits or SourceDiscoveryLimits()
        self.transport = transport

    async def __call__(
        self,
        lease: TaskLease,
        coordinator: CoordinatorClient,
        keeper: LeaseKeeper,
    ) -> None:
        if lease.work.task_class is not TaskClass.SOURCE_PAGE:
            raise ValueError("SourceDiscoveryProducer requires SOURCE_PAGE")
        if lease.cursor == self.EOF_CURSOR:
            return

        coverage = dict(lease.work.coverage)
        raw_url = str(coverage.get("url", lease.work.input_identity))
        page_url = canonical_http_url(raw_url)
        max_bytes = min(
            self.limits.max_bytes,
            int(coverage.get("max_bytes", self.limits.max_bytes)),
        )
        max_links = min(
            self.limits.max_links,
            int(coverage.get("max_links", self.limits.max_links)),
        )
        if max_bytes < 1024 or max_links < 1:
            raise ValueError("invalid source page work limits")

        gate = DistributedProviderGate(
            coordinator,
            keeper,
            self.PROVIDER,
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
        payload = b""
        final_url = page_url
        content_type = ""
        async with httpx.AsyncClient(
            transport=transport,
            timeout=httpx.Timeout(self.limits.timeout_seconds),
            follow_redirects=True,
            trust_env=False,
            headers={
                "User-Agent": (
                    "Creeper-Fabric/0.1 source-discovery "
                    "(research; https://github.com/Qesire/Creeper)"
                ),
                "Accept": "text/html,application/xhtml+xml;q=0.9,text/plain;q=0.5",
                "Accept-Encoding": "gzip, deflate",
            },
        ) as client:
            keeper.assert_owned()
            async with client.stream("GET", page_url) as response:
                status = int(response.status_code)
                if status >= 500:
                    raise SourceDiscoveryError(
                        f"source discovery HTTP {status}"
                    )
                if status >= 400:
                    await coordinator.commit_batch(
                        keeper.lease,
                        sequence_no=lease.next_sequence_no,
                        results=[],
                        cursor_after=self.EOF_CURSOR,
                    )
                    return

                final_url = canonical_http_url(str(response.url))
                content_type = response.headers.get(
                    "Content-Type",
                    "",
                ).split(";", 1)[0].strip().lower()
                if content_type not in {
                    "text/html",
                    "application/xhtml+xml",
                    "text/plain",
                    "",
                }:
                    await coordinator.commit_batch(
                        keeper.lease,
                        sequence_no=lease.next_sequence_no,
                        results=[],
                        cursor_after=self.EOF_CURSOR,
                    )
                    return

                raw_length = response.headers.get("Content-Length")
                if raw_length is not None:
                    try:
                        declared_length = int(raw_length)
                    except ValueError:
                        declared_length = -1
                    if declared_length > max_bytes:
                        await coordinator.commit_batch(
                            keeper.lease,
                            sequence_no=lease.next_sequence_no,
                            results=[],
                            cursor_after=self.EOF_CURSOR,
                        )
                        return

                chunks: list[bytes] = []
                seen_bytes = 0
                async for chunk in response.aiter_bytes():
                    keeper.assert_owned()
                    if seen_bytes + len(chunk) > max_bytes:
                        remaining = max_bytes - seen_bytes
                        if remaining > 0:
                            chunks.append(chunk[:remaining])
                        break
                    chunks.append(chunk)
                    seen_bytes += len(chunk)
                payload = b"".join(chunks)

        if content_type == "text/plain":
            links: list[str] = []
            for raw_line in payload.decode("utf-8", errors="replace").splitlines():
                text = raw_line.strip()
                if not text.startswith(("http://", "https://")):
                    continue
                try:
                    links.append(canonical_http_url(text))
                except ValueError:
                    continue
        else:
            tree = HTMLParser(payload)
            links = []
            for node in tree.css("a[href]"):
                raw_href = node.attributes.get("href", "").strip()
                if not raw_href:
                    continue
                absolute = urljoin(final_url, raw_href)
                try:
                    links.append(canonical_http_url(absolute))
                except ValueError:
                    continue

        unique = sorted(set(links))
        classified = [
            (url, *_classify_candidate(url))
            for url in unique
        ]
        # Prefer likely finite/bulk artifacts before generic navigational pages.
        classified.sort(
            key=lambda row: (
                0 if row[1] == "bulk_artifact" else 1,
                row[0],
            )
        )
        results = [
            {
                "kind": "SOURCE_CANDIDATE",
                "url": url,
                "candidate_type": candidate_type,
                "parser_kind": parser_kind,
                "referrer_url": final_url,
            }
            for url, candidate_type, parser_kind in classified[:max_links]
        ]
        keeper.assert_owned()
        await coordinator.commit_batch(
            keeper.lease,
            sequence_no=lease.next_sequence_no,
            results=results,
            cursor_after=self.EOF_CURSOR,
        )
