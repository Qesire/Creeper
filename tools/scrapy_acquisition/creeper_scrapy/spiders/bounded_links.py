"""Bounded same-site link enumerator backed by Scrapy's native scheduler."""

from __future__ import annotations

from pathlib import PurePosixPath
from urllib.parse import urldefrag, urlsplit

import scrapy
from scrapy.linkextractors import LinkExtractor


_FOLLOW_SUFFIXES = frozenset(
    {
        "",
        ".htm",
        ".html",
        ".shtml",
        ".xhtml",
        ".php",
        ".asp",
        ".aspx",
        ".jsp",
        ".cgi",
        ".pl",
    }
)


def _default_port(scheme: str) -> int | None:
    return {"http": 80, "https": 443}.get(scheme.lower())


def _host(value: str | None) -> str | None:
    if value is None:
        return None
    try:
        return value.encode("idna").decode("ascii").lower()
    except UnicodeError:
        return None


class BoundedLinksSpider(scrapy.Spider):
    """Enumerate links while following only bounded same-site HTML-like paths.

    Every HTTP(S) link is emitted as a discovery record, including external and
    dataset-looking links. Only same-site HTML-like URLs are scheduled for
    further crawling. Request deduplication, disk persistence and priority queue
    semantics are intentionally left to Scrapy.
    """

    name = "bounded_links"

    def __init__(
        self,
        *,
        start_url: str,
        source_key: str,
        follow_query: str = "false",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        start = urlsplit(start_url)
        if start.scheme.lower() not in {"http", "https"} or start.hostname is None:
            raise ValueError("start_url must be an absolute HTTP(S) URL")
        if not source_key.strip():
            raise ValueError("source_key is required")
        allowed_host = _host(start.hostname)
        if allowed_host is None:
            raise ValueError("invalid start_url hostname")
        try:
            explicit_port = start.port
        except ValueError as exc:
            raise ValueError("invalid start_url port") from exc
        if explicit_port == _default_port(start.scheme):
            explicit_port = None

        self.start_urls = [start_url]
        self.source_key = source_key
        self.allowed_host = allowed_host
        self.allowed_explicit_port = explicit_port
        self.follow_query = str(follow_query).strip().lower() in {"1", "true", "yes", "on"}
        # Empty deny_extensions is deliberate: archives/datasets should still be
        # emitted as source candidates even though they are not followed.
        self.link_extractor = LinkExtractor(unique=True, deny_extensions=())

    async def start(self):
        """Emit the seed once per JOBDIR lifecycle.

        Scrapy's default ``Spider.start()`` marks start requests ``dont_filter``.
        For a resumable bounded scout that would refetch the root on every
        process restart.  SpiderState is a built-in JOBDIR extension, so use it
        as the durable seed marker and let the seed pass through the native
        scheduler/dupefilter like every other request.
        """
        state = getattr(self, "state", None)
        if state is not None and state.get("seed_emitted"):
            return
        if state is not None:
            state["seed_emitted"] = True
        yield scrapy.Request(self.start_urls[0], callback=self.parse, dont_filter=False)

    def _same_site(self, url: str) -> bool:
        parsed = urlsplit(url)
        if parsed.scheme.lower() not in {"http", "https"}:
            return False
        if _host(parsed.hostname) != self.allowed_host:
            return False
        try:
            port = parsed.port
        except ValueError:
            return False
        if self.allowed_explicit_port is not None:
            return port == self.allowed_explicit_port
        return port is None or port == _default_port(parsed.scheme)

    def _can_follow(self, url: str) -> bool:
        parsed = urlsplit(url)
        if not self._same_site(url):
            return False
        if parsed.query and not self.follow_query:
            return False
        suffix = PurePosixPath(parsed.path).suffix.lower()
        return suffix in _FOLLOW_SUFFIXES

    def parse(self, response: scrapy.http.Response):
        content_type = response.headers.get(b"Content-Type", b"").decode(
            "latin-1", errors="ignore"
        ).lower()
        if content_type and "html" not in content_type and "xhtml" not in content_type:
            return

        depth = int(response.meta.get("depth", 0))
        for link in self.link_extractor.extract_links(response):
            discovered_url, _fragment = urldefrag(link.url)
            parsed = urlsplit(discovered_url)
            if parsed.scheme.lower() not in {"http", "https"} or parsed.hostname is None:
                continue
            same_site = self._same_site(discovered_url)
            anchor_text = " ".join((link.text or "").split())[:512]
            yield {
                "record_type": "LINK_DISCOVERY",
                "source_key": self.source_key,
                "page_url": response.url,
                "discovered_url": discovered_url,
                "anchor_text": anchor_text,
                "depth": depth,
                "same_site": same_site,
            }
            if self._can_follow(discovered_url) and not getattr(link, "nofollow", False):
                yield response.follow(discovered_url, callback=self.parse)
