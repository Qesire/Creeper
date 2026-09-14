"""Archive-It research root.

The adapter intentionally exposes collection/seed metadata only as research hits.  It
never promotes collection dates, seed URLs, or Internet Archive provenance into
annual evidence.
"""
from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin

from .base import (
    RootCapabilityReport,
    SearchCheckpoint,
    SearchHit,
    SearchPage,
    is_retryable,
    response_json,
    retry_delay_seconds,
)

DEFAULT_API_ENDPOINT = "https://partner.archive-it.org/api/collection"
DEFAULT_EXPLORE_ENDPOINT = "https://archive-it.org/explore"
_TARGET_YEARS = frozenset(range(1996, 2002))
_YEAR_RE = re.compile(r"\b(?:19\d{2}|20\d{2})\b")
_COLLECTION_ID_RE = re.compile(r"/collections?/(\d+)(?:/|$)", re.I)


class _ExploreParser(HTMLParser):
    """Extract bounded collection links from Archive-It public Explore HTML."""

    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.rows: list[dict[str, str]] = []
        self._anchor_href: str | None = None
        self._anchor_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        href = dict(attrs).get("href") or ""
        if _COLLECTION_ID_RE.search(href):
            self._anchor_href = href
            self._anchor_text = []

    def handle_data(self, data: str) -> None:
        if self._anchor_href is not None:
            self._anchor_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or self._anchor_href is None:
            return
        match = _COLLECTION_ID_RE.search(self._anchor_href)
        title = " ".join(" ".join(self._anchor_text).split())
        if match and title:
            self.rows.append(
                {
                    "collection_id": match.group(1),
                    "title": title,
                    "url": urljoin(self.base_url, self._anchor_href),
                }
            )
        self._anchor_href = None
        self._anchor_text = []


class ArchiveItAdapter:
    root_id = "archiveit"
    # Original V7.1 safety envelope: one request in flight at about 0.1 RPS.
    max_inflight = 1
    requests_per_second = 0.1

    def __init__(
        self,
        *,
        transport: Any,
        api_endpoint: str = DEFAULT_API_ENDPOINT,
        explore_endpoint: str = DEFAULT_EXPLORE_ENDPOINT,
        max_limit: int = 100,
    ) -> None:
        self.transport = transport
        self.api_endpoint = api_endpoint.rstrip("/")
        self.explore_endpoint = explore_endpoint
        self.max_limit = max(1, int(max_limit))

    async def probe_capabilities(self) -> RootCapabilityReport:
        """Probe API first; public Explore remains a legitimate fallback surface."""
        try:
            response = await self.transport(
                self.api_endpoint,
                {"limit": 1, "offset": 0},
                {"Accept": "application/json"},
            )
        except Exception as exc:
            return RootCapabilityReport(
                self.root_id,
                True,
                ("explore_fallback",),
                reason=f"API_UNAVAILABLE:{exc}",
            )
        status = int(getattr(response, "status_code", 200))
        if 200 <= status < 300:
            return RootCapabilityReport(
                self.root_id,
                True,
                ("collection_api", "seed_api", "bounded_pagination", "explore_fallback"),
                status_code=status,
            )
        if is_retryable(response):
            return RootCapabilityReport(
                self.root_id,
                True,
                ("explore_fallback",),
                reason="API_RETRYABLE",
                status_code=status,
            )
        return RootCapabilityReport(
            self.root_id,
            True,
            ("explore_fallback",),
            reason="API_UNAVAILABLE",
            status_code=status,
        )

    async def search(self, query: Any, checkpoint: SearchCheckpoint | None) -> SearchPage:
        cp = checkpoint or SearchCheckpoint(start=0, query_variant="api")
        if cp.query_variant == "explore":
            return await self._search_explore(query, cp)
        return await self._search_api(query, cp)

    async def _search_api(self, query: Any, cp: SearchCheckpoint) -> SearchPage:
        limit = min(self.max_limit, max(1, int(getattr(query, "page_size", self.max_limit))))
        filters = dict(getattr(query, "native_filters", {}) or {})
        params: dict[str, Any] = {
            "q": getattr(query, "query_text", ""),
            "limit": limit,
            "offset": max(0, int(cp.start)),
        }
        # Archive-It accepts attribute filters plus sort/pluck.  Preserve the
        # bounded native program, but never allow the caller to replace our
        # limit/offset with an unbounded `limit=-1`.
        resource = str(filters.pop("resource", "collection") or "collection").strip().lower()
        if resource not in {"collection", "seed"}:
            resource = "collection"
        for key, value in filters.items():
            if key in {"limit", "offset"} or value in (None, ""):
                continue
            params[key] = value
        endpoint = _resource_endpoint(self.api_endpoint, resource)
        response = await self.transport(
            cp.next_url or endpoint,
            {} if cp.next_url else params,
            {"Accept": "application/json"},
        )
        status = int(getattr(response, "status_code", 200))
        if is_retryable(response):
            return SearchPage(
                next_checkpoint=cp,
                terminal=False,
                retry_after=retry_delay_seconds(response),
            )
        if not 200 <= status < 300:
            # A missing/private API is not root failure: deterministically switch to
            # the public Explore surface on the next scheduler step.
            return SearchPage(
                next_checkpoint=SearchCheckpoint(page=1, query_variant="explore"),
                terminal=False,
            )

        payload = response_json(response)
        rows = _api_rows(payload)
        hits = tuple(_api_hit(self.root_id, getattr(query, "query_id", ""), row) for row in rows)
        hits = tuple(hit for hit in hits if hit is not None)

        next_url = _next_url(payload)
        if next_url:
            next_cp = SearchCheckpoint(next_url=next_url, start=cp.start + len(rows), query_variant="api")
            terminal = False
        else:
            total = _safe_int(payload.get("total") or payload.get("count") or payload.get("total_count"))
            next_start = cp.start + len(rows)
            more = bool(rows) and len(rows) >= limit and (total is None or next_start < total)
            next_cp = SearchCheckpoint(start=next_start, query_variant="api") if more else None
            terminal = not more
        return SearchPage(
            hits=hits,
            next_checkpoint=next_cp,
            terminal=terminal,
            bytes_read=len(str(payload)),
        )

    async def _search_explore(self, query: Any, cp: SearchCheckpoint) -> SearchPage:
        params = {"q": getattr(query, "query_text", "")}
        if cp.page > 1:
            params["page"] = cp.page
        response = await self.transport(self.explore_endpoint, params, {"Accept": "text/html"})
        status = int(getattr(response, "status_code", 200))
        if is_retryable(response):
            return SearchPage(
                next_checkpoint=cp,
                terminal=False,
                retry_after=retry_delay_seconds(response),
            )
        if not 200 <= status < 300:
            return SearchPage(terminal=True)
        text = _response_text(response)
        parser = _ExploreParser(self.explore_endpoint)
        parser.feed(text)
        seen: set[str] = set()
        hits: list[SearchHit] = []
        for row in parser.rows:
            cid = row["collection_id"]
            if cid in seen:
                continue
            seen.add(cid)
            title = row["title"]
            target_signal = _target_period_signal(title)
            hits.append(
                SearchHit(
                    self.root_id,
                    getattr(query, "query_id", ""),
                    cid,
                    row["url"],
                    "COLLECTION",
                    title,
                    metadata={
                        "source_surface": "PUBLIC_EXPLORE",
                        "target_period_signal": target_signal,
                        "ia_derived_overlap_penalty": True,
                        "annual_evidence_authority": False,
                    },
                )
            )
        # Explore pages do not expose a universally stable pagination contract.
        # Keep pagination bounded and only continue when the fixture/page explicitly
        # advertises a rel=next link.
        next_url = _html_next_url(text, self.explore_endpoint)
        next_cp = SearchCheckpoint(next_url=next_url, page=cp.page + 1, query_variant="explore") if next_url else None
        return SearchPage(
            hits=tuple(hits),
            next_checkpoint=next_cp,
            terminal=next_cp is None,
            bytes_read=len(text.encode("utf-8", errors="ignore")),
        )

    async def resolve(self, node: Any) -> tuple[Any, ...]:
        # Collections and seeds remain research nodes/pivots.  Resolution into a
        # concrete derivative/artifact is owned by the research resolver.
        return (node,)


def _resource_endpoint(endpoint: str, resource: str) -> str:
    base = endpoint.rstrip("/")
    tail = base.rsplit("/", 1)[-1].lower()
    if tail in {"collection", "seed"}:
        return base.rsplit("/", 1)[0] + "/" + resource
    if tail == "api":
        return base + "/" + resource
    return base


def _api_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("collections", "results", "objects", "data", "seeds"):
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
        if isinstance(value, dict):
            for nested in ("results", "items", "objects"):
                rows = value.get(nested)
                if isinstance(rows, list):
                    return [row for row in rows if isinstance(row, dict)]
    return []


def _api_hit(root_id: str, query_id: str, row: dict[str, Any]) -> SearchHit | None:
    raw_id = row.get("id") or row.get("collection_id") or row.get("collectionId") or row.get("identifier")
    if raw_id is None:
        return None
    native_id = str(raw_id)
    title = str(row.get("name") or row.get("title") or row.get("description") or native_id)
    url = str(row.get("url") or row.get("public_url") or f"https://archive-it.org/collections/{native_id}")
    kind = "SEED" if any(k in row for k in ("seed", "seed_url", "seedUrl")) else "COLLECTION"
    metadata = {
        "organization": row.get("organization") or row.get("org"),
        "archived_since": row.get("archived_since") or row.get("archivedSince"),
        "subjects": row.get("subjects") or row.get("subject"),
        "target_period_signal": _target_period_signal(" ".join(map(str, row.values()))),
        "ia_derived_overlap_penalty": True,
        "annual_evidence_authority": False,
        "raw": row,
    }
    return SearchHit(root_id, query_id, native_id, url, kind, title, str(row.get("description") or ""), metadata)


def _target_period_signal(text: str) -> bool:
    years = {_safe_int(match.group(0)) for match in _YEAR_RE.finditer(str(text))}
    return any(year in _TARGET_YEARS for year in years if year is not None)


def _next_url(payload: dict[str, Any]) -> str | None:
    for value in (payload.get("next"), (payload.get("links") or {}).get("next") if isinstance(payload.get("links"), dict) else None):
        if isinstance(value, str) and value:
            return value
    return None


def _html_next_url(text: str, base: str) -> str | None:
    patterns = (
        r'<a(?=[^>]*\brel=["\']next["\'])(?=[^>]*\bhref=["\']([^"\']+)["\'])[^>]*>',
        r'<link(?=[^>]*\brel=["\']next["\'])(?=[^>]*\bhref=["\']([^"\']+)["\'])[^>]*>',
    )
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return urljoin(base, match.group(1))
    return None


def _response_text(response: Any) -> str:
    value = getattr(response, "text", "")
    if callable(value):
        value = value()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value or "")


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
