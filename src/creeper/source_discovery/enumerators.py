"""Bounded deterministic enumerators used by the V7 region executor."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass
from html.parser import HTMLParser
from itertools import product
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit


@dataclass(frozen=True)
class EnumeratedBatch:
    artifacts: tuple[str, ...]
    page: int | None = None
    cursor: object | None = None
    next_cursor: object | None = None
    bytes_read: int = 0
    requests: int = 1
    terminal: bool = False


class _LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self.links.append(href)


def _select_path(value: Any, selector: str | None) -> Any:
    if not selector:
        return value
    current = value
    for part in selector.split("."):
        if isinstance(current, Mapping):
            current = current.get(part)
        elif isinstance(current, (list, tuple)) and part.isdigit():
            current = current[int(part)]
        else:
            return None
    return current


def _render_url(template: str, value: Any, variable: str = "page") -> str:
    if "{" in template:
        return template.format(**{variable: value, "page": value, "cursor": value})
    parts = urlsplit(template)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query[variable] = str(value)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


class StaticListEnumerator:
    async def enumerate(self, *, config: Mapping[str, Any], start: int = 0, **_: Any) -> AsyncIterator[EnumeratedBatch]:
        urls = tuple(config.get("urls", ()))
        if start < len(urls):
            yield EnumeratedBatch(artifacts=urls[start:], page=start, terminal=True)


class FilenamePatternEnumerator:
    async def enumerate(self, *, config: Mapping[str, Any], start: int = 0, **_: Any) -> AsyncIterator[EnumeratedBatch]:
        template = str(config["template"])
        dimensions = {name: tuple(values) for name, values in config.get("dimensions", {}).items()}
        values = tuple(
            template.format(**dict(zip(dimensions, combo, strict=True)))
            for combo in product(*(dimensions[name] for name in dimensions))
        )
        if start < len(values):
            yield EnumeratedBatch(artifacts=values[start:], page=start, terminal=True)


class IntegerPaginationEnumerator:
    async def enumerate(self, *, config: Mapping[str, Any], start: int = 0, max_pages: int = 0, fetcher: Callable[..., Any] | None = None, **_: Any) -> AsyncIterator[EnumeratedBatch]:
        first = int(config.get("start", 1)) + start * int(config.get("step", 1))
        stop = int(config["max_page"])
        terminal_rule = str(config.get("terminal_condition", "EMPTY"))
        page = first
        while page <= stop and (not max_pages or page - first < max_pages):
            if fetcher is None:
                yield EnumeratedBatch(artifacts=(_render_url(str(config["url_template"]), page),), page=page, terminal=page == stop)
            else:
                raw = await fetcher(_render_url(str(config["url_template"]), page), page)
                batch = _coerce_batch(raw, page=page)
                terminal = batch.terminal or _terminal(batch, terminal_rule, stop)
                yield EnumeratedBatch(batch.artifacts, page, None, None, batch.bytes_read, batch.requests, terminal)
                if terminal:
                    return
            page += int(config.get("step", 1))


class CursorApiEnumerator:
    async def enumerate(self, *, config: Mapping[str, Any], cursor: object | None = None, start: int = 0, max_pages: int = 0, fetcher: Callable[..., Any] | None = None, **_: Any) -> AsyncIterator[EnumeratedBatch]:
        if fetcher is None:
            raise ValueError("CURSOR_API requires an injected fetcher")
        current = cursor
        page = start
        while not max_pages or page - start < max_pages:
            params = dict(config.get("params", {}))
            if current is not None:
                params[str(config.get("cursor_param", "cursor"))] = current
            raw = await fetcher(str(config["endpoint"]), params, int(config.get("max_bytes", 0)))
            payload = raw
            if isinstance(raw, (bytes, str)):
                payload = json.loads(raw)
            records = _select_path(payload, config.get("record_selector")) or ()
            artifacts = tuple(
                str(_select_path(record, config.get("artifact_field", "url")))
                for record in records
                if _select_path(record, config.get("artifact_field", "url"))
            )
            next_cursor = _select_path(payload, config.get("next_cursor_selector"))
            yield EnumeratedBatch(artifacts, page, current, next_cursor, _bytes(raw), 1, next_cursor is None)
            if next_cursor is None:
                return
            current = next_cursor
            page += 1


class HtmlCatalogEnumerator:
    async def enumerate(self, *, config: Mapping[str, Any], start: int = 0, max_pages: int = 1, fetcher: Callable[..., Any] | None = None, **_: Any) -> AsyncIterator[EnumeratedBatch]:
        if fetcher is None:
            raise ValueError("HTML_CATALOG requires an injected fetcher")
        root = str(config["root"])
        raw = await fetcher(root, int(config.get("max_bytes", 0)))
        body = raw.get("body", b"") if isinstance(raw, Mapping) else raw
        if isinstance(body, str):
            body = body.encode()
        parser = _LinkParser()
        parser.feed(body.decode("utf-8", errors="replace"))
        base_origin = urlsplit(root).netloc.lower()
        artifacts: list[str] = []
        for href in parser.links:
            url = urljoin(root, href)
            parsed = urlsplit(url)
            if parsed.scheme not in {"http", "https"} or parsed.netloc.lower() != base_origin:
                continue
            if any(token in href.lower() for token in config.get("artifact_suffixes", (".cdx", ".cdxj", ".warc", ".gz"))):
                artifacts.append(urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, "")))
        yield EnumeratedBatch(tuple(dict.fromkeys(artifacts)), 0, None, None, _bytes(body), 1, True)


def _bytes(raw: Any) -> int:
    if isinstance(raw, Mapping) and "bytes" in raw:
        return int(raw["bytes"])
    if isinstance(raw, (bytes, bytearray, str)):
        return len(raw)
    return 0


def _coerce_batch(raw: Any, *, page: int) -> EnumeratedBatch:
    if isinstance(raw, EnumeratedBatch):
        return raw
    if isinstance(raw, Mapping):
        items = raw.get("items", raw.get("artifacts", ()))
        return EnumeratedBatch(tuple(str(item) for item in items), page, None, raw.get("next"), _bytes(raw), int(raw.get("requests", 1)), bool(raw.get("terminal", False)))
    if isinstance(raw, (list, tuple)):
        return EnumeratedBatch(tuple(str(item) for item in raw), page)
    raise TypeError("fetcher must return an EnumeratedBatch, mapping, or sequence")


def _terminal(batch: EnumeratedBatch, rule: str, max_page: int | None = None) -> bool:
    rule = rule.upper()
    if rule in {"EMPTY", "EMPTY_PAGE"}:
        return not batch.artifacts
    if rule in {"LAST", "MAX_PAGE"}:
        return max_page is not None and batch.page is not None and batch.page >= max_page
    return False
