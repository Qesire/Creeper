"""Bounded deterministic enumerators used by the region executor."""

from __future__ import annotations

import inspect
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
    next_page: int | None = None
    cursor: object | None = None
    next_cursor: object | None = None
    bytes_read: int = 0
    requests: int = 0
    status: int | None = None
    terminal: bool = False


class _LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
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
    return urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment)
    )


async def _invoke(fetcher: Callable[..., Any], *args: Any) -> Any:
    """Call injected fetchers while preserving compatibility with narrow fakes."""
    try:
        signature = inspect.signature(fetcher)
    except (TypeError, ValueError):
        result = fetcher(*args)
    else:
        params = tuple(signature.parameters.values())
        if any(param.kind is inspect.Parameter.VAR_POSITIONAL for param in params):
            result = fetcher(*args)
        else:
            positional = tuple(
                param
                for param in params
                if param.kind
                in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                )
            )
            result = fetcher(*args[: len(positional)])
    if inspect.isawaitable(result):
        return await result
    return result


class StaticListEnumerator:
    async def enumerate(
        self, *, config: Mapping[str, Any], start: int = 0, **_: Any
    ) -> AsyncIterator[EnumeratedBatch]:
        urls = tuple(str(value) for value in config.get("urls", ()))
        for index in range(start, len(urls)):
            yield EnumeratedBatch(
                artifacts=(urls[index],),
                page=index,
                next_page=index + 1,
                requests=0,
                terminal=index + 1 >= len(urls),
            )


class FilenamePatternEnumerator:
    async def enumerate(
        self, *, config: Mapping[str, Any], start: int = 0, **_: Any
    ) -> AsyncIterator[EnumeratedBatch]:
        template = str(config["template"])
        dimensions = {
            name: tuple(values)
            for name, values in config.get("dimensions", {}).items()
        }
        names = tuple(dimensions)
        combos = product(*(dimensions[name] for name in names))
        for index, combo in enumerate(combos):
            if index < start:
                continue
            url = template.format(**dict(zip(names, combo, strict=True)))
            terminal = index + 1 >= _product_size(dimensions)
            yield EnumeratedBatch(
                artifacts=(url,),
                page=index,
                next_page=index + 1,
                requests=0,
                terminal=terminal,
            )


class IntegerPaginationEnumerator:
    async def enumerate(
        self,
        *,
        config: Mapping[str, Any],
        start: int = 0,
        max_pages: int = 0,
        fetcher: Callable[..., Any] | None = None,
        **_: Any,
    ) -> AsyncIterator[EnumeratedBatch]:
        if fetcher is None:
            raise ValueError("INTEGER_PAGINATION requires an injected fetcher")
        first = int(config.get("start", 1))
        step = int(config.get("step", 1))
        max_page = int(config["max_page"])
        terminal_rule = str(config.get("terminal_condition", "EMPTY")).upper()
        ordinal = start
        while (not max_pages or ordinal < max_pages):
            page = first + ordinal * step
            if page > max_page:
                return
            raw = await _invoke(
                fetcher,
                _render_url(str(config["url_template"]), page),
                page,
                int(config.get("max_bytes", 0)),
            )
            batch = _coerce_batch(raw, page=page)
            terminal = (
                batch.terminal
                or _terminal(batch, terminal_rule, max_page=max_page)
                or page + step > max_page
            )
            yield EnumeratedBatch(
                artifacts=batch.artifacts,
                page=ordinal,
                next_page=ordinal + 1,
                bytes_read=batch.bytes_read,
                requests=max(1, batch.requests),
                status=batch.status,
                terminal=terminal,
            )
            if terminal:
                return
            ordinal += 1


class CursorApiEnumerator:
    async def enumerate(
        self,
        *,
        config: Mapping[str, Any],
        cursor: object | None = None,
        start: int = 0,
        max_pages: int = 0,
        fetcher: Callable[..., Any] | None = None,
        **_: Any,
    ) -> AsyncIterator[EnumeratedBatch]:
        if fetcher is None:
            raise ValueError("CURSOR_API requires an injected fetcher")
        current = cursor
        page = start
        while not max_pages or page < max_pages:
            params = dict(config.get("params", {}))
            if current is not None:
                params[str(config.get("cursor_param", "cursor"))] = current
            raw = await _invoke(
                fetcher,
                str(config["endpoint"]),
                params,
                int(config.get("max_bytes", 0)),
            )
            payload = raw.get("payload", raw) if isinstance(raw, Mapping) else raw
            if isinstance(payload, bytes):
                payload = json.loads(payload.decode("utf-8"))
            elif isinstance(payload, str):
                payload = json.loads(payload)
            records = _select_path(payload, str(config["record_selector"])) or ()
            if isinstance(records, (str, bytes)) or not isinstance(records, (list, tuple)):
                raise TypeError("CURSOR_API record_selector must resolve to a sequence")
            artifact_field = str(config.get("artifact_field", "url"))
            artifacts = tuple(
                str(value)
                for record in records
                if (value := _select_path(record, artifact_field))
            )
            next_cursor = _select_path(payload, str(config["next_cursor_selector"]))
            terminal = next_cursor is None
            yield EnumeratedBatch(
                artifacts=artifacts,
                page=page,
                next_page=page + 1,
                cursor=current,
                next_cursor=next_cursor,
                bytes_read=_bytes(raw),
                requests=1,
                status=_status(raw),
                terminal=terminal,
            )
            if terminal:
                return
            current = next_cursor
            page += 1


class HtmlCatalogEnumerator:
    async def enumerate(
        self,
        *,
        config: Mapping[str, Any],
        start: int = 0,
        max_pages: int = 1,
        fetcher: Callable[..., Any] | None = None,
        **_: Any,
    ) -> AsyncIterator[EnumeratedBatch]:
        if start > 0:
            return
        if fetcher is None:
            raise ValueError("HTML_CATALOG requires an injected fetcher")
        root = str(config["root"])
        raw = await _invoke(fetcher, root, int(config.get("max_bytes", 0)))
        body = raw.get("body", b"") if isinstance(raw, Mapping) else raw
        if isinstance(body, str):
            body = body.encode("utf-8")
        if not isinstance(body, (bytes, bytearray)):
            raise TypeError("HTML_CATALOG fetcher body must be bytes or text")
        parser = _LinkParser()
        parser.feed(bytes(body).decode("utf-8", errors="replace"))

        artifacts: list[str] = []
        for href in parser.links:
            url = urljoin(root, href)
            parsed = urlsplit(url)
            if parsed.scheme not in {"http", "https"}:
                continue
            if not _origin_allowed(root, url, config):
                continue
            artifacts.append(
                urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, ""))
            )
        yield EnumeratedBatch(
            artifacts=tuple(dict.fromkeys(artifacts)),
            page=0,
            next_page=1,
            bytes_read=_bytes(raw) or len(body),
            requests=1,
            status=_status(raw),
            terminal=True,
        )


def _origin_allowed(root: str, candidate: str, config: Mapping[str, Any]) -> bool:
    policy = str(config["origin_policy"]).upper()
    root_parts = urlsplit(root)
    candidate_parts = urlsplit(candidate)
    if policy == "SAME_ORIGIN":
        return (
            candidate_parts.scheme.lower(),
            candidate_parts.netloc.lower(),
        ) == (
            root_parts.scheme.lower(),
            root_parts.netloc.lower(),
        )
    if policy == "SAME_HOST":
        return candidate_parts.hostname == root_parts.hostname
    if policy == "ALLOWLIST":
        allowed = {
            value.rstrip("/").lower()
            for value in tuple(config.get("allowed_origins", ()))
        }
        origin = f"{candidate_parts.scheme}://{candidate_parts.netloc}".lower()
        return origin in allowed
    return False


def _product_size(dimensions: Mapping[str, tuple[Any, ...]]) -> int:
    result = 1
    for values in dimensions.values():
        result *= len(values)
    return result


def _bytes(raw: Any) -> int:
    if isinstance(raw, Mapping):
        if "bytes" in raw:
            return int(raw["bytes"])
        if "body" in raw:
            return _bytes(raw["body"])
    if isinstance(raw, (bytes, bytearray)):
        return len(raw)
    if isinstance(raw, str):
        return len(raw.encode("utf-8"))
    return 0


def _status(raw: Any) -> int | None:
    if isinstance(raw, Mapping) and raw.get("status") is not None:
        return int(raw["status"])
    return None


def _coerce_batch(raw: Any, *, page: int) -> EnumeratedBatch:
    if isinstance(raw, EnumeratedBatch):
        return raw
    if isinstance(raw, Mapping):
        items = raw.get("items", raw.get("artifacts", ()))
        return EnumeratedBatch(
            tuple(str(item) for item in items),
            page=page,
            next_page=page + 1,
            next_cursor=raw.get("next"),
            bytes_read=_bytes(raw),
            requests=int(raw.get("requests", 1)),
            status=_status(raw),
            terminal=bool(raw.get("terminal", False)),
        )
    if isinstance(raw, (list, tuple)):
        return EnumeratedBatch(tuple(str(item) for item in raw), page=page, requests=1)
    raise TypeError(
        "fetcher must return an EnumeratedBatch, mapping, or sequence"
    )


def _terminal(
    batch: EnumeratedBatch,
    rule: str,
    *,
    max_page: int | None = None,
) -> bool:
    rule = rule.upper()
    if rule in {"EMPTY", "EMPTY_PAGE"}:
        return not batch.artifacts
    if rule in {"HTTP_404", "STATUS_404", "NOT_FOUND"}:
        return batch.status == 404
    if rule in {"LAST", "MAX_PAGE"}:
        return (
            max_page is not None
            and batch.page is not None
            and batch.page >= max_page
        )
    if rule in {"EXPLICIT", "FETCHER_TERMINAL"}:
        return batch.terminal
    raise ValueError(f"unsupported integer pagination terminal rule: {rule}")
