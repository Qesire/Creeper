"""Frozen contracts and transport helpers for deterministic repository roots.

The objects in this module are research/discovery state only.  They intentionally
have no annual-evidence authority: repository metadata, publication dates and
artifact metadata may schedule downstream work but cannot prove a historical
``(canonical_hostname, year)`` pair.
"""

from __future__ import annotations

import json
import math
from collections.abc import Awaitable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Protocol
from urllib.parse import unquote, urljoin, urlsplit


@dataclass(frozen=True)
class RootQuery:
    query_id: str
    root_id: str
    query_text: str
    max_pages: int
    max_wall_seconds: float
    page_size: int = 1000
    native_filters: dict[str, str] = field(default_factory=dict)
    expected_signal: str = ""
    expected_artifact_family: str = ""

    def __post_init__(self) -> None:
        if not self.query_id.strip() or not self.root_id.strip():
            raise ValueError("query_id and root_id are required")
        if self.max_pages < 1:
            raise ValueError("max_pages must be positive")
        if not math.isfinite(self.max_wall_seconds) or self.max_wall_seconds <= 0:
            raise ValueError("max_wall_seconds must be finite and positive")
        if self.page_size < 1:
            raise ValueError("page_size must be positive")
        if not isinstance(self.native_filters, dict):
            raise TypeError("native_filters must be a dict")
        if any(not isinstance(key, str) or not isinstance(value, str) for key, value in self.native_filters.items()):
            raise TypeError("native_filters keys and values must be strings")


@dataclass(frozen=True)
class SearchCheckpoint:
    cursor: str | None = None
    next_url: str | None = None
    page: int = 1
    start: int = 0
    query_variant: str | None = None

    def __post_init__(self) -> None:
        if self.page < 1:
            raise ValueError("checkpoint page must be >= 1")
        if self.start < 0:
            raise ValueError("checkpoint start must be non-negative")


@dataclass(frozen=True)
class SearchHit:
    root_id: str
    query_id: str
    provider_native_id: str
    provider_url: str
    provider_type: str
    title: str = ""
    description: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    observed_at: float = 0.0
    research_node_id: str = ""

    def __post_init__(self) -> None:
        if not self.root_id.strip() or not self.query_id.strip():
            raise ValueError("root_id and query_id are required")
        if not self.provider_native_id.strip():
            raise ValueError("provider_native_id is required")
        if not self.provider_type.strip():
            raise ValueError("provider_type is required")


@dataclass(frozen=True)
class ArtifactLead:
    root_id: str
    provider_native_id: str
    locator: str
    content_type: str = ""
    size: int | None = None
    checksum: str | None = None
    persistent_id: str | None = None
    parent_persistent_id: str | None = None
    kind: str = field(default="ARTIFACT_LEAD", init=False)
    evidence_year: None = field(default=None, init=False)

    def __post_init__(self) -> None:
        if not self.root_id.strip() or not self.provider_native_id.strip():
            raise ValueError("root_id and provider_native_id are required")
        parsed = urlsplit(self.locator)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("artifact locator must be an absolute http(s) URL")
        if self.size is not None and self.size < 0:
            raise ValueError("artifact size must be non-negative")


@dataclass(frozen=True)
class SearchPage:
    hits: tuple[SearchHit, ...] = ()
    artifact_leads: tuple[ArtifactLead, ...] = ()
    next_checkpoint: SearchCheckpoint | None = None
    terminal: bool = True
    requests: int = 1
    bytes_read: int = 0
    retry_after: float | None = None

    def __post_init__(self) -> None:
        if self.requests < 0 or self.bytes_read < 0:
            raise ValueError("requests and bytes_read must be non-negative")
        if self.retry_after is not None and self.retry_after < 0:
            raise ValueError("retry_after must be non-negative")
        if self.retry_after is not None and self.terminal:
            raise ValueError("retryable pages cannot be terminal")


@dataclass(frozen=True)
class RootCapabilityReport:
    root_id: str
    available: bool
    capabilities: tuple[str, ...] = ()
    reason: str = ""
    status_code: int | None = None


class JsonTransport(Protocol):
    def __call__(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Awaitable[Any]: ...


class RootRequestError(RuntimeError):
    """A non-retryable structured-root request failed."""

    def __init__(self, message: str, *, status_code: int | None = None, url: str = "") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.url = url


def status_code(response: Any) -> int:
    try:
        return int(getattr(response, "status_code", 200))
    except (TypeError, ValueError):
        return 200


def response_json(response: Any) -> dict[str, Any]:
    payload = response.json() if callable(getattr(response, "json", None)) else response
    if not isinstance(payload, dict):
        raise ValueError("root response must be a JSON object")
    return payload


def response_bytes_len(response: Any, payload: Mapping[str, Any] | None = None) -> int:
    content = getattr(response, "content", None)
    if isinstance(content, (bytes, bytearray, memoryview)):
        return len(content)
    if isinstance(content, str):
        return len(content.encode("utf-8"))
    if payload is None:
        return 0
    return len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def is_retryable(response: Any) -> bool:
    status = status_code(response)
    return status == 429 or 500 <= status <= 599


def retry_after_seconds(response: Any, *, now: datetime | None = None) -> float | None:
    value = getattr(response, "headers", {}).get("Retry-After")
    if value is None:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        try:
            target = parsedate_to_datetime(str(value))
        except (TypeError, ValueError, OverflowError):
            return None
        if target.tzinfo is None:
            target = target.replace(tzinfo=timezone.utc)
        current = now or datetime.now(timezone.utc)
        seconds = (target - current).total_seconds()
    return max(0.0, seconds)


def retry_delay_seconds(response: Any) -> float:
    explicit = retry_after_seconds(response)
    if explicit is not None:
        return explicit
    return 30.0 if status_code(response) == 429 else 5.0


def ensure_success(response: Any, *, url: str = "") -> None:
    status = status_code(response)
    if status >= 400:
        raise RootRequestError(
            f"structured-root request failed with HTTP {status}",
            status_code=status,
            url=url,
        )


def safe_next_url(endpoint: str, candidate: str | None) -> str | None:
    if not candidate:
        return None
    resolved = urljoin(endpoint, str(candidate))
    expected = urlsplit(endpoint)
    parsed = urlsplit(resolved)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("next URL must be absolute http(s)")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("next URL must not contain userinfo")
    if (parsed.scheme.lower(), parsed.netloc.lower()) != (
        expected.scheme.lower(),
        expected.netloc.lower(),
    ):
        raise ValueError("next URL escaped the configured repository origin")
    return resolved


def normalize_doi(value: Any) -> str:
    text = unquote(str(value or "").strip())
    lower = text.lower()
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if lower.startswith(prefix):
            text = text[len(prefix) :]
            break
    return text.strip().lower()


def merge_native_filters(
    base: Mapping[str, Any],
    native_filters: Mapping[str, str],
    *,
    reserved: frozenset[str],
) -> dict[str, Any]:
    collisions = sorted(key for key in native_filters if key in reserved)
    if collisions:
        raise ValueError(
            "native_filters cannot override adapter control fields: " + ", ".join(collisions)
        )
    merged = dict(base)
    merged.update(native_filters)
    return merged


def checkpoint_with_page_budget(
    query: RootQuery,
    *,
    current_page: int,
    checkpoint: SearchCheckpoint | None,
) -> SearchCheckpoint | None:
    if checkpoint is None or current_page >= query.max_pages:
        return None
    return checkpoint
