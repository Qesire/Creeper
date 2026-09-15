"""Four-level deterministic identity for residual-search results."""

from __future__ import annotations

import hashlib
import posixpath
import re
import sqlite3
import time
from dataclasses import dataclass
from typing import Callable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_TRACKING_PARAMS = frozenset({"fbclid", "gclid", "mc_cid", "mc_eid", "msclkid"})
_YEAR_RE = re.compile(r"\b(?:199[0-9]|200[0-9])\b")
_VERSION_RE = re.compile(r"\b(?:v(?:ersion)?\s*)?\d+(?:\.\d+){0,3}\b", re.IGNORECASE)
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_DOI_RE = re.compile(r"^(?:https?://(?:dx\.)?doi\.org/)?(10\.\d{4,9}/\S+)$", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class RawSearchResult:
    provider: str
    provider_result_id: str
    url: str
    title: str = ""
    description: str = ""
    publisher: str = ""
    creators: tuple[str, ...] = ()
    publication_year: int | None = None
    resource_type: str = ""
    identifiers: tuple[str, ...] = ()
    content_length: int | None = None
    etag: str | None = None
    checksum_sha256: str | None = None

    def __post_init__(self) -> None:
        if not self.provider.strip() or not self.provider_result_id.strip() or not self.url.strip():
            raise ValueError("provider, provider_result_id and url are required")
        object.__setattr__(self, "creators", tuple(self.creators))
        object.__setattr__(self, "identifiers", tuple(self.identifiers))
        if self.publication_year is not None and (
            isinstance(self.publication_year, bool)
            or not isinstance(self.publication_year, int)
            or not 1000 <= self.publication_year <= 9999
        ):
            raise ValueError("publication_year must be a four-digit integer")
        if self.content_length is not None and (
            isinstance(self.content_length, bool)
            or not isinstance(self.content_length, int)
            or self.content_length < 0
        ):
            raise ValueError("content_length must be non-negative")


@dataclass(frozen=True, slots=True)
class CanonicalSearchResult:
    raw: RawSearchResult
    canonical_url: str
    url_key: str
    artifact_key: str
    dataset_key: str
    family_key: str
    family_label: str
    relevance_score: float
    qualified: bool

    def __post_init__(self) -> None:
        if not 0.0 <= float(self.relevance_score) <= 1.0:
            raise ValueError("relevance_score must be within [0,1]")


@dataclass(frozen=True, slots=True)
class IdentityRegistration:
    new_url: bool
    new_artifact: bool
    new_dataset: bool
    new_family: bool


def _hash(prefix: str, value: str) -> str:
    return prefix + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _normalize_text(value: str) -> str:
    return " ".join(_TOKEN_RE.findall(value.lower()))


def canonicalize_result_url(value: str) -> str:
    parsed = urlsplit(value.strip())
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"} or parsed.hostname is None:
        raise ValueError("search-result URL must be http(s) with a hostname")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("search-result URL must not contain userinfo")
    try:
        host = parsed.hostname.encode("idna").decode("ascii").lower()
        port = parsed.port
    except (UnicodeError, ValueError) as exc:
        raise ValueError("invalid search-result URL") from exc
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if port is not None and not (
        (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    ):
        host = f"{host}:{port}"
    raw_path = parsed.path or "/"
    path = posixpath.normpath(raw_path)
    if not path.startswith("/"):
        path = "/" + path
    if raw_path.endswith("/") and path != "/" and not path.endswith("/"):
        path += "/"
    query = []
    for key, val in parse_qsl(parsed.query, keep_blank_values=True):
        lowered = key.lower()
        if lowered.startswith("utm_") or lowered in _TRACKING_PARAMS:
            continue
        query.append((key, val))
    query.sort()
    return urlunsplit((scheme, host, path, urlencode(query, doseq=True), ""))


def _normalized_doi(value: str) -> str | None:
    match = _DOI_RE.match(value.strip().rstrip(".,;"))
    return None if match is None else match.group(1).lower()


def _doi(result: RawSearchResult) -> str | None:
    values = list(result.identifiers)
    if result.provider.lower() == "datacite":
        values.insert(0, result.provider_result_id)
    for value in values:
        normalized = _normalized_doi(value)
        if normalized is not None:
            return normalized
    return None


def _family_label(result: RawSearchResult, canonical_url: str) -> str:
    title = _VERSION_RE.sub(" ", _YEAR_RE.sub(" ", _normalize_text(result.title)))
    title = " ".join(title.split())
    publisher = _normalize_text(result.publisher)
    host = urlsplit(canonical_url).hostname or ""
    if len(title) < 6:
        title = _normalize_text(posixpath.basename(urlsplit(canonical_url).path))
    if len(title) < 6:
        title = host
    if publisher and publisher not in title:
        return f"{title} | {publisher}"[:240]
    return f"{title} | {host}"[:240]


def canonicalize_search_result(
    result: RawSearchResult,
    *,
    relevance_score: float,
    qualified: bool,
) -> CanonicalSearchResult:
    url = canonicalize_result_url(result.url)
    url_key = _hash("url:", url)
    checksum = (result.checksum_sha256 or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{64}", checksum):
        artifact_key = f"artifact:sha256:{checksum}"
    elif result.etag and result.content_length is not None:
        artifact_key = _hash("artifact:etag:", f"{result.etag}\x1f{result.content_length}")
    else:
        artifact_key = _hash("artifact:url:", url)

    doi = _doi(result)
    if doi is not None:
        dataset_key = f"dataset:doi:{doi}"
    else:
        title = _normalize_text(result.title)
        publisher = _normalize_text(result.publisher)
        creators = "|".join(_normalize_text(item) for item in result.creators[:3])
        dataset_key = (
            _hash("dataset:meta:", f"{title}\x1f{publisher}\x1f{creators}")
            if len(title) >= 8
            else _hash("dataset:url:", url)
        )

    family_label = _family_label(result, url)
    return CanonicalSearchResult(
        raw=result,
        canonical_url=url,
        url_key=url_key,
        artifact_key=artifact_key,
        dataset_key=dataset_key,
        family_key=_hash("family:", family_label),
        family_label=family_label,
        relevance_score=float(relevance_score),
        qualified=bool(qualified),
    )


class SearchIdentityLedger:
    """Durable URL -> artifact -> dataset -> source-family identity."""

    def __init__(
        self,
        connection: sqlite3.Connection,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.connection = connection
        self.clock = clock
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS residual_search_urls(
                url_key TEXT PRIMARY KEY,
                canonical_url TEXT NOT NULL UNIQUE,
                first_seen_at REAL NOT NULL,
                last_seen_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS residual_search_artifacts(
                artifact_key TEXT PRIMARY KEY,
                first_url_key TEXT NOT NULL,
                first_seen_at REAL NOT NULL,
                last_seen_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS residual_search_families(
                family_key TEXT PRIMARY KEY,
                family_label TEXT NOT NULL,
                first_seen_at REAL NOT NULL,
                last_seen_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS residual_search_datasets(
                dataset_key TEXT PRIMARY KEY,
                family_key TEXT NOT NULL,
                title TEXT NOT NULL,
                publisher TEXT NOT NULL,
                first_seen_at REAL NOT NULL,
                last_seen_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS residual_search_references(
                provider TEXT NOT NULL,
                provider_result_id TEXT NOT NULL,
                cell_key TEXT NOT NULL,
                url_key TEXT NOT NULL,
                artifact_key TEXT NOT NULL,
                dataset_key TEXT NOT NULL,
                family_key TEXT NOT NULL,
                relevance_score REAL NOT NULL,
                qualified INTEGER NOT NULL,
                seen_at REAL NOT NULL,
                PRIMARY KEY(provider, provider_result_id, cell_key)
            );
            CREATE INDEX IF NOT EXISTS idx_residual_refs_dataset
                ON residual_search_references(dataset_key);
            CREATE INDEX IF NOT EXISTS idx_residual_refs_family
                ON residual_search_references(family_key);
            """
        )

    def _exists(self, table: str, column: str, value: str) -> bool:
        return self.connection.execute(
            f"SELECT 1 FROM {table} WHERE {column}=? LIMIT 1", (value,)
        ).fetchone() is not None

    def register(self, *, cell_key: str, result: CanonicalSearchResult) -> IdentityRegistration:
        now = float(self.clock())
        registration = IdentityRegistration(
            new_url=not self._exists("residual_search_urls", "url_key", result.url_key),
            new_artifact=not self._exists("residual_search_artifacts", "artifact_key", result.artifact_key),
            new_dataset=not self._exists("residual_search_datasets", "dataset_key", result.dataset_key),
            new_family=not self._exists("residual_search_families", "family_key", result.family_key),
        )
        with self.connection:
            self.connection.execute(
                "INSERT INTO residual_search_urls VALUES(?,?,?,?) "
                "ON CONFLICT(url_key) DO UPDATE SET last_seen_at=excluded.last_seen_at",
                (result.url_key, result.canonical_url, now, now),
            )
            self.connection.execute(
                "INSERT INTO residual_search_artifacts VALUES(?,?,?,?) "
                "ON CONFLICT(artifact_key) DO UPDATE SET last_seen_at=excluded.last_seen_at",
                (result.artifact_key, result.url_key, now, now),
            )
            self.connection.execute(
                "INSERT INTO residual_search_families VALUES(?,?,?,?) "
                "ON CONFLICT(family_key) DO UPDATE SET last_seen_at=excluded.last_seen_at",
                (result.family_key, result.family_label, now, now),
            )
            self.connection.execute(
                "INSERT INTO residual_search_datasets VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(dataset_key) DO UPDATE SET last_seen_at=excluded.last_seen_at",
                (result.dataset_key, result.family_key, result.raw.title, result.raw.publisher, now, now),
            )
            self.connection.execute(
                """
                INSERT INTO residual_search_references
                VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(provider,provider_result_id,cell_key) DO UPDATE SET
                    url_key=excluded.url_key,
                    artifact_key=excluded.artifact_key,
                    dataset_key=excluded.dataset_key,
                    family_key=excluded.family_key,
                    relevance_score=excluded.relevance_score,
                    qualified=excluded.qualified,
                    seen_at=excluded.seen_at
                """,
                (
                    result.raw.provider,
                    result.raw.provider_result_id,
                    cell_key,
                    result.url_key,
                    result.artifact_key,
                    result.dataset_key,
                    result.family_key,
                    result.relevance_score,
                    int(result.qualified),
                    now,
                ),
            )
        return registration
