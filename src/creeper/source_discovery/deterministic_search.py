"""Deterministic residual-search providers and pure result classification."""

from __future__ import annotations

import asyncio
import math
import re
import time
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Protocol
from urllib.parse import urlsplit

import httpx

from creeper.source_discovery.models import (
    SourceCandidate,
    SourceLevel,
    is_common_crawl_provenance,
    is_direct_evidence_entrypoint,
)
from creeper.source_discovery.residual_search import QueryPlan, SearchCell
from creeper.source_discovery.search_identity import (
    CanonicalSearchResult,
    RawSearchResult,
    canonicalize_search_result,
)


_MECHANISM_TERMS: dict[str, tuple[str, ...]] = {
    "proxy_access": ("proxy", "cache", "access log", "http trace"),
    "client_trace": ("client trace", "web trace", "http trace", "browser trace"),
    "dns_survey": ("dns", "host survey", "zone transfer", "hostcount"),
    "ftp": ("ftp", "anonymous ftp", "ftp sites"),
    "bbs_telnet": ("bbs", "telnet", "bulletin board"),
    "gopher": ("gopher",),
    "mail": ("mail archive", "mailing list", "mbox"),
    "usenet": ("usenet", "netnews"),
    "search_engine": ("search engine", "web index", "search index"),
    "crawler_frontier": ("crawler frontier", "crawl seeds", "seed list"),
    "human_directory": ("web directory", "internet directory", "site directory"),
    "link_graph": ("link graph", "hyperlink graph", "web links"),
    "nic_registry": ("nic", "registry", "host list"),
    "isp_inventory": ("isp", "host inventory", "network inventory"),
    "software_mirror": ("mirror sites", "software mirror", "mirror list"),
}
_ARTIFACT_TERMS = (
    "dataset",
    "data set",
    "trace",
    "log",
    "dump",
    "list",
    "index",
    "catalog",
    "database",
    "archive",
)
_DIRECT_SUFFIXES = (
    ".cdx",
    ".cdx.gz",
    ".cdxj",
    ".cdxj.gz",
)
_SOURCE_SUFFIXES = (
    ".txt",
    ".csv",
    ".tsv",
    ".json",
    ".jsonl",
    ".xml",
    ".rdf",
    ".zip",
    ".gz",
    ".bz2",
    ".xz",
    ".tar",
    ".tgz",
    ".warc",
    ".warc.gz",
    ".arc",
    ".arc.gz",
) + _DIRECT_SUFFIXES


@dataclass(frozen=True, slots=True)
class DeterministicSearchPolicy:
    results_per_provider: int = 100
    max_total_results: int = 300
    min_relevance_score: float = 0.55
    timeout_seconds: float = 20.0

    def __post_init__(self) -> None:
        for name in ("results_per_provider", "max_total_results"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if not 0.0 <= float(self.min_relevance_score) <= 1.0:
            raise ValueError("min_relevance_score must be within [0,1]")
        if not math.isfinite(float(self.timeout_seconds)) or self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be finite and positive")


@dataclass(frozen=True, slots=True)
class DeterministicSearchBatch:
    backend: str
    query: str
    actor: str
    results: tuple[CanonicalSearchResult, ...] = ()
    search_cost_seconds: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "results", tuple(self.results))
        if not self.backend.strip() or not self.query.strip() or not self.actor.strip():
            raise ValueError("deterministic search attribution is required")
        if self.search_cost_seconds is not None and (
            isinstance(self.search_cost_seconds, bool)
            or not isinstance(self.search_cost_seconds, (int, float))
            or not math.isfinite(float(self.search_cost_seconds))
            or self.search_cost_seconds < 0
        ):
            raise ValueError("search_cost_seconds must be finite and non-negative")


class DeterministicSearchProvider(Protocol):
    name: str

    async def search(
        self,
        plan: QueryPlan,
        *,
        limit: int,
    ) -> tuple[RawSearchResult, ...]: ...


def _period_years(period: str) -> tuple[int, ...]:
    if "-" not in period:
        return (int(period),)
    left, right = period.split("-", 1)
    return tuple(range(max(1996, int(left)), min(2001, int(right)) + 1))


def _result_text(result: RawSearchResult) -> str:
    return " ".join(
        (
            result.title,
            result.description,
            result.publisher,
            result.resource_type,
            " ".join(result.creators),
        )
    ).lower()


def relevance_score(cell: SearchCell, result: RawSearchResult) -> float:
    text = _result_text(result)
    years = _period_years(cell.period)
    year_hit = (
        result.publication_year in years
        or any(re.search(rf"\b{year}\b", text) for year in years)
    )
    terms = _MECHANISM_TERMS[cell.mechanism]
    mechanism_hit = any(term in text for term in terms)
    artifact_hit = (
        result.resource_type.lower() in {"dataset", "collection", "software"}
        or any(term in text for term in _ARTIFACT_TERMS)
        or urlsplit(result.url).path.lower().endswith(_SOURCE_SUFFIXES)
    )
    score = 0.45 * float(mechanism_hit)
    score += 0.35 * float(year_hit)
    score += 0.20 * float(artifact_hit)
    return min(1.0, score)


def classify_result(
    plan: QueryPlan,
    result: RawSearchResult,
    *,
    policy: DeterministicSearchPolicy,
) -> CanonicalSearchResult | None:
    if is_common_crawl_provenance(
        result.provider,
        result.url,
        result.title,
        result.description,
    ):
        return None
    score = relevance_score(plan.cell, result)
    return canonicalize_search_result(
        result,
        relevance_score=score,
        qualified=score >= policy.min_relevance_score,
    )


def candidate_from_result(
    plan: QueryPlan,
    result: CanonicalSearchResult,
) -> SourceCandidate:
    path = urlsplit(result.canonical_url).path.lower()
    source_like = path.endswith(_SOURCE_SUFFIXES)
    direct = is_direct_evidence_entrypoint(result.canonical_url)
    years = _period_years(plan.cell.period)
    temporal = 0.85 if any(
        re.search(rf"\b{year}\b", _result_text(result.raw)) for year in years
    ) else 0.60
    return SourceCandidate(
        canonical_entrypoint=result.canonical_url,
        source_family=f"RESIDUAL_{plan.cell.mechanism.upper()}",
        level=SourceLevel.SOURCE if source_like else SourceLevel.COLLECTION,
        discovered_by=f"deterministic:{result.raw.provider}",
        discovery_strategy="RESIDUAL_CELL_SEARCH",
        expected_year_from=min(years),
        expected_year_to=max(years),
        expected_volume=None,
        temporal_semantics_prior=temporal,
        enumerability_prior=0.90 if source_like else 0.65,
        direct_evidence_prior=1.0 if direct else 0.0,
        baseline_overlap_prior=0.5,
        access_cost_prior=0.45 if source_like else 0.75,
        adapter_cost_prior=0.40 if source_like else 0.90,
        confidence=max(0.35, result.relevance_score),
    )


def _string_publisher(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("name", "publisher", "title"):
            item = value.get(key)
            if isinstance(item, str):
                return item
    return ""


def _first_title(attributes: dict[str, object]) -> str:
    titles = attributes.get("titles")
    if not isinstance(titles, list):
        return ""
    for item in titles:
        if isinstance(item, dict) and isinstance(item.get("title"), str):
            return str(item["title"])
    return ""


def _description(attributes: dict[str, object]) -> str:
    values: list[str] = []
    descriptions = attributes.get("descriptions")
    if isinstance(descriptions, list):
        for item in descriptions[:3]:
            if isinstance(item, dict) and isinstance(item.get("description"), str):
                values.append(str(item["description"]))
    subjects = attributes.get("subjects")
    if isinstance(subjects, list):
        for item in subjects[:12]:
            if isinstance(item, dict) and isinstance(item.get("subject"), str):
                values.append(str(item["subject"]))
    return " ".join(values)


def _creators(attributes: dict[str, object]) -> tuple[str, ...]:
    creators = attributes.get("creators")
    if not isinstance(creators, list):
        return ()
    values: list[str] = []
    for item in creators[:8]:
        if isinstance(item, dict) and isinstance(item.get("name"), str):
            values.append(str(item["name"]))
    return tuple(values)


def _content_urls(attributes: dict[str, object]) -> tuple[str, ...]:
    raw = attributes.get("contentUrl")
    if not isinstance(raw, list):
        return ()
    urls: list[str] = []
    for item in raw:
        if isinstance(item, str) and item.startswith(("http://", "https://")):
            urls.append(item)
        elif isinstance(item, dict):
            for key in ("url", "href"):
                value = item.get(key)
                if isinstance(value, str) and value.startswith(("http://", "https://")):
                    urls.append(value)
                    break
    return tuple(urls)


def _best_url(attributes: dict[str, object]) -> str | None:
    content = _content_urls(attributes)
    if content:
        ranked = sorted(
            content,
            key=lambda value: (
                not urlsplit(value).path.lower().endswith(_SOURCE_SUFFIXES),
                len(value),
                value,
            ),
        )
        return ranked[0]
    landing = attributes.get("url")
    if isinstance(landing, str) and landing.startswith(("http://", "https://")):
        return landing
    return None


class DataCiteSearchProvider:
    """Public unauthenticated DataCite DOI metadata search."""

    name = "datacite"

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        endpoint: str = "https://api.datacite.org/dois",
        timeout_seconds: float = 20.0,
    ) -> None:
        self.client = client
        self.endpoint = endpoint
        self.timeout_seconds = float(timeout_seconds)

    @staticmethod
    def _query(plan: QueryPlan) -> str:
        phrase = _MECHANISM_TERMS[plan.cell.mechanism][0]
        return f'"{plan.cell.period}" "{phrase}"'

    async def search(
        self,
        plan: QueryPlan,
        *,
        limit: int,
    ) -> tuple[RawSearchResult, ...]:
        page_size = min(1000, max(1, int(limit)))
        response = await self.client.get(
            self.endpoint,
            params={
                "query": self._query(plan),
                "page[size]": page_size,
                "sort": "relevance",
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list):
            return ()

        results: list[RawSearchResult] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            attributes = item.get("attributes")
            if not isinstance(attributes, dict):
                continue
            url = _best_url(attributes)
            if url is None:
                continue
            record_id = item.get("id")
            if not isinstance(record_id, str) or not record_id.strip():
                doi = attributes.get("doi")
                if not isinstance(doi, str) or not doi.strip():
                    continue
                record_id = doi
            publication_year = attributes.get("publicationYear")
            try:
                year = int(publication_year) if publication_year is not None else None
            except (TypeError, ValueError):
                year = None
            types = attributes.get("types")
            resource_type = ""
            if isinstance(types, dict):
                value = types.get("resourceTypeGeneral") or types.get("resourceType")
                if isinstance(value, str):
                    resource_type = value
            doi = attributes.get("doi")
            identifiers = (str(doi),) if isinstance(doi, str) else ()
            results.append(
                RawSearchResult(
                    provider=self.name,
                    provider_result_id=record_id,
                    url=url,
                    title=_first_title(attributes),
                    description=_description(attributes),
                    publisher=_string_publisher(attributes.get("publisher")),
                    creators=_creators(attributes),
                    publication_year=year,
                    resource_type=resource_type,
                    identifiers=identifiers,
                )
            )
            if len(results) >= page_size:
                break
        return tuple(results)


class DeterministicSearchExecutor:
    """Execute one search cell across independent structured providers."""

    def __init__(
        self,
        providers: tuple[DeterministicSearchProvider, ...],
        *,
        policy: DeterministicSearchPolicy | None = None,
        actor: str = "deterministic:residual-search",
    ) -> None:
        if not providers:
            raise ValueError("at least one deterministic search provider is required")
        self.providers = tuple(providers)
        self.policy = policy or DeterministicSearchPolicy()
        self.actor = actor

    async def __call__(self, plan: QueryPlan) -> DeterministicSearchBatch:
        started = time.perf_counter()
        calls = [
            provider.search(plan, limit=self.policy.results_per_provider)
            for provider in self.providers
        ]
        outcomes = await asyncio.gather(*calls, return_exceptions=True)
        successful: list[str] = []
        raw_results: list[RawSearchResult] = []
        errors: list[Exception] = []
        for provider, outcome in zip(self.providers, outcomes, strict=True):
            if isinstance(outcome, Exception):
                errors.append(outcome)
                continue
            successful.append(provider.name)
            raw_results.extend(outcome)
        if not successful:
            detail = "; ".join(type(error).__name__ for error in errors) or "no providers"
            raise RuntimeError(f"all deterministic search providers failed: {detail}")

        canonical: list[CanonicalSearchResult] = []
        for result in raw_results[: self.policy.max_total_results]:
            try:
                classified = classify_result(plan, result, policy=self.policy)
            except ValueError:
                continue
            if classified is not None:
                canonical.append(classified)
        return DeterministicSearchBatch(
            backend="+".join(successful),
            query=plan.query,
            actor=self.actor,
            results=tuple(canonical),
            search_cost_seconds=time.perf_counter() - started,
        )
