"""Deterministic residual-search providers and pure result classification."""

from __future__ import annotations

import asyncio
import math
import re
import time
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urlsplit

import httpx

from creeper.source_discovery.models import (
    SourceCandidate,
    SourceLevel,
    is_common_crawl_provenance,
    is_direct_evidence_entrypoint,
    format_path_from_locator,
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
        or format_path_from_locator(result.url).endswith(_SOURCE_SUFFIXES)
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
    path = format_path_from_locator(result.canonical_url)
    source_like = path.endswith(_SOURCE_SUFFIXES)
    direct = is_direct_evidence_entrypoint(result.canonical_url)
    years = _period_years(plan.cell.period)
    temporal = 0.85 if any(
        re.search(rf"\b{year}\b", _result_text(result.raw)) for year in years
    ) else 0.60
    return SourceCandidate(
        canonical_entrypoint=result.canonical_url,
        source_family=(
            f"RESIDUAL_{plan.cell.mechanism.upper()}:"
            f"{result.family_key.removeprefix('family:')[:20]}"
        ),
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
        # Provider queries must preserve all SearchCell dimensions; otherwise
        # distinct coverage cells collapse back into the same famous result set.
        variants = _MECHANISM_TERMS[plan.cell.mechanism]
        phrase = variants[plan.variant % len(variants)]
        institution = plan.cell.institution.replace("_", " ")
        artifact = plan.cell.artifact.replace("_", " ")
        exclusions = " ".join(
            f'-"{item}"'
            for item in plan.exclusions
            if item and len(item) <= 80
        )
        query = (
            f'"{plan.cell.period}" "{phrase}" '
            f'"{institution}" "{artifact}"'
        )
        return f"{query} {exclusions}".strip()

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


def _zenodo_records(payload: object) -> list[dict[str, object]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    hits = payload.get("hits")
    if isinstance(hits, dict):
        records = hits.get("hits")
        if isinstance(records, list):
            return [item for item in records if isinstance(item, dict)]
    data = payload.get("data")
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []


def _zenodo_doi(item: dict[str, object], metadata: dict[str, object]) -> str | None:
    for value in (item.get("doi"), metadata.get("doi")):
        if isinstance(value, str) and value.strip():
            return value.strip()
    pids = item.get("pids")
    if isinstance(pids, dict):
        doi = pids.get("doi")
        if isinstance(doi, dict):
            identifier = doi.get("identifier")
            if isinstance(identifier, str) and identifier.strip():
                return identifier.strip()
    return None


def _zenodo_file_candidates(
    item: dict[str, object],
) -> list[tuple[str, int | None, str | None]]:
    values: list[tuple[str, int | None, str | None]] = []
    files = item.get("files")
    raw_files: list[dict[str, object]] = []
    if isinstance(files, list):
        raw_files.extend(entry for entry in files if isinstance(entry, dict))
    elif isinstance(files, dict):
        entries = files.get("entries")
        if isinstance(entries, dict):
            raw_files.extend(
                entry for entry in entries.values() if isinstance(entry, dict)
            )
    for entry in raw_files:
        links = entry.get("links")
        url = None
        if isinstance(links, dict):
            for key in ("content", "download", "self"):
                candidate = links.get(key)
                if isinstance(candidate, str) and candidate.startswith(
                    ("http://", "https://")
                ):
                    url = candidate
                    break
        if url is None:
            for key in ("download", "url"):
                candidate = entry.get(key)
                if isinstance(candidate, str) and candidate.startswith(
                    ("http://", "https://")
                ):
                    url = candidate
                    break
        if url is None:
            continue
        size = entry.get("size")
        if size is None:
            size = entry.get("filesize")
        try:
            content_length = int(size) if size is not None else None
        except (TypeError, ValueError):
            content_length = None
        checksum = entry.get("checksum")
        sha256 = None
        if isinstance(checksum, str):
            lowered = checksum.lower()
            if lowered.startswith("sha256:"):
                candidate = lowered.split(":", 1)[1]
                if re.fullmatch(r"[0-9a-f]{64}", candidate):
                    sha256 = candidate
        values.append((url, content_length, sha256))
    values.sort(
        key=lambda item: (
            not urlsplit(item[0]).path.lower().endswith(_SOURCE_SUFFIXES),
            item[1] is None,
            item[1] or 0,
            item[0],
        )
    )
    return values


def _zenodo_landing_url(item: dict[str, object]) -> str | None:
    links = item.get("links")
    if isinstance(links, dict):
        for key in ("self_html", "html", "latest_html"):
            value = links.get(key)
            if isinstance(value, str) and value.startswith(("http://", "https://")):
                return value
    record_id = item.get("id")
    if isinstance(record_id, (str, int)) and str(record_id).strip():
        return f"https://zenodo.org/records/{record_id}"
    return None


def _zenodo_creators(metadata: dict[str, object]) -> tuple[str, ...]:
    creators = metadata.get("creators")
    if not isinstance(creators, list):
        return ()
    result: list[str] = []
    for creator in creators[:8]:
        if not isinstance(creator, dict):
            continue
        for key in ("name", "person_or_org"):
            value = creator.get(key)
            if isinstance(value, str) and value.strip():
                result.append(value.strip())
                break
            if isinstance(value, dict):
                name = value.get("name")
                if isinstance(name, str) and name.strip():
                    result.append(name.strip())
                    break
    return tuple(result)


def _zenodo_resource_type(metadata: dict[str, object]) -> str:
    value = metadata.get("resource_type")
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("title", "id"):
            item = value.get(key)
            if isinstance(item, str):
                return item
    upload_type = metadata.get("upload_type")
    return upload_type if isinstance(upload_type, str) else ""


class ZenodoSearchProvider:
    """Anonymous bounded Zenodo record search with direct-file preference."""

    name = "zenodo"

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        endpoint: str = "https://zenodo.org/api/records",
        timeout_seconds: float = 20.0,
    ) -> None:
        self.client = client
        self.endpoint = endpoint
        self.timeout_seconds = float(timeout_seconds)

    @staticmethod
    def _query(plan: QueryPlan) -> str:
        variants = _MECHANISM_TERMS[plan.cell.mechanism]
        phrase = variants[plan.variant % len(variants)]
        institution = plan.cell.institution.replace("_", " ")
        artifact = plan.cell.artifact.replace("_", " ")
        clauses = [
            f'"{plan.cell.period}"',
            f'"{phrase}"',
            f'"{institution}"',
            f'"{artifact}"',
        ]
        clauses.extend(
            f'NOT "{item}"'
            for item in plan.exclusions
            if item and len(item) <= 80
        )
        return " AND ".join(clauses)

    async def search(
        self,
        plan: QueryPlan,
        *,
        limit: int,
    ) -> tuple[RawSearchResult, ...]:
        # Zenodo documents a maximum anonymous page size of 25.
        page_size = min(25, max(1, int(limit)))
        response = await self.client.get(
            self.endpoint,
            params={
                "q": self._query(plan),
                "size": page_size,
                "sort": "bestmatch",
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        records = _zenodo_records(response.json())
        results: list[RawSearchResult] = []
        for item in records:
            metadata_raw = item.get("metadata")
            metadata = metadata_raw if isinstance(metadata_raw, dict) else {}
            record_id = item.get("id")
            if not isinstance(record_id, (str, int)) or not str(record_id).strip():
                continue
            files = _zenodo_file_candidates(item)
            if files:
                url, content_length, sha256 = files[0]
            else:
                url = _zenodo_landing_url(item)
                content_length = None
                sha256 = None
            if url is None:
                continue
            title = metadata.get("title")
            if not isinstance(title, str):
                raw_title = item.get("title")
                title = raw_title if isinstance(raw_title, str) else ""
            description = metadata.get("description")
            description = description if isinstance(description, str) else ""
            keywords = metadata.get("keywords")
            if isinstance(keywords, list):
                description = " ".join(
                    (
                        description,
                        " ".join(
                            str(value)
                            for value in keywords[:16]
                            if isinstance(value, str)
                        ),
                    )
                ).strip()
            publication_date = metadata.get("publication_date")
            year = None
            if isinstance(publication_date, str) and len(publication_date) >= 4:
                prefix = publication_date[:4]
                if prefix.isdigit():
                    year = int(prefix)
            doi = _zenodo_doi(item, metadata)
            identifiers = (doi,) if doi is not None else ()
            results.append(
                RawSearchResult(
                    provider=self.name,
                    provider_result_id=str(record_id),
                    url=url,
                    title=title,
                    description=description,
                    publisher="Zenodo",
                    creators=_zenodo_creators(metadata),
                    publication_year=year,
                    resource_type=_zenodo_resource_type(metadata),
                    identifiers=identifiers,
                    content_length=content_length,
                    checksum_sha256=sha256,
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
