"""DataCite REST root with deterministic cursor traversal and metadata pivots."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import urlsplit

from .base import (
    ArtifactLead,
    JsonTransport,
    RootCapabilityReport,
    RootQuery,
    SearchCheckpoint,
    SearchHit,
    SearchPage,
    checkpoint_with_page_budget,
    ensure_success,
    is_retryable,
    merge_native_filters,
    normalize_doi,
    response_bytes_len,
    response_json,
    retry_delay_seconds,
    safe_next_url,
    status_code,
)

API = "https://api.datacite.org/dois"
PROJECTION = ",".join(
    (
        "doi",
        "types",
        "titles",
        "descriptions",
        "publisher",
        "publicationYear",
        "creators",
        "contributors",
        "dates",
        "relatedIdentifiers",
        "url",
        "contentUrl",
    )
)
_RESERVED_FILTERS = frozenset({"query", "page[size]", "page[cursor]", "page[number]", "fields[dois]", "detail"})


class DataCiteAdapter:
    root_id = "datacite"

    def __init__(
        self,
        *,
        transport: JsonTransport,
        endpoint: str = API,
        user_agent: str = "Creeper/2.1 (+https://github.com/Qesire/Creeper)",
    ) -> None:
        self.transport = transport
        self.endpoint = endpoint.rstrip("/")
        self.user_agent = user_agent
        parsed = urlsplit(self.endpoint)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("DataCite endpoint must be an absolute http(s) URL")

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/vnd.api+json",
            "User-Agent": self.user_agent,
        }

    async def probe_capabilities(self) -> RootCapabilityReport:
        try:
            response = await self.transport(
                self.endpoint,
                {
                    "page[size]": 1,
                    "page[cursor]": "1",
                    "fields[dois]": "doi",
                },
                self._headers(),
            )
        except Exception as exc:  # transport failure is capability state, not evidence state
            return RootCapabilityReport(self.root_id, False, reason=str(exc))
        code = status_code(response)
        if is_retryable(response):
            return RootCapabilityReport(
                self.root_id,
                False,
                reason=f"retryable_status:{code}",
                status_code=code,
            )
        try:
            ensure_success(response, url=self.endpoint)
            response_json(response)
        except Exception as exc:
            return RootCapabilityReport(
                self.root_id,
                False,
                reason=str(exc),
                status_code=code,
            )
        return RootCapabilityReport(
            self.root_id,
            True,
            ("cursor", "projection", "related_identifiers", "repository_relationships"),
            status_code=code,
        )

    async def search(
        self,
        query: RootQuery,
        checkpoint: SearchCheckpoint | None,
    ) -> SearchPage:
        cp = checkpoint or SearchCheckpoint(cursor="1", page=1)
        if cp.page > query.max_pages:
            return SearchPage(requests=0, terminal=True)

        url = safe_next_url(self.endpoint, cp.next_url) if cp.next_url else self.endpoint
        if cp.next_url:
            params: dict[str, Any] = {}
        else:
            params = merge_native_filters(
                {
                    "query": query.query_text,
                    "page[size]": min(1000, query.page_size),
                    "page[cursor]": cp.cursor or "1",
                    "fields[dois]": PROJECTION,
                    # provider relationship is included by DataCite when detail is enabled.
                    "detail": "true",
                },
                query.native_filters,
                reserved=_RESERVED_FILTERS,
            )

        response = await self.transport(url, params, self._headers())
        if is_retryable(response):
            return SearchPage(
                next_checkpoint=cp,
                terminal=False,
                retry_after=retry_delay_seconds(response),
            )
        ensure_success(response, url=url)
        payload = response_json(response)
        rows = payload.get("data") or []
        if not isinstance(rows, list):
            raise ValueError("DataCite response data must be a list")

        hits: list[SearchHit] = []
        seen_dois: set[str] = set()
        for item in rows:
            if not isinstance(item, Mapping):
                continue
            attrs = item.get("attributes") or {}
            if not isinstance(attrs, Mapping):
                attrs = {}
            doi = normalize_doi(item.get("id") or attrs.get("doi"))
            if not doi or doi in seen_dois:
                continue
            seen_dois.add(doi)
            metadata = _metadata(attrs, item, doi)
            provider_url = str(metadata.get("landing_url") or f"https://doi.org/{doi}")
            hits.append(
                SearchHit(
                    root_id=self.root_id,
                    query_id=query.query_id,
                    provider_native_id=doi,
                    provider_url=provider_url,
                    provider_type="DOI",
                    title=_title(attrs),
                    description=_description(attrs),
                    metadata=metadata,
                )
            )

        links = payload.get("links") or {}
        raw_next = links.get("next") if isinstance(links, Mapping) else None
        next_url = safe_next_url(self.endpoint, str(raw_next)) if raw_next else None
        next_cp = None
        if next_url:
            next_cp = SearchCheckpoint(next_url=next_url, page=cp.page + 1)
            next_cp = checkpoint_with_page_budget(
                query,
                current_page=cp.page,
                checkpoint=next_cp,
            )
        return SearchPage(
            hits=tuple(hits),
            next_checkpoint=next_cp,
            terminal=next_cp is None,
            bytes_read=response_bytes_len(response, payload),
        )

    async def resolve(self, node: SearchHit) -> tuple[ArtifactLead, ...]:
        if node.root_id != self.root_id:
            raise ValueError("cannot resolve a node from another root")
        urls = node.metadata.get("content_urls") or ()
        leads: list[ArtifactLead] = []
        seen: set[str] = set()
        for index, raw in enumerate(urls):
            locator = str(raw or "").strip()
            if not locator or locator in seen:
                continue
            parsed = urlsplit(locator)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                continue
            seen.add(locator)
            leads.append(
                ArtifactLead(
                    root_id=self.root_id,
                    provider_native_id=f"{node.provider_native_id}:content:{index}",
                    locator=locator,
                    persistent_id=node.provider_native_id,
                )
            )
        return tuple(leads)


def _title(attributes: Mapping[str, Any]) -> str:
    titles = attributes.get("titles") or []
    if isinstance(titles, list) and titles and isinstance(titles[0], Mapping):
        return str(titles[0].get("title") or "")
    return ""


def _description(attributes: Mapping[str, Any]) -> str:
    descriptions = attributes.get("descriptions") or []
    if isinstance(descriptions, list) and descriptions and isinstance(descriptions[0], Mapping):
        return str(descriptions[0].get("description") or "")
    return ""


def _relationship_id(item: Mapping[str, Any], name: str) -> str | None:
    relationships = item.get("relationships") or {}
    if not isinstance(relationships, Mapping):
        return None
    relationship = relationships.get(name) or {}
    if not isinstance(relationship, Mapping):
        return None
    data = relationship.get("data")
    if isinstance(data, Mapping):
        value = data.get("id")
        return str(value).strip() if value else None
    return None


def _content_urls(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    values: Iterable[Any]
    if isinstance(value, str):
        values = (value,)
    elif isinstance(value, Iterable):
        values = value
    else:
        return ()
    output: list[str] = []
    seen: set[str] = set()
    for raw in values:
        text = str(raw or "").strip()
        parsed = urlsplit(text)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return tuple(output)


def _creator_values(creators: Any) -> tuple[str, ...]:
    if not isinstance(creators, list):
        return ()
    values: list[str] = []
    for creator in creators:
        if not isinstance(creator, Mapping):
            continue
        name = str(creator.get("name") or "").strip()
        if name:
            values.append(name)
        identifiers = creator.get("nameIdentifiers") or []
        if isinstance(identifiers, list):
            for identifier in identifiers:
                if not isinstance(identifier, Mapping):
                    continue
                value = str(identifier.get("nameIdentifier") or "").strip()
                if value:
                    values.append(value)
    return tuple(values)


def _related_identifier_values(related: Any) -> tuple[str, ...]:
    if not isinstance(related, list):
        return ()
    values: list[str] = []
    for item in related:
        if isinstance(item, Mapping):
            value = item.get("relatedIdentifier") or item.get("identifier")
        else:
            value = item
        text = str(value or "").strip()
        if text:
            values.append(text)
    return tuple(values)


def _pivot_candidates(
    *,
    client_id: str | None,
    provider_id: str | None,
    publisher: Any,
    creators: Any,
    related_identifiers: Any,
) -> tuple[dict[str, str], ...]:
    candidates: list[tuple[str, str]] = []
    if client_id:
        candidates.append(("client", client_id))
    if provider_id:
        candidates.append(("provider", provider_id))
    publisher_text = str(publisher or "").strip()
    if publisher_text:
        candidates.append(("publisher", publisher_text))
    candidates.extend(("creator", value) for value in _creator_values(creators))
    candidates.extend(("related_identifier", value) for value in _related_identifier_values(related_identifiers))

    deduped = sorted({(kind, value) for kind, value in candidates}, key=lambda pair: (pair[0], pair[1].casefold(), pair[1]))
    return tuple({"kind": kind, "value": value} for kind, value in deduped)


def _metadata(attributes: Mapping[str, Any], item: Mapping[str, Any], doi: str) -> dict[str, Any]:
    client_id = _relationship_id(item, "client")
    provider_id = _relationship_id(item, "provider")
    publisher = attributes.get("publisher")
    creators = attributes.get("creators") or []
    related = attributes.get("relatedIdentifiers") or []
    landing_url = str(attributes.get("url") or "").strip() or f"https://doi.org/{doi}"
    return {
        "doi": doi,
        # Scheduling/research metadata only.  This key is intentionally not evidence_year.
        "publication_year": attributes.get("publicationYear"),
        "publisher": publisher,
        "client_id": client_id,
        "provider_id": provider_id,
        "creators": creators,
        "related_identifiers": related,
        "landing_url": landing_url,
        "content_urls": _content_urls(attributes.get("contentUrl")),
        "pivots": _pivot_candidates(
            client_id=client_id,
            provider_id=provider_id,
            publisher=publisher,
            creators=creators,
            related_identifiers=related,
        ),
    }
