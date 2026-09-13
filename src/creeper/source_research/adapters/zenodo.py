"""Zenodo REST root with bounded deterministic pagination and direct file leads."""

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

API = "https://zenodo.org/api/records/"
_RESERVED_FILTERS = frozenset({"q", "page", "size", "all_versions"})
_AU_SUFFIXES = ("-auk.tar.gz", "-parquet.tar.gz")


def recognize_archives_unleashed(
    title: str,
    filenames: Iterable[str],
    schema_text: str = "",
) -> bool:
    """Return scheduling knowledge only; this is never evidence authority."""

    title_text = str(title).casefold()
    names = tuple(str(name).casefold() for name in filenames)
    schema = str(schema_text).casefold()
    derivative_file = any(name.endswith(_AU_SUFFIXES) for name in names)
    schema_signature = all(token in schema for token in ("crawl_date", "src", "dest", "anchor"))
    explicit_title = "web archive collection derivatives" in title_text
    return derivative_file and (explicit_title or schema_signature)


class ZenodoAdapter:
    root_id = "zenodo"

    def __init__(
        self,
        *,
        transport: JsonTransport,
        endpoint: str = API,
        token: str | None = None,
    ) -> None:
        self.transport = transport
        self.endpoint = endpoint if endpoint.endswith("/") else endpoint + "/"
        self.token = token
        parsed = urlsplit(self.endpoint)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Zenodo endpoint must be an absolute http(s) URL")

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    async def probe_capabilities(self) -> RootCapabilityReport:
        try:
            response = await self.transport(
                self.endpoint,
                {"page": 1, "size": 1, "all_versions": "true"},
                self._headers(),
            )
        except Exception as exc:
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
            return RootCapabilityReport(self.root_id, False, reason=str(exc), status_code=code)
        return RootCapabilityReport(
            self.root_id,
            True,
            ("records", "files", "versions", "concept_identity"),
            status_code=code,
        )

    async def search(
        self,
        query: RootQuery,
        checkpoint: SearchCheckpoint | None,
    ) -> SearchPage:
        cp = checkpoint or SearchCheckpoint(page=1)
        if cp.page > query.max_pages:
            return SearchPage(requests=0, terminal=True)

        url = safe_next_url(self.endpoint, cp.next_url) if cp.next_url else self.endpoint
        if cp.next_url:
            params: dict[str, Any] = {}
        else:
            page_size = min(query.page_size, 100 if self.token else 25)
            params = merge_native_filters(
                {
                    "q": query.query_text,
                    "page": cp.page,
                    "size": page_size,
                    "all_versions": "true",
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
        hit_container = payload.get("hits") or {}
        if not isinstance(hit_container, Mapping):
            raise ValueError("Zenodo response hits must be an object")
        rows = hit_container.get("hits") or []
        if not isinstance(rows, list):
            raise ValueError("Zenodo response hits.hits must be a list")

        hits: list[SearchHit] = []
        leads: list[ArtifactLead] = []
        seen_records: set[str] = set()
        seen_leads: set[tuple[str, str]] = set()
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            record_id = str(row.get("id") or "").strip()
            if not record_id or record_id in seen_records:
                continue
            seen_records.add(record_id)
            metadata = _record_metadata(row)
            hit = SearchHit(
                root_id=self.root_id,
                query_id=query.query_id,
                provider_native_id=record_id,
                provider_url=_record_url(row, record_id),
                provider_type="RECORD",
                title=str(metadata.get("title") or ""),
                description=str(metadata.get("description") or ""),
                metadata=metadata,
            )
            hits.append(hit)
            for lead in _artifact_leads(self.root_id, hit):
                key = (lead.provider_native_id, lead.locator)
                if key not in seen_leads:
                    seen_leads.add(key)
                    leads.append(lead)

        next_url = _next_link(payload, self.endpoint)
        next_cp: SearchCheckpoint | None = None
        if next_url:
            next_cp = SearchCheckpoint(next_url=next_url, page=cp.page + 1)
        else:
            total = _total_value(hit_container.get("total"))
            page_size = min(query.page_size, 100 if self.token else 25)
            if rows and total is not None and cp.page * page_size < total:
                next_cp = SearchCheckpoint(page=cp.page + 1)
        next_cp = checkpoint_with_page_budget(query, current_page=cp.page, checkpoint=next_cp)

        return SearchPage(
            hits=tuple(hits),
            artifact_leads=tuple(leads),
            next_checkpoint=next_cp,
            terminal=next_cp is None,
            bytes_read=response_bytes_len(response, payload),
        )

    async def resolve(self, node: SearchHit) -> tuple[ArtifactLead, ...]:
        if node.root_id != self.root_id:
            raise ValueError("cannot resolve a node from another root")
        return _artifact_leads(self.root_id, node)


def _next_link(payload: Mapping[str, Any], endpoint: str) -> str | None:
    links = payload.get("links") or {}
    if not isinstance(links, Mapping):
        return None
    raw = links.get("next")
    if isinstance(raw, Mapping):
        raw = raw.get("href")
    return safe_next_url(endpoint, str(raw)) if raw else None


def _total_value(value: Any) -> int | None:
    if isinstance(value, Mapping):
        value = value.get("value")
    try:
        total = int(value)
    except (TypeError, ValueError):
        return None
    return max(0, total)


def _record_url(row: Mapping[str, Any], record_id: str) -> str:
    links = row.get("links") or {}
    if isinstance(links, Mapping):
        for key in ("html", "self_html"):
            value = str(links.get(key) or "").strip()
            if value:
                return value
    return f"https://zenodo.org/records/{record_id}"


def _record_doi(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    candidates: list[Any] = [metadata.get("doi"), row.get("doi")]
    pids = row.get("pids") or {}
    if isinstance(pids, Mapping):
        doi = pids.get("doi") or {}
        if isinstance(doi, Mapping):
            candidates.append(doi.get("identifier"))
    for value in candidates:
        normalized = normalize_doi(value)
        if normalized:
            return normalized
    return None


def _concept_doi(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    for value in (row.get("conceptdoi"), metadata.get("conceptdoi")):
        normalized = normalize_doi(value)
        if normalized:
            return normalized
    return None


def _iter_files(row: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    raw = row.get("files") or []
    values: list[Mapping[str, Any]] = []
    if isinstance(raw, list):
        values = [value for value in raw if isinstance(value, Mapping)]
    elif isinstance(raw, Mapping):
        entries = raw.get("entries")
        if isinstance(entries, Mapping):
            for key, value in entries.items():
                if not isinstance(value, Mapping):
                    continue
                item = dict(value)
                item.setdefault("key", key)
                values.append(item)
        elif isinstance(entries, list):
            values = [value for value in entries if isinstance(value, Mapping)]
    normalized: list[dict[str, Any]] = []
    for value in values:
        links = value.get("links") or {}
        locator = ""
        if isinstance(links, Mapping):
            for key in ("content", "download", "self"):
                locator = str(links.get(key) or "").strip()
                if locator:
                    break
        if not locator:
            locator = str(value.get("link") or "").strip()
        key = str(value.get("key") or value.get("filename") or value.get("id") or "").strip()
        normalized.append(
            {
                "key": key,
                "locator": locator,
                "size": _int(value.get("size") if value.get("size") is not None else value.get("filesize")),
                "checksum": _checksum(value.get("checksum")),
                "content_type": str(value.get("mimetype") or value.get("type") or ""),
            }
        )
    return tuple(normalized)


def _checksum(value: Any) -> str | None:
    if isinstance(value, Mapping):
        algorithm = str(value.get("algorithm") or "").strip()
        digest = str(value.get("value") or value.get("digest") or "").strip()
        if digest:
            return f"{algorithm}:{digest}" if algorithm else digest
        return None
    text = str(value or "").strip()
    return text or None


def _record_metadata(row: Mapping[str, Any]) -> dict[str, Any]:
    metadata = row.get("metadata") or {}
    if not isinstance(metadata, Mapping):
        metadata = {}
    files = _iter_files(row)
    doi = _record_doi(row, metadata)
    concept_doi = _concept_doi(row, metadata)
    concept_record_id = str(row.get("conceptrecid") or row.get("concept_id") or "").strip() or None
    title = str(metadata.get("title") or row.get("title") or "")
    description = str(metadata.get("description") or row.get("description") or "")
    prior = recognize_archives_unleashed(
        title,
        [str(file.get("key") or "") for file in files],
        description,
    )
    return {
        "record_id": str(row.get("id") or ""),
        "concept_record_id": concept_record_id,
        "doi": doi,
        "concept_doi": concept_doi,
        "version": metadata.get("version") or row.get("version"),
        "title": title,
        "description": description,
        "creators": metadata.get("creators") or [],
        "files": files,
        # Scheduling knowledge only; never evidence authority.
        "family_prior": prior,
        "scheduling_prior": {"archives_unleashed": prior},
    }


def _artifact_leads(root_id: str, node: SearchHit) -> tuple[ArtifactLead, ...]:
    files = node.metadata.get("files") or ()
    record_doi = normalize_doi(node.metadata.get("doi")) or None
    concept_doi = normalize_doi(node.metadata.get("concept_doi")) or None
    output: list[ArtifactLead] = []
    seen: set[tuple[str, str]] = set()
    for index, file in enumerate(files):
        if not isinstance(file, Mapping):
            continue
        locator = str(file.get("locator") or "").strip()
        parsed = urlsplit(locator)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            continue
        key = str(file.get("key") or index)
        identity = (key, locator)
        if identity in seen:
            continue
        seen.add(identity)
        output.append(
            ArtifactLead(
                root_id=root_id,
                provider_native_id=f"{node.provider_native_id}:{key}",
                locator=locator,
                content_type=str(file.get("content_type") or ""),
                size=_int(file.get("size")),
                checksum=_checksum(file.get("checksum")),
                persistent_id=record_doi,
                parent_persistent_id=concept_doi,
            )
        )
    return tuple(output)


def _int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
