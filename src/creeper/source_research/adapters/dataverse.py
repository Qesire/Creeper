"""Dataverse Search API root with direct file ArtifactLead emission."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import quote, urlsplit

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
    response_bytes_len,
    response_json,
    retry_delay_seconds,
    status_code,
)

_RESERVED_FILTERS = frozenset({"q", "type", "per_page", "start", "show_api_urls", "show_entity_ids"})


class DataverseAdapter:
    def __init__(
        self,
        instance: str,
        *,
        transport: JsonTransport,
        token: str | None = None,
    ) -> None:
        self.instance = instance.rstrip("/")
        self.transport = transport
        self.token = token
        parsed = urlsplit(self.instance)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Dataverse instance must be an absolute http(s) URL")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("Dataverse instance URL must not contain userinfo")
        self.root_id = "dataverse:" + parsed.netloc.lower()

    @property
    def search_endpoint(self) -> str:
        return self.instance + "/api/search"

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.token:
            headers["X-Dataverse-key"] = self.token
        return headers

    async def probe_capabilities(self) -> RootCapabilityReport:
        """Probe this installation instead of assuming a global Dataverse version."""

        try:
            response = await self.transport(
                self.search_endpoint,
                {
                    "q": "*",
                    "type": "file",
                    "per_page": 1,
                    "start": 0,
                    "show_api_urls": "true",
                    "show_entity_ids": "true",
                },
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
            ensure_success(response, url=self.search_endpoint)
            payload = response_json(response)
            data = payload.get("data") or {}
            if not isinstance(data, Mapping) or not isinstance(data.get("items") or [], list):
                raise ValueError("Dataverse search response lacks data.items")
        except Exception as exc:
            return RootCapabilityReport(self.root_id, False, reason=str(exc), status_code=code)
        return RootCapabilityReport(
            self.root_id,
            True,
            ("dataset_search", "file_search", "persistent_ids", "direct_file_access", "api_urls"),
            status_code=code,
        )

    async def search(
        self,
        query: RootQuery,
        checkpoint: SearchCheckpoint | None,
    ) -> SearchPage:
        variant, filters = _query_variant(query, checkpoint)
        cp = checkpoint or SearchCheckpoint(start=0, page=1, query_variant=variant)
        if cp.page > query.max_pages:
            return SearchPage(requests=0, terminal=True)

        params = merge_native_filters(
            {
                "q": query.query_text,
                "type": variant,
                "per_page": min(1000, query.page_size),
                "start": cp.start,
                "show_api_urls": "true",
                "show_entity_ids": "true",
            },
            filters,
            reserved=_RESERVED_FILTERS,
        )
        response = await self.transport(self.search_endpoint, params, self._headers())
        if is_retryable(response):
            return SearchPage(
                next_checkpoint=cp,
                terminal=False,
                retry_after=retry_delay_seconds(response),
            )
        ensure_success(response, url=self.search_endpoint)
        payload = response_json(response)
        data = payload.get("data") or {}
        if not isinstance(data, Mapping):
            raise ValueError("Dataverse response data must be an object")
        items = data.get("items") or []
        if not isinstance(items, list):
            raise ValueError("Dataverse response data.items must be a list")

        hits: list[SearchHit] = []
        leads: list[ArtifactLead] = []
        seen: set[tuple[str, str]] = set()
        for item in items:
            if not isinstance(item, Mapping):
                continue
            parsed = _parse_item(self, query.query_id, item)
            if parsed is None:
                continue
            hit, lead = parsed
            dedupe_key = (hit.provider_type, hit.provider_native_id)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            hits.append(hit)
            if lead is not None:
                leads.append(lead)

        total = _int(data.get("total_count"))
        count = len(items)
        next_start = cp.start + count
        page_size = int(params["per_page"])
        provider_exhausted = count == 0 or count < page_size or (total is not None and next_start >= total)
        next_cp: SearchCheckpoint | None = None
        if not provider_exhausted:
            next_cp = SearchCheckpoint(
                start=next_start,
                page=cp.page + 1,
                query_variant=variant,
            )
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
        if node.provider_type != "FILE":
            # Dataset hits stay research nodes until a deterministic file query resolves them.
            return ()
        locator = str(node.metadata.get("download_url") or "").strip()
        if not locator:
            return ()
        return (
            ArtifactLead(
                root_id=self.root_id,
                provider_native_id=f"file:{node.provider_native_id}",
                locator=locator,
                content_type=str(node.metadata.get("content_type") or ""),
                size=_int(node.metadata.get("size")),
                checksum=_checksum(node.metadata.get("checksum") or node.metadata.get("md5")),
                persistent_id=_optional_text(node.metadata.get("persistent_id")),
                parent_persistent_id=_optional_text(node.metadata.get("dataset_persistent_id")),
            ),
        )


def _query_variant(
    query: RootQuery,
    checkpoint: SearchCheckpoint | None,
) -> tuple[str, dict[str, str]]:
    filters = dict(query.native_filters)
    filter_variant = filters.pop("type", None)
    variant = (checkpoint.query_variant if checkpoint else None) or filter_variant or "dataset"
    variant = str(variant).casefold()
    if variant not in {"dataset", "file"}:
        raise ValueError("Dataverse query variant must be 'dataset' or 'file'")
    return variant, filters


def _parse_item(
    adapter: DataverseAdapter,
    query_id: str,
    item: Mapping[str, Any],
) -> tuple[SearchHit, ArtifactLead | None] | None:
    item_type = str(item.get("type") or "").casefold()
    if item_type == "file":
        return _parse_file_item(adapter, query_id, item)
    if item_type == "dataset":
        return _parse_dataset_item(adapter, query_id, item)
    return None


def _parse_file_item(
    adapter: DataverseAdapter,
    query_id: str,
    item: Mapping[str, Any],
) -> tuple[SearchHit, ArtifactLead | None] | None:
    nested = item.get("dataFile") or {}
    if not isinstance(nested, Mapping):
        nested = {}
    file_id = _first_text(
        nested.get("id"),
        item.get("file_id"),
        item.get("entity_id"),
        item.get("id"),
    )
    persistent_id = _first_text(
        nested.get("persistentId"),
        item.get("file_persistent_id"),
        item.get("persistent_id"),
    )
    dataset_pid = _first_text(
        nested.get("datasetPersistentId"),
        item.get("dataset_persistent_id"),
        item.get("datasetPersistentId"),
    )
    native_id = file_id or persistent_id
    if not native_id:
        return None
    download_url = _file_download_url(adapter.instance, file_id=file_id, persistent_id=persistent_id)
    if not download_url:
        api_url = _first_text(item.get("api_url"), item.get("url"))
        if api_url and urlsplit(api_url).scheme in {"http", "https"}:
            download_url = api_url
    size = _first_int(
        nested.get("filesize"),
        nested.get("sizeInBytes"),
        item.get("size_in_bytes"),
        item.get("filesize"),
        item.get("size"),
    )
    checksum = _checksum(
        nested.get("checksum")
        or nested.get("md5")
        or item.get("checksum")
        or item.get("md5")
    )
    content_type = _first_text(
        nested.get("contentType"),
        item.get("file_content_type"),
        item.get("content_type"),
    ) or ""
    title = _first_text(nested.get("filename"), item.get("name"), item.get("filename")) or ""
    metadata = {
        "file_id": file_id,
        "persistent_id": persistent_id,
        "dataset_persistent_id": dataset_pid,
        "size": size,
        "checksum": checksum,
        "md5": _first_text(nested.get("md5"), item.get("md5")),
        "content_type": content_type,
        "download_url": download_url,
    }
    hit = SearchHit(
        root_id=adapter.root_id,
        query_id=query_id,
        provider_native_id=native_id,
        provider_url=download_url or _first_text(item.get("url"), item.get("api_url")) or adapter.instance,
        provider_type="FILE",
        title=title,
        metadata=metadata,
    )
    lead = None
    if download_url:
        lead = ArtifactLead(
            root_id=adapter.root_id,
            provider_native_id=f"file:{native_id}",
            locator=download_url,
            content_type=content_type,
            size=size,
            checksum=checksum,
            persistent_id=persistent_id,
            parent_persistent_id=dataset_pid,
        )
    return hit, lead


def _parse_dataset_item(
    adapter: DataverseAdapter,
    query_id: str,
    item: Mapping[str, Any],
) -> tuple[SearchHit, None] | None:
    persistent_id = _first_text(
        item.get("global_id"),
        item.get("globalId"),
        item.get("persistent_id"),
        item.get("dataset_persistent_id"),
    )
    entity_id = _first_text(item.get("entity_id"), item.get("id"))
    native_id = persistent_id or entity_id
    if not native_id:
        return None
    url = _first_text(item.get("url"), item.get("api_url")) or adapter.instance
    hit = SearchHit(
        root_id=adapter.root_id,
        query_id=query_id,
        provider_native_id=native_id,
        provider_url=url,
        provider_type="DATASET",
        title=_first_text(item.get("name"), item.get("title")) or "",
        description=_first_text(item.get("description")) or "",
        metadata={
            "persistent_id": persistent_id,
            # Research metadata only; never annual Web evidence.
            "publication_date": item.get("published_at") or item.get("publication_date"),
            "api_url": item.get("api_url"),
        },
    )
    return hit, None


def _file_download_url(
    instance: str,
    *,
    file_id: str | None,
    persistent_id: str | None,
) -> str | None:
    if file_id:
        return f"{instance}/api/access/datafile/{quote(file_id, safe='')}"
    if persistent_id:
        return f"{instance}/api/access/datafile/:persistentId?persistentId={quote(persistent_id, safe='')}"
    return None


def _checksum(value: Any) -> str | None:
    if isinstance(value, Mapping):
        algorithm = _first_text(value.get("type"), value.get("algorithm"))
        digest = _first_text(value.get("value"), value.get("digest"))
        if digest:
            return f"{algorithm.lower()}:{digest}" if algorithm else digest
        return None
    text = _optional_text(value)
    return text


def _first_text(*values: Any) -> str | None:
    for value in values:
        text = _optional_text(value)
        if text:
            return text
    return None


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _first_int(*values: Any) -> int | None:
    for value in values:
        parsed = _int(value)
        if parsed is not None:
            return parsed
    return None
