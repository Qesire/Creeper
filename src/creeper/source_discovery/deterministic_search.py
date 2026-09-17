"""Deterministic residual-search public facade with fail-closed provider schemas.

The provider implementation is retained verbatim in ``deterministic_search_core``.
This facade strengthens only two protocol boundaries:

* HTTP 2xx is not equivalent to a successful search unless the documented
  result container is structurally present.  A WAF page, API drift, or partial
  JSON object must fail the whole finite query variant rather than masquerade as
  a legitimate zero-result response.
* configured provider names must be unique, so one physical provider cannot be
  counted twice toward complete residual coverage.
"""

from __future__ import annotations

from collections.abc import Callable
from urllib.parse import urlsplit

import httpx

from creeper.source_discovery import deterministic_search_core as _core

for _name in dir(_core):
    if not _name.startswith("__"):
        globals()[_name] = getattr(_core, _name)


_ResponseValidator = Callable[[httpx.Response], None]


class _SchemaValidatedClient:
    """Delegate ``get`` while validating successful response envelopes."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        validator: _ResponseValidator,
    ) -> None:
        self._client = client
        self._validator = validator

    async def get(self, *args, **kwargs) -> httpx.Response:
        response = await self._client.get(*args, **kwargs)
        if 200 <= response.status_code < 300:
            self._validator(response)
        return response


def _schema_error(provider: str, detail: str) -> RuntimeError:
    return RuntimeError(f"{provider} response schema incomplete: {detail}")


def _validate_datacite(response: httpx.Response) -> None:
    payload = response.json()
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise _schema_error("DataCite", "expected top-level data array")


def _validate_zenodo(response: httpx.Response) -> None:
    payload = response.json()
    if isinstance(payload, list):
        return
    if isinstance(payload, dict):
        hits = payload.get("hits")
        if isinstance(hits, dict) and isinstance(hits.get("hits"), list):
            return
        if isinstance(payload.get("data"), list):
            return
    raise _schema_error(
        "Zenodo",
        "expected record array, hits.hits array, or data array",
    )


def _validate_dataverse(response: httpx.Response) -> None:
    payload = response.json()
    if not isinstance(payload, dict):
        raise _schema_error("Harvard Dataverse", "expected top-level object")
    data = payload.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("items"), list):
        raise _schema_error("Harvard Dataverse", "expected data.items array")


def _ia_validator(
    *,
    search_endpoint: str,
    metadata_endpoint: str,
) -> _ResponseValidator:
    search_path = urlsplit(search_endpoint).path.rstrip("/") or "/"
    metadata_path = urlsplit(metadata_endpoint).path.rstrip("/")

    def validate(response: httpx.Response) -> None:
        payload = response.json()
        path = response.request.url.path.rstrip("/") or "/"
        if path == search_path:
            if not isinstance(payload, dict):
                raise _schema_error("Internet Archive", "expected search object")
            envelope = payload.get("response")
            if not isinstance(envelope, dict) or not isinstance(
                envelope.get("docs"), list
            ):
                raise _schema_error(
                    "Internet Archive",
                    "expected response.docs array",
                )
            return
        if metadata_path and path.startswith(metadata_path + "/"):
            if not isinstance(payload, dict) or not isinstance(
                payload.get("result"), list
            ):
                raise _schema_error(
                    "Internet Archive",
                    "expected metadata result array",
                )
            return
        raise _schema_error(
            "Internet Archive",
            f"unexpected provider response path {path!r}",
        )

    return validate


class DataCiteSearchProvider(_core.DataCiteSearchProvider):
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        endpoint: str = "https://api.datacite.org/dois",
        timeout_seconds: float = 20.0,
    ) -> None:
        super().__init__(
            _SchemaValidatedClient(client, _validate_datacite),
            endpoint=endpoint,
            timeout_seconds=timeout_seconds,
        )


class ZenodoSearchProvider(_core.ZenodoSearchProvider):
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        endpoint: str = "https://zenodo.org/api/records",
        timeout_seconds: float = 20.0,
    ) -> None:
        super().__init__(
            _SchemaValidatedClient(client, _validate_zenodo),
            endpoint=endpoint,
            timeout_seconds=timeout_seconds,
        )


class HarvardDataverseSearchProvider(_core.HarvardDataverseSearchProvider):
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        endpoint: str = "https://dataverse.harvard.edu/api/search",
        timeout_seconds: float = 20.0,
    ) -> None:
        super().__init__(
            _SchemaValidatedClient(client, _validate_dataverse),
            endpoint=endpoint,
            timeout_seconds=timeout_seconds,
        )


class InternetArchiveSearchProvider(_core.InternetArchiveSearchProvider):
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        search_endpoint: str = "https://archive.org/advancedsearch.php",
        metadata_endpoint: str = "https://archive.org/metadata",
        download_endpoint: str = "https://archive.org/download",
        timeout_seconds: float = 20.0,
        max_items: int = 8,
        files_per_item: int = 4,
        file_slice_count: int = 128,
    ) -> None:
        validator = _ia_validator(
            search_endpoint=search_endpoint,
            metadata_endpoint=metadata_endpoint,
        )
        super().__init__(
            _SchemaValidatedClient(client, validator),
            search_endpoint=search_endpoint,
            metadata_endpoint=metadata_endpoint,
            download_endpoint=download_endpoint,
            timeout_seconds=timeout_seconds,
            max_items=max_items,
            files_per_item=files_per_item,
            file_slice_count=file_slice_count,
        )


class DeterministicSearchExecutor(_core.DeterministicSearchExecutor):
    def __init__(self, providers, *, policy=None, actor="deterministic:residual-search"):
        names: list[str] = []
        for provider in providers:
            name = getattr(provider, "name", None)
            if not isinstance(name, str) or not name.strip():
                raise ValueError("deterministic search provider name must be non-empty")
            names.append(name.strip().casefold())
        if len(names) != len(set(names)):
            raise ValueError("deterministic search provider names must be unique")
        super().__init__(tuple(providers), policy=policy, actor=actor)
