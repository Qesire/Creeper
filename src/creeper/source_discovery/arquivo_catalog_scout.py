"""Deterministic expansion of the audited Arquivo.pt CDXJ catalog.

This executor is intentionally narrow. It accepts only the curated Arquivo.pt
catalog metasource, performs one bounded catalog fetch, parses the existing
same-origin CDXJ listing format, and returns child SourceCandidate proposals.
It never opens child resources and never grants annual evidence authority.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from urllib.request import Request, urlopen

from creeper.source_discovery.coordinator import ScoutDisposition, ScoutResult
from creeper.source_discovery.models import (
    SourceCandidate,
    SourceLevel,
    SourceState,
    canonicalize_source_entrypoint,
)
from creeper.sources.archive.catalog import parse_cdxj_catalog


AUDITED_ARQUIVO_CATALOG_URL = canonicalize_source_entrypoint(
    "https://arquivo.pt/datasets/cdxj/"
)
AUDITED_ARQUIVO_CATALOG_FAMILY = "PUBLIC_ARCHIVE_INDEX_CATALOG"
AUDITED_ARQUIVO_DISCOVERED_BY = "curated-official-seed"


@dataclass(frozen=True)
class ArquivoCatalogFetch:
    body: bytes
    status_code: int
    final_url: str


CatalogFetcher = Callable[[str, int, float], Awaitable[ArquivoCatalogFetch]]


@dataclass(frozen=True)
class ArquivoCatalogScoutPolicy:
    max_catalog_bytes: int = 5_000_000
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        if (
            not isinstance(self.max_catalog_bytes, int)
            or isinstance(self.max_catalog_bytes, bool)
            or self.max_catalog_bytes < 1
        ):
            raise ValueError("max_catalog_bytes must be a positive integer")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")


def is_audited_arquivo_catalog(candidate: SourceCandidate) -> bool:
    """Return whether the candidate is the exact curated Arquivo metasource."""
    return (
        candidate.canonical_entrypoint == AUDITED_ARQUIVO_CATALOG_URL
        and candidate.source_family == AUDITED_ARQUIVO_CATALOG_FAMILY
        and candidate.level is SourceLevel.METASOURCE
        and candidate.discovered_by == AUDITED_ARQUIVO_DISCOVERED_BY
    )


def _fetch_catalog_sync(
    url: str,
    max_bytes: int,
    timeout_seconds: float,
) -> ArquivoCatalogFetch:
    request = Request(
        url,
        headers={
            "User-Agent": "Creeper/2.1 (research; contact administrator)",
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1",
            "Accept-Encoding": "identity",
        },
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        body = response.read(max_bytes + 1)
        if len(body) > max_bytes:
            raise ValueError(
                f"Arquivo catalog exceeds max_catalog_bytes={max_bytes}"
            )
        return ArquivoCatalogFetch(
            body=body,
            status_code=int(response.status),
            final_url=str(response.geturl()),
        )


async def fetch_arquivo_catalog(
    url: str,
    max_bytes: int,
    timeout_seconds: float,
) -> ArquivoCatalogFetch:
    """Fetch one bounded catalog document without blocking the async coordinator."""
    return await asyncio.to_thread(
        _fetch_catalog_sync,
        url,
        max_bytes,
        timeout_seconds,
    )


class ArquivoCatalogScoutExecutor:
    """Expand the audited Arquivo catalog into direct CDXJ source candidates."""

    def __init__(
        self,
        *,
        policy: ArquivoCatalogScoutPolicy | None = None,
        fetcher: CatalogFetcher | None = None,
    ) -> None:
        self.policy = policy or ArquivoCatalogScoutPolicy()
        self.fetcher = fetcher or fetch_arquivo_catalog

    @staticmethod
    def _child(
        parent: SourceCandidate,
        *,
        entry_url: str,
    ) -> SourceCandidate:
        return SourceCandidate(
            canonical_entrypoint=entry_url,
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            discovered_by=f"arquivo-catalog:{parent.source_key}",
            discovery_strategy="DETERMINISTIC_AUDITED_CATALOG_EXPANSION",
            temporal_semantics_prior=1.0,
            enumerability_prior=1.0,
            direct_evidence_prior=1.0,
            baseline_overlap_prior=0.5,
            access_cost_prior=0.2,
            adapter_cost_prior=0.2,
            confidence=1.0,
            state=SourceState.DISCOVERED,
        )

    async def __call__(self, candidate: SourceCandidate) -> ScoutResult:
        if not is_audited_arquivo_catalog(candidate):
            raise ValueError(
                "ArquivoCatalogScoutExecutor only accepts the audited Arquivo catalog"
            )

        fetched = await self.fetcher(
            AUDITED_ARQUIVO_CATALOG_URL,
            self.policy.max_catalog_bytes,
            self.policy.timeout_seconds,
        )
        if not 200 <= fetched.status_code <= 299:
            raise RuntimeError(
                f"Arquivo catalog returned HTTP {fetched.status_code}"
            )
        if (
            canonicalize_source_entrypoint(fetched.final_url)
            != AUDITED_ARQUIVO_CATALOG_URL
        ):
            raise RuntimeError(
                "Arquivo catalog redirected away from the audited catalog URL"
            )

        html = fetched.body.decode("utf-8", errors="replace")
        entries = parse_cdxj_catalog(
            html,
            base_url=AUDITED_ARQUIVO_CATALOG_URL,
        )
        children = tuple(
            self._child(candidate, entry_url=entry.url)
            for entry in entries
        )

        return ScoutResult(
            ScoutDisposition.HOLD,
            reason=(
                f"audited Arquivo catalog yielded {len(children)} direct CDXJ children"
            ),
            discovered_candidates=children,
            edge_relation="catalog_enumerates_cdxj",
        )
