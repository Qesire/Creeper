"""Deterministic high-reservoir promotion from bounded link-discovery spools.

The Scrapy sidecar may see many ordinary HTML URLs. Those URLs remain inside
Scrapy's frontier and must not become Creeper source-graph nodes. This module
promotes only strongly resource-like targets, with bounded aggregation across
referring pages.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from urllib.parse import urlsplit

from creeper.source_discovery.models import SourceCandidate, SourceLevel
from creeper.source_discovery.scrapy_sidecar import ScrapyLinkDiscovery


_BULK_SUFFIXES = (
    ".cdx",
    ".cdx.gz",
    ".cdxj",
    ".warc",
    ".warc.gz",
    ".arc",
    ".arc.gz",
    ".jsonl",
    ".jsonl.gz",
    ".csv",
    ".csv.gz",
    ".tsv",
    ".tsv.gz",
    ".xml.gz",
    ".txt.gz",
    ".tar",
    ".tar.gz",
    ".tgz",
    ".zip",
    ".7z",
    ".bz2",
    ".xz",
)
_RESOURCE_TERMS = frozenset(
    {
        "archive",
        "archives",
        "catalog",
        "catalogue",
        "collection",
        "collections",
        "dataset",
        "datasets",
        "directory",
        "directories",
        "dump",
        "export",
        "index",
        "indexes",
        "indices",
        "links",
        "resources",
        "sites",
        "urls",
        "websites",
    }
)
_META_TERMS = frozenset(
    {
        "catalog",
        "catalogue",
        "collections",
        "datasets",
        "registry",
        "repositories",
        "resources",
    }
)
_TOKEN_RE = re.compile(r"[a-z0-9]+")


def _tokens(value: str) -> frozenset[str]:
    return frozenset(_TOKEN_RE.findall(value.lower()))


def _is_bulk(url: str) -> bool:
    path = urlsplit(url).path.lower()
    return any(path.endswith(suffix) for suffix in _BULK_SUFFIXES)


def _resource_signal(link: ScrapyLinkDiscovery) -> tuple[int, int]:
    parsed = urlsplit(link.discovered_url)
    path_tokens = _tokens(parsed.path)
    anchor_tokens = _tokens(link.anchor_text)
    query_tokens = _tokens(parsed.query)
    all_tokens = path_tokens | anchor_tokens | query_tokens
    return len(all_tokens & _RESOURCE_TERMS), len(all_tokens & _META_TERMS)


@dataclass(frozen=True)
class PromotionPolicy:
    max_promotions: int = 64
    min_distinct_referrers: int = 2
    max_input_links: int = 50_000
    max_query_length: int = 512

    def __post_init__(self) -> None:
        if self.max_promotions < 1:
            raise ValueError("max_promotions must be positive")
        if self.min_distinct_referrers < 1:
            raise ValueError("min_distinct_referrers must be positive")
        if self.max_input_links < 1:
            raise ValueError("max_input_links must be positive")
        if self.max_query_length < 0:
            raise ValueError("max_query_length must be non-negative")


@dataclass(frozen=True)
class PromotionEvidence:
    url: str
    distinct_referrers: int
    observations: int
    bulk_artifact: bool
    resource_hits: int
    meta_hits: int
    score: int


@dataclass(frozen=True)
class PromotedSource:
    candidate: SourceCandidate
    evidence: PromotionEvidence


@dataclass
class _Aggregate:
    referrers: set[str]
    observations: int = 0
    resource_hits: int = 0
    meta_hits: int = 0
    bulk_artifact: bool = False


class LinkPromotionAccumulator:
    """Bounded deterministic reducer from URL observations to source proposals."""

    def __init__(self, *, policy: PromotionPolicy | None = None) -> None:
        self.policy = policy or PromotionPolicy()
        self._seen = 0
        self._aggregates: dict[str, _Aggregate] = {}

    @property
    def input_links(self) -> int:
        return self._seen

    def add(self, link: ScrapyLinkDiscovery) -> None:
        if self._seen >= self.policy.max_input_links:
            return
        self._seen += 1

        # Same-site HTML navigation belongs to Scrapy's URL frontier, not the
        # Creeper source graph. Same-site bulk artifacts are still valuable.
        bulk = _is_bulk(link.discovered_url)
        if link.same_site and not bulk:
            return

        parsed = urlsplit(link.discovered_url)
        if len(parsed.query) > self.policy.max_query_length:
            return
        resource_hits, meta_hits = _resource_signal(link)
        if not bulk and resource_hits == 0:
            return

        aggregate = self._aggregates.get(link.discovered_url)
        if aggregate is None:
            aggregate = _Aggregate(referrers=set())
            self._aggregates[link.discovered_url] = aggregate
        aggregate.referrers.add(link.page_url)
        aggregate.observations += 1
        aggregate.resource_hits = max(aggregate.resource_hits, resource_hits)
        aggregate.meta_hits = max(aggregate.meta_hits, meta_hits)
        aggregate.bulk_artifact = aggregate.bulk_artifact or bulk

    def _score(self, aggregate: _Aggregate) -> int:
        # Integer score keeps ordering reproducible across Python/platforms.
        return (
            (100 if aggregate.bulk_artifact else 0)
            + min(20, len(aggregate.referrers) * 4)
            + min(20, aggregate.resource_hits * 5)
            + min(15, aggregate.meta_hits * 5)
            + min(10, aggregate.observations)
        )

    def promoted(
        self,
        *,
        discovered_by: str = "scrapy_sidecar",
        discovery_strategy: str = "DETERMINISTIC_LINK_EXPANSION",
    ) -> list[PromotedSource]:
        ranked: list[tuple[int, str, _Aggregate]] = []
        for url, aggregate in self._aggregates.items():
            referrers = len(aggregate.referrers)
            # Bulk artifacts are strong resource identities and may pass from a
            # single link. Generic catalog/directory pages require corroboration
            # from multiple distinct pages to avoid promoting navigational noise.
            if not aggregate.bulk_artifact and referrers < self.policy.min_distinct_referrers:
                continue
            ranked.append((self._score(aggregate), url, aggregate))

        ranked.sort(key=lambda item: (-item[0], item[1]))
        result: list[PromotedSource] = []
        for score, url, aggregate in ranked[: self.policy.max_promotions]:
            if aggregate.bulk_artifact:
                family = "BULK_ARTIFACT"
                level = SourceLevel.SOURCE
                enumerability = 0.95
                temporal = 0.45
            elif aggregate.meta_hits >= 2:
                family = "RESOURCE_CATALOG"
                level = SourceLevel.METASOURCE
                enumerability = 0.9
                temporal = 0.35
            else:
                family = "RESOURCE_DIRECTORY"
                level = SourceLevel.COLLECTION
                enumerability = 0.75
                temporal = 0.3

            # These are deterministic discovery priors only. No direct-evidence
            # or low-baseline-overlap claim is inferred from link structure.
            confidence = min(
                0.95,
                0.45
                + (0.25 if aggregate.bulk_artifact else 0.0)
                + min(0.2, len(aggregate.referrers) * 0.04)
                + min(0.1, aggregate.resource_hits * 0.025),
            )
            candidate = SourceCandidate(
                canonical_entrypoint=url,
                source_family=family,
                level=level,
                discovered_by=discovered_by,
                discovery_strategy=discovery_strategy,
                temporal_semantics_prior=temporal,
                enumerability_prior=enumerability,
                direct_evidence_prior=0.0,
                baseline_overlap_prior=0.5,
                access_cost_prior=0.5 if aggregate.bulk_artifact else 1.0,
                adapter_cost_prior=0.75 if aggregate.bulk_artifact else 1.0,
                confidence=confidence,
            )
            evidence = PromotionEvidence(
                url=url,
                distinct_referrers=len(aggregate.referrers),
                observations=aggregate.observations,
                bulk_artifact=aggregate.bulk_artifact,
                resource_hits=aggregate.resource_hits,
                meta_hits=aggregate.meta_hits,
                score=score,
            )
            result.append(PromotedSource(candidate, evidence))
        return result
