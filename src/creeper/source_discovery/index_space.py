"""Capability-aware historical-index space models.

This module is intentionally control-plane only.  It compiles an already-known
SourceCandidate into a small, auditable description of *how* Creeper may access
that source.  It does not grant evidence authority and it never performs I/O.

The important distinction is between a source URL and an index space:
Factory -> Collection -> Index -> HarvestRegion.  Existing discovery candidates
remain the durable source identity while these objects describe production work
that can later be scheduled at region granularity.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from enum import StrEnum
from pathlib import PurePosixPath
from urllib.parse import urlsplit

from creeper.source_discovery.coordinator import TriageResult
from creeper.source_discovery.models import (
    MeasurementMode,
    SourceCandidate,
    SourceLevel,
    source_origin,
)


class SourceAccessMode(StrEnum):
    """Best deterministic access mode currently proven for one source."""

    OPAQUE = "OPAQUE"
    RANGE_STREAM = "RANGE_STREAM"
    SORTED_INDEX = "SORTED_INDEX"
    QUERY_API = "QUERY_API"


class RegionKind(StrEnum):
    """Finite partition of an index that can be independently valued."""

    FULL = "FULL"
    BYTE_RANGE = "BYTE_RANGE"
    KEY_PREFIX = "KEY_PREFIX"
    YEAR = "YEAR"
    SHARD = "SHARD"


@dataclass(frozen=True)
class QueryCapabilityHints:
    """Explicitly verified query features.

    These flags are never inferred from a product name or an LLM claim.  They
    are set only by a protocol adapter/probe that has verified the endpoint.
    """

    supports_query: bool = False
    supports_prefix: bool = False
    supports_domain: bool = False
    supports_date_filter: bool = False

    def __post_init__(self) -> None:
        if (
            self.supports_prefix
            or self.supports_domain
            or self.supports_date_filter
        ) and not self.supports_query:
            raise ValueError("query sub-capabilities require supports_query")


@dataclass(frozen=True)
class SourceCapabilities:
    """Access features proven for an index/factory."""

    access_mode: SourceAccessMode
    format: str
    hierarchical: bool
    range_supported: bool
    timestamp_bearing: bool
    direct_evidence_authority: bool
    sorted_keyspace: str | None = None
    supports_query: bool = False
    supports_prefix: bool = False
    supports_domain: bool = False
    supports_date_filter: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "access_mode", SourceAccessMode(self.access_mode))
        if not self.format.strip():
            raise ValueError("source capability format is required")
        if self.sorted_keyspace is not None and not self.sorted_keyspace.strip():
            raise ValueError("sorted_keyspace must be non-empty when supplied")
        if self.access_mode is SourceAccessMode.QUERY_API and not self.supports_query:
            raise ValueError("QUERY_API access requires supports_query")
        if (
            self.supports_prefix
            or self.supports_domain
            or self.supports_date_filter
        ) and not self.supports_query:
            raise ValueError("query sub-capabilities require supports_query")

    @property
    def can_push_down(self) -> bool:
        return self.access_mode is SourceAccessMode.QUERY_API

    @property
    def can_partition_without_full_download(self) -> bool:
        return (
            self.range_supported
            or self.access_mode
            in {SourceAccessMode.SORTED_INDEX, SourceAccessMode.QUERY_API}
        )


@dataclass(frozen=True)
class SourceFactorySpec:
    factory_key: str
    canonical_root: str
    source_family: str
    capabilities: SourceCapabilities


@dataclass(frozen=True)
class SourceIndexSpec:
    index_key: str
    factory_key: str
    source_key: str
    locator: str
    capabilities: SourceCapabilities
    expected_year_from: int | None = None
    expected_year_to: int | None = None
    expected_volume: int | None = None
    content_length: int | None = None

    def __post_init__(self) -> None:
        if not self.index_key or not self.factory_key or not self.source_key:
            raise ValueError("index identity fields are required")
        if not self.locator.strip():
            raise ValueError("index locator is required")
        if (self.expected_year_from is None) != (self.expected_year_to is None):
            raise ValueError("index year bounds must be both set or both omitted")
        if (
            self.expected_year_from is not None
            and self.expected_year_to is not None
            and self.expected_year_from > self.expected_year_to
        ):
            raise ValueError("index year range is reversed")
        if self.expected_volume is not None and self.expected_volume < 0:
            raise ValueError("expected_volume must be non-negative")
        if self.content_length is not None and self.content_length < 0:
            raise ValueError("content_length must be non-negative")


@dataclass(frozen=True)
class HarvestRegion:
    """One finite unit of probe/harvest work inside an index."""

    region_key: str
    index_key: str
    kind: RegionKind
    locator: str
    parent_region_key: str | None = None
    depth: int = 0
    byte_start: int | None = None
    byte_end: int | None = None
    key_prefix: str | None = None
    year_from: int | None = None
    year_to: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", RegionKind(self.kind))
        if not self.region_key or not self.index_key or not self.locator.strip():
            raise ValueError("region identity and locator are required")
        if self.depth < 0:
            raise ValueError("region depth must be non-negative")
        if (self.byte_start is None) != (self.byte_end is None):
            raise ValueError("byte range bounds must be both set or both omitted")
        if self.byte_start is not None:
            if self.byte_start < 0 or self.byte_end is None or self.byte_end < self.byte_start:
                raise ValueError("invalid byte range")
        if self.kind is RegionKind.BYTE_RANGE and self.byte_start is None:
            raise ValueError("BYTE_RANGE region requires byte bounds")
        if self.kind is RegionKind.KEY_PREFIX and not self.key_prefix:
            raise ValueError("KEY_PREFIX region requires key_prefix")
        if (self.year_from is None) != (self.year_to is None):
            raise ValueError("region year bounds must be both set or both omitted")
        if (
            self.year_from is not None
            and self.year_to is not None
            and self.year_from > self.year_to
        ):
            raise ValueError("region year range is reversed")


@dataclass(frozen=True)
class RegionSynopsis:
    """Small summary used to value a region before full harvest."""

    region_key: str
    sampled_records: int
    unique_hosts: int
    novel_hosts: int
    observed_host_year_pairs: int
    novel_host_year_pairs: int
    novel_eed: float
    bytes_read: int
    requests: int
    measurement_mode: MeasurementMode = MeasurementMode.HOST_YEAR
    observed_year_histogram: tuple[tuple[int, int], ...] = ()
    novel_year_histogram: tuple[tuple[int, int], ...] = ()
    tld_host_year_histogram: tuple[tuple[str, int], ...] = ()
    minhash_values: tuple[int, ...] = ()
    confidence: float = 0.0
    complete: bool = False

    def __post_init__(self) -> None:
        if not self.region_key:
            raise ValueError("region_key is required")
        object.__setattr__(
            self,
            "measurement_mode",
            MeasurementMode(self.measurement_mode),
        )
        for name in (
            "sampled_records",
            "unique_hosts",
            "novel_hosts",
            "observed_host_year_pairs",
            "novel_host_year_pairs",
            "bytes_read",
            "requests",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.novel_hosts > self.unique_hosts:
            raise ValueError("novel_hosts cannot exceed unique_hosts")
        if self.novel_host_year_pairs > self.observed_host_year_pairs:
            raise ValueError("novel host-year pairs cannot exceed observed pairs")
        if not math.isfinite(self.novel_eed) or self.novel_eed < 0:
            raise ValueError("novel_eed must be finite and non-negative")
        if not math.isfinite(self.confidence) or not 0 <= self.confidence <= 1:
            raise ValueError("confidence must be within [0, 1]")
        for histogram in (
            self.observed_year_histogram,
            self.novel_year_histogram,
        ):
            if any(year < 1000 or count < 0 for year, count in histogram):
                raise ValueError("invalid year histogram")
        if any(not tld or count < 0 for tld, count in self.tld_host_year_histogram):
            raise ValueError("invalid TLD histogram")
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in self.minhash_values
        ):
            raise ValueError("minhash values must be non-negative integers")

    @property
    def observed_count_for_value(self) -> int:
        if self.measurement_mode is MeasurementMode.HOST_YEAR:
            return self.observed_host_year_pairs
        return self.unique_hosts

    @property
    def novel_count_for_value(self) -> int:
        if self.measurement_mode is MeasurementMode.HOST_YEAR:
            return self.novel_host_year_pairs
        return self.novel_hosts

    @property
    def novel_fraction(self) -> float:
        observed = self.observed_count_for_value
        if observed <= 0:
            return 0.0
        return self.novel_count_for_value / observed

    @property
    def novel_eed_per_byte(self) -> float:
        if self.bytes_read <= 0:
            return 0.0
        return self.novel_eed / self.bytes_read

    @property
    def novel_eed_per_request(self) -> float:
        if self.requests <= 0:
            return 0.0
        return self.novel_eed / self.requests


@dataclass(frozen=True)
class CompiledIndexSpace:
    factory: SourceFactorySpec
    index: SourceIndexSpec
    root_region: HarvestRegion


def _stable_key(prefix: str, *parts: object) -> str:
    payload = "\x00".join(str(part) for part in parts).encode("utf-8")
    return f"{prefix}:" + hashlib.sha256(payload).hexdigest()


def _source_format(entrypoint: str) -> tuple[str, bool, bool]:
    """Return format, timestamp-bearing, sorted-index semantics."""

    name = PurePosixPath(urlsplit(entrypoint).path.lower()).name
    if name.endswith(".cdxj.gz") or name.endswith(".cdxj"):
        return "CDXJ", True, True
    if name.endswith(".cdx.gz") or name.endswith(".cdx"):
        return "CDX", True, True
    if name.endswith((".warc.gz", ".warc", ".arc.gz", ".arc")):
        return "WARC_ARC", True, False
    if name.endswith((".jsonl.gz", ".jsonl", ".ndjson.gz", ".ndjson")):
        return "JSONL", False, False
    if name.endswith((".csv.gz", ".csv", ".tsv.gz", ".tsv")):
        return "TABULAR", False, False
    if name.endswith((".txt.gz", ".txt", ".list.gz", ".list", ".urls.gz", ".urls")):
        return "TEXT", False, False
    return "UNKNOWN", False, False


def compile_candidate_index_space(
    candidate: SourceCandidate,
    *,
    triage: TriageResult | None = None,
    range_supported: bool | None = None,
    content_length: int | None = None,
    query_hints: QueryCapabilityHints | None = None,
    direct_evidence_authority: bool | None = None,
) -> CompiledIndexSpace:
    """Compile one existing discovery candidate into a P0 index-space contract.

    The compiler is deliberately conservative:
    * CDX/CDXJ ordering is recognized as SURT-like sorted index semantics.
    * HTTP Range is trusted only when triage actually observed it.
    * query pushdown exists only when a protocol adapter passes explicit hints.
    * timestamp-bearing data is not automatically granted competition evidence
      authority.  By default only the existing candidate direct-evidence prior
      can grant that flag.
    """

    hints = query_hints or QueryCapabilityHints()
    source_format, timestamp_bearing, sorted_index = _source_format(
        candidate.canonical_entrypoint
    )
    observed_range = (
        range_supported
        if range_supported is not None
        else (triage.range_supported if triage is not None else None)
    )
    range_supported = bool(observed_range)
    observed_length = (
        content_length
        if content_length is not None
        else (triage.content_length if triage is not None else None)
    )
    if observed_length is not None and observed_length < 0:
        raise ValueError("content_length must be non-negative")
    hierarchical = (
        candidate.level in {SourceLevel.COLLECTION, SourceLevel.METASOURCE}
        or candidate.source_family in {"RESOURCE_CATALOG", "RESOURCE_DIRECTORY"}
    )

    if hints.supports_query:
        access_mode = SourceAccessMode.QUERY_API
    elif sorted_index:
        access_mode = SourceAccessMode.SORTED_INDEX
    elif range_supported:
        access_mode = SourceAccessMode.RANGE_STREAM
    else:
        access_mode = SourceAccessMode.OPAQUE

    authority = (
        candidate.direct_evidence_prior >= 0.5
        if direct_evidence_authority is None
        else bool(direct_evidence_authority)
    )
    capabilities = SourceCapabilities(
        access_mode=access_mode,
        format=source_format,
        hierarchical=hierarchical,
        range_supported=range_supported,
        timestamp_bearing=timestamp_bearing,
        direct_evidence_authority=authority,
        sorted_keyspace=("SURT_URLKEY" if sorted_index else None),
        supports_query=hints.supports_query,
        supports_prefix=hints.supports_prefix,
        supports_domain=hints.supports_domain,
        supports_date_filter=hints.supports_date_filter,
    )

    origin = source_origin(candidate.canonical_entrypoint)
    factory = SourceFactorySpec(
        factory_key=_stable_key("factory", origin),
        canonical_root=origin + "/",
        source_family=candidate.source_family,
        capabilities=capabilities,
    )
    index = SourceIndexSpec(
        index_key=_stable_key("index", candidate.source_key),
        factory_key=factory.factory_key,
        source_key=candidate.source_key,
        locator=candidate.canonical_entrypoint,
        capabilities=capabilities,
        expected_year_from=candidate.expected_year_from,
        expected_year_to=candidate.expected_year_to,
        expected_volume=candidate.expected_volume,
        content_length=observed_length,
    )
    root_region = HarvestRegion(
        region_key=_stable_key("region", index.index_key, RegionKind.FULL.value),
        index_key=index.index_key,
        kind=RegionKind.FULL,
        locator=index.locator,
        byte_start=(0 if observed_length else None),
        byte_end=(observed_length - 1 if observed_length else None),
    )
    return CompiledIndexSpace(
        factory=factory,
        index=index,
        root_region=root_region,
    )


def child_region(
    parent: HarvestRegion,
    *,
    kind: RegionKind,
    byte_start: int | None = None,
    byte_end: int | None = None,
    key_prefix: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    locator: str | None = None,
) -> HarvestRegion:
    """Create a deterministic child-region identity for adaptive refinement."""

    kind = RegionKind(kind)
    target_locator = locator or parent.locator
    region_key = _stable_key(
        "region",
        parent.index_key,
        parent.region_key,
        kind.value,
        target_locator,
        byte_start,
        byte_end,
        key_prefix,
        year_from,
        year_to,
    )
    return HarvestRegion(
        region_key=region_key,
        index_key=parent.index_key,
        kind=kind,
        locator=target_locator,
        parent_region_key=parent.region_key,
        depth=parent.depth + 1,
        byte_start=byte_start,
        byte_end=byte_end,
        key_prefix=key_prefix,
        year_from=year_from,
        year_to=year_to,
    )
