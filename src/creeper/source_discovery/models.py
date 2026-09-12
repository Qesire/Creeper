"""Immutable source-discovery models and stable source identity."""

from __future__ import annotations

import hashlib
import math
import posixpath
import re
from dataclasses import dataclass
from enum import StrEnum
from urllib.parse import SplitResult, urlsplit, urlunsplit


class SourceLevel(StrEnum):
    """Source graph level; larger-scale enumerators are discovered first."""

    SOURCE = "SOURCE"
    COLLECTION = "COLLECTION"
    METASOURCE = "METASOURCE"


def is_common_crawl_provenance(*values: str) -> bool:
    """Return whether source provenance identifies the excluded CC corpus."""
    text = " ".join(str(value) for value in values).lower()
    compact = re.sub(r"[^a-z0-9]+", "", text)
    return "commoncrawl" in compact


class SourceState(StrEnum):
    DISCOVERED = "DISCOVERED"
    TRIAGED = "TRIAGED"
    SCOUT_READY = "SCOUT_READY"
    SCOUTING = "SCOUTING"
    WARM = "WARM"
    ACTIVE = "ACTIVE"
    HOLD = "HOLD"
    REJECTED = "REJECTED"
    EXHAUSTED = "EXHAUSTED"


class SuppressionScope(StrEnum):
    SOURCE = "SOURCE"
    FAMILY = "FAMILY"
    ORIGIN = "ORIGIN"


class MeasurementMode(StrEnum):
    """Granularity at which a scout can prove baseline novelty."""

    HOST_ONLY = "HOST_ONLY"
    HOST_YEAR = "HOST_YEAR"


def canonicalize_source_entrypoint(value: str) -> str:
    """Return a conservative stable identity for an HTTP(S) source resource.

    Query strings are intentionally preserved byte-for-byte and schemes are not
    merged: both can change the represented collection. Fragments are client-side
    navigation and therefore do not participate in source identity.
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError("source entrypoint must be a non-empty URL")
    parsed = urlsplit(value.strip())
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        raise ValueError("source entrypoint must use http or https")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("source entrypoint must not contain userinfo")
    if parsed.hostname is None:
        raise ValueError("source entrypoint requires a hostname")

    try:
        host = parsed.hostname.encode("idna").decode("ascii").lower()
    except UnicodeError as exc:
        raise ValueError("invalid source hostname") from exc
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("invalid source port") from exc
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
    canonical = SplitResult(scheme, host, path, parsed.query, "")
    return urlunsplit(canonical)





_DIRECT_EVIDENCE_SUFFIXES = (
    ".cdx", ".cdx.gz", ".cdxj", ".cdxj.gz"
)


def is_direct_evidence_entrypoint(value: str) -> bool:
    """Whether a source resource encodes exact capture timestamp + URL rows."""
    try:
        path = urlsplit(canonicalize_source_entrypoint(value)).path.lower()
    except ValueError:
        return False
    return path.endswith(_DIRECT_EVIDENCE_SUFFIXES)

def source_key(entrypoint: str) -> str:
    canonical = canonicalize_source_entrypoint(entrypoint)
    return "src:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def source_origin(entrypoint: str) -> str:
    parsed = urlsplit(canonicalize_source_entrypoint(entrypoint))
    return f"{parsed.scheme}://{parsed.netloc}"


@dataclass(frozen=True)
class SourceCandidate:
    canonical_entrypoint: str
    source_family: str
    level: SourceLevel
    discovered_by: str
    discovery_strategy: str
    expected_year_from: int | None = None
    expected_year_to: int | None = None
    expected_volume: int | None = None
    temporal_semantics_prior: float = 0.0
    enumerability_prior: float = 0.0
    direct_evidence_prior: float = 0.0
    baseline_overlap_prior: float = 0.5
    access_cost_prior: float = 1.0
    adapter_cost_prior: float = 1.0
    confidence: float = 0.0
    state: SourceState = SourceState.DISCOVERED

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "canonical_entrypoint",
            canonicalize_source_entrypoint(self.canonical_entrypoint),
        )
        object.__setattr__(self, "level", SourceLevel(self.level))
        object.__setattr__(self, "state", SourceState(self.state))
        if not self.source_family.strip():
            raise ValueError("source_family is required")
        if not self.discovered_by.strip() or not self.discovery_strategy.strip():
            raise ValueError("source discovery attribution is required")
        if (self.expected_year_from is None) != (self.expected_year_to is None):
            raise ValueError("expected year bounds must be both set or both omitted")
        if (
            self.expected_year_from is not None
            and self.expected_year_to is not None
            and self.expected_year_from > self.expected_year_to
        ):
            raise ValueError("expected year range is reversed")
        if self.expected_volume is not None and self.expected_volume < 0:
            raise ValueError("expected_volume must be non-negative")
        for name in (
            "temporal_semantics_prior",
            "enumerability_prior",
            "direct_evidence_prior",
            "baseline_overlap_prior",
            "confidence",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be within [0, 1]")
        for name in ("access_cost_prior", "adapter_cost_prior"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and non-negative")

    @property
    def source_key(self) -> str:
        return source_key(self.canonical_entrypoint)

    @property
    def origin(self) -> str:
        return source_origin(self.canonical_entrypoint)

    @property
    def scout_priority(self) -> float:
        """Dimensionless triage score used only before deterministic scouting.

        This is deliberately not an EED estimate. Agent priors merely order cheap
        scout work; measured downstream reward supersedes them after scouting.
        """
        level_multiplier = {
            SourceLevel.SOURCE: 1.0,
            SourceLevel.COLLECTION: 1.25,
            SourceLevel.METASOURCE: 1.6,
        }[self.level]
        # Exact timestamp-bearing bulk resources are unusually valuable:
        # they combine discovery and evidence and bypass per-host Wayback. Link
        # expansion often cannot know their record count in advance, so give a
        # conservative volume proxy instead of treating unknown direct files as
        # one-record sources.
        direct = float(self.direct_evidence_prior)
        volume_hint = self.expected_volume
        if volume_hint is None and direct >= 0.5:
            volume_hint = 100_000
        volume = math.log1p(max(1, volume_hint or 1))
        novelty = max(0.01, 1.0 - self.baseline_overlap_prior)
        quality = (
            1.0
            + self.temporal_semantics_prior
            + self.enumerability_prior
            + direct
        ) / 4.0
        direct_multiplier = 1.0 + 4.0 * direct
        cost = max(0.1, 1.0 + self.access_cost_prior + self.adapter_cost_prior)
        confidence = max(0.05, self.confidence)
        return (
            level_multiplier
            * volume
            * novelty
            * quality
            * direct_multiplier
            * confidence
            / cost
        )


@dataclass(frozen=True)
class ScoutMeasurement:
    sampled_records: int
    unique_hosts: int
    novel_hosts: int
    direct_host_years: int
    requests: int
    bytes_read: int
    elapsed_seconds: float
    novel_eed: float = 0.0
    measurement_mode: MeasurementMode = MeasurementMode.HOST_ONLY
    observed_host_year_pairs: int = 0
    novel_host_year_pairs: int = 0
    novel_pair_eed: float = 0.0
    singleton_observations: int = 0
    doubleton_observations: int = 0
    estimated_unseen_fraction: float = 0.0
    minhash_values: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "measurement_mode", MeasurementMode(self.measurement_mode))
        for name in (
            "sampled_records",
            "unique_hosts",
            "novel_hosts",
            "direct_host_years",
            "requests",
            "bytes_read",
            "observed_host_year_pairs",
            "novel_host_year_pairs",
            "singleton_observations",
            "doubleton_observations",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.novel_hosts > self.unique_hosts:
            raise ValueError("novel_hosts cannot exceed unique_hosts")
        if self.novel_host_year_pairs > self.observed_host_year_pairs:
            raise ValueError("novel_host_year_pairs cannot exceed observed_host_year_pairs")
        if not math.isfinite(self.elapsed_seconds) or self.elapsed_seconds < 0:
            raise ValueError("elapsed_seconds must be finite and non-negative")
        if not math.isfinite(self.novel_eed) or self.novel_eed < 0:
            raise ValueError("novel_eed must be finite and non-negative")
        if not math.isfinite(self.novel_pair_eed) or self.novel_pair_eed < 0:
            raise ValueError("novel_pair_eed must be finite and non-negative")
        if (
            not math.isfinite(self.estimated_unseen_fraction)
            or not 0.0 <= self.estimated_unseen_fraction <= 1.0
        ):
            raise ValueError(
                "estimated_unseen_fraction must be within [0, 1]"
            )
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in self.minhash_values
        ):
            raise ValueError("minhash_values must be non-negative integers")

    @property
    def measured_baseline_overlap(self) -> float:
        if self.measurement_mode is MeasurementMode.HOST_YEAR:
            if self.observed_host_year_pairs == 0:
                return 1.0
            return 1.0 - (self.novel_host_year_pairs / self.observed_host_year_pairs)
        if self.unique_hosts == 0:
            return 1.0
        return 1.0 - (self.novel_hosts / self.unique_hosts)

    @property
    def observed_count_for_threshold(self) -> int:
        if self.measurement_mode is MeasurementMode.HOST_YEAR:
            return self.observed_host_year_pairs
        return self.unique_hosts

    @property
    def novel_count_for_threshold(self) -> int:
        if self.measurement_mode is MeasurementMode.HOST_YEAR:
            return self.novel_host_year_pairs
        return self.novel_hosts

    @property
    def novel_eed_for_ranking(self) -> float:
        if self.measurement_mode is MeasurementMode.HOST_YEAR:
            return self.novel_pair_eed
        return self.novel_eed

    @property
    def novel_eed_per_second(self) -> float:
        if self.elapsed_seconds <= 0:
            return 0.0
        return self.novel_eed_for_ranking / self.elapsed_seconds

    @property
    def residual_opportunity(self) -> float:
        """Good-Turing-style residual mass used only for scheduling."""
        return (
            self.estimated_unseen_fraction
            * max(1, self.observed_count_for_threshold)
        )


@dataclass(frozen=True)
class SearchEpisode:
    episode_id: str
    strategy: str
    backend: str
    query: str
    actor: str
    started_at: float
    finished_at: float | None = None
    search_cost_seconds: float = 0.0
    accepted_novel_eed: float = 0.0

    @property
    def reward_per_cost(self) -> float:
        if self.search_cost_seconds <= 0:
            return 0.0
        return self.accepted_novel_eed / self.search_cost_seconds


@dataclass(frozen=True)
class StrategyReward:
    strategy: str
    episodes: int
    accepted_novel_eed: float
    search_cost_seconds: float

    @property
    def reward_per_cost(self) -> float:
        if self.search_cost_seconds <= 0:
            return 0.0
        return self.accepted_novel_eed / self.search_cost_seconds
