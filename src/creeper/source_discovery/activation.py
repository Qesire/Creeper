"""Compile measured discovery candidates into durable production reservoirs."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import PurePosixPath
from urllib.parse import urlsplit

from creeper.source_discovery.models import SourceCandidate, SourceState
from creeper.sources.domains import DomainState, SourceDomain
from creeper.sources.reservoirs import Reservoir, ReservoirState
from creeper.storage.control_store import ControlStore
from creeper.source_discovery.registry import SourceDiscoveryRegistry


class SourceActivationError(ValueError):
    """Raised when a discovered source cannot be safely activated."""


@dataclass(frozen=True)
class ProductionSourceSpec:
    source_key: str
    domain_id: str
    reservoir_id: str
    adapter_id: str
    adapter_kind: str
    root_locator: str
    source_family: str
    temporal_scope: tuple[int, int]
    enumeration_kind: str
    evidence_mode: str
    capacity_lower: int
    capacity_upper: int | None
    cursor: str | None


def _adapter_kind(entrypoint: str) -> tuple[str, str]:
    path = PurePosixPath(urlsplit(entrypoint).path.lower())
    name = path.name
    suffixes = (".warc.gz", ".arc.gz", ".warc", ".arc")
    if name.endswith(suffixes):
        return "warc_arc", "archive_records"
    structured = (
        ".cdxj", ".cdxj.gz", ".cdx", ".cdx.gz",
        ".jsonl", ".jsonl.gz",
        ".csv", ".csv.gz", ".tsv", ".tsv.gz",
        ".txt", ".txt.gz", ".list", ".list.gz", ".urls", ".urls.gz",
    )
    if name.endswith(structured):
        return "structured", "structured_records"
    raise SourceActivationError(
        f"unsupported adapter for discovered source: {entrypoint}"
    )


def _direct_year_capable(entrypoint: str) -> bool:
    name = PurePosixPath(urlsplit(entrypoint).path.lower()).name
    return name.endswith((".cdx", ".cdx.gz", ".cdxj", ".cdxj.gz"))


class SourceActivationCompiler:
    """Turn an ACTIVE candidate into one idempotent durable Reservoir."""

    def __init__(
        self,
        control_store: ControlStore,
        *,
        registry: SourceDiscoveryRegistry,
    ) -> None:
        self.control_store = control_store
        self.registry = registry

    def compile(self, candidate: SourceCandidate) -> ProductionSourceSpec:
        if candidate.state is not SourceState.ACTIVE:
            raise SourceActivationError("only ACTIVE candidates can be activated")
        stored = self.registry.get_candidate(candidate.source_key)
        if stored is None:
            raise SourceActivationError("candidate is not registered in discovery registry")
        if stored.state is not SourceState.ACTIVE:
            raise SourceActivationError("only ACTIVE candidates can be activated")
        measurement = self.registry.get_scout_measurement(candidate.source_key)
        if measurement is None:
            raise SourceActivationError("ACTIVE candidate requires scout measurement")
        adapter_kind, enumeration_kind = _adapter_kind(candidate.canonical_entrypoint)
        source_key = candidate.source_key
        domain_id = f"domain:{source_key.removeprefix('src:')}"
        reservoir_id = f"reservoir:{source_key.removeprefix('src:')}"
        adapter_id = f"{adapter_kind}:{source_key.removeprefix('src:')}"
        year_from = candidate.expected_year_from or 1996
        year_to = candidate.expected_year_to or 2001
        capacity_lower = max(
            0,
            int(
                measurement.observed_host_year_pairs
                if measurement.measurement_mode.value == "HOST_YEAR"
                else measurement.unique_hosts
            ),
        )
        capacity_upper = (
            max(capacity_lower, int(candidate.expected_volume))
            if candidate.expected_volume is not None
            else None
        )
        config_hash = hashlib.sha256(
            "\x00".join(
                (
                    adapter_kind,
                    candidate.canonical_entrypoint,
                    str(year_from),
                    str(year_to),
                )
            ).encode("utf-8")
        ).hexdigest()

        existing = self.control_store.get_reservoir(reservoir_id)
        if existing is not None:
            activation = self.control_store.get_activation(source_key)
            if activation is None:
                raise SourceActivationError(
                    "reservoir exists without source activation lineage"
                )
            return ProductionSourceSpec(
                source_key=source_key,
                domain_id=existing.domain_id,
                reservoir_id=existing.reservoir_id,
                adapter_id=existing.adapter_id,
                adapter_kind=adapter_kind,
                root_locator=existing.root_locator,
                source_family=candidate.source_family,
                temporal_scope=(year_from, year_to),
                enumeration_kind=existing.enumeration_kind,
                evidence_mode=existing.evidence_mode,
                capacity_lower=existing.capacity_lower,
                capacity_upper=existing.capacity_upper,
                cursor=existing.cursor,
            )

        domain = SourceDomain(
            domain_id=domain_id,
            family=candidate.source_family,
            discovery_mechanism=(
                f"activated:{candidate.discovered_by}:{candidate.discovery_strategy}"
            ),
            temporal_scope=(year_from, year_to),
            state=DomainState.EXPLORING,
        )
        reservoir = Reservoir(
            reservoir_id=reservoir_id,
            domain_id=domain_id,
            adapter_id=adapter_id,
            root_locator=candidate.canonical_entrypoint,
            enumeration_kind=enumeration_kind,
            capacity_lower=capacity_lower,
            capacity_upper=capacity_upper,
            evidence_mode=("direct_year" if _direct_year_capable(candidate.canonical_entrypoint)
                           else "discovery_only"),
            state=ReservoirState.READY,
        )
        self.control_store.save_activation(
            source_key=source_key,
            domain=domain,
            reservoir=reservoir,
            adapter_kind=adapter_kind,
            config_hash=config_hash,
        )
        return ProductionSourceSpec(
            source_key=source_key,
            domain_id=domain_id,
            reservoir_id=reservoir_id,
            adapter_id=adapter_id,
            adapter_kind=adapter_kind,
            root_locator=reservoir.root_locator,
            source_family=candidate.source_family,
            temporal_scope=(year_from, year_to),
            enumeration_kind=reservoir.enumeration_kind,
            evidence_mode=reservoir.evidence_mode,
            capacity_lower=reservoir.capacity_lower,
            capacity_upper=reservoir.capacity_upper,
            cursor=reservoir.cursor,
        )

    def compile_active(self, *, limit: int | None = None) -> list[ProductionSourceSpec]:
        """Compile all currently ACTIVE candidates in deterministic order."""
        if limit is not None and limit < 1:
            raise ValueError("limit must be positive")
        candidates = sorted(
            self.registry.list_candidates(state=SourceState.ACTIVE),
            key=lambda item: item.source_key,
        )
        if limit is not None:
            candidates = candidates[:limit]
        return [self.compile(candidate) for candidate in candidates]
