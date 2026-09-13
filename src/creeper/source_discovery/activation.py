"""Compile measured discovery candidates into durable production reservoirs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
from pathlib import PurePosixPath
from urllib.parse import urlsplit

from creeper.evidence.contracts import (
    SourceEvidenceContract,
    bind_contract_to_adapter_id,
    contract_from_adapter_id,
    discovery_only_contract,
    freeze_contract_bindings,
    parser_kind_from_locator,
    resolve_source_evidence_contract,
)
from creeper.evidence.contract_registry import (
    IdentityObserver,
    ReviewedArtifactIdentityError,
    ReviewedArtifactIdentityUnverifiable,
    ReviewedContractRegistry,
    bind_reviewed_artifact_to_adapter_id,
    reviewed_artifact_from_adapter_id,
    verify_reviewed_artifact_identity,
)
from creeper.source_discovery.index_identity import HistoricalIndexObjectIdentity
from creeper.source_discovery.index_registry import IndexSpaceRegistry
from creeper.source_discovery.index_space import RegionSynopsis, compile_candidate_index_space
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


class SourceActivationCompiler:
    """Turn an ACTIVE candidate into one idempotent durable Reservoir."""

    def __init__(
        self,
        control_store: ControlStore,
        *,
        registry: SourceDiscoveryRegistry,
        evidence_contracts: Mapping[str, SourceEvidenceContract] | None = None,
        reviewed_contracts: ReviewedContractRegistry | None = None,
        identity_observer: IdentityObserver | None = None,
    ) -> None:
        self.control_store = control_store
        self.registry = registry
        self.index_registry = IndexSpaceRegistry(control_store)
        self.evidence_contracts = freeze_contract_bindings(evidence_contracts)
        self.reviewed_contracts = reviewed_contracts or ReviewedContractRegistry()
        self.identity_observer = identity_observer
        self._verified_reviewed_adapters: set[str] = set()

    def compile(self, candidate: SourceCandidate) -> ProductionSourceSpec:
        if candidate.state is not SourceState.ACTIVE:
            raise SourceActivationError("only ACTIVE candidates can be activated")
        stored = self.registry.get_candidate(candidate.source_key)
        if stored is None:
            raise SourceActivationError("candidate is not registered in discovery registry")
        if stored.state is not SourceState.ACTIVE:
            raise SourceActivationError("only ACTIVE candidates can be activated")
        adapter_kind, enumeration_kind = _adapter_kind(candidate.canonical_entrypoint)
        source_key = candidate.source_key
        domain_id = f"domain:{source_key.removeprefix('src:')}"
        reservoir_id = f"reservoir:{source_key.removeprefix('src:')}"
        base_adapter_id = f"{adapter_kind}:{source_key.removeprefix('src:')}"
        year_from = candidate.expected_year_from or 1996
        year_to = candidate.expected_year_to or 2001

        # Hot-path idempotence matters here: every producer worker refreshes
        # ACTIVE sources repeatedly, and the historical-index service does the
        # same. Once both the production reservoir and capability-aware index
        # exist, recompilation must be read-only. In particular, never replace a
        # real tomography synopsis with the older scout synopsis.
        existing = self.control_store.get_reservoir(reservoir_id)
        if existing is not None:
            activation = self.control_store.get_activation(source_key)
            if activation is None:
                raise SourceActivationError(
                    "reservoir exists without source activation lineage"
                )
            frozen_artifact = reviewed_artifact_from_adapter_id(
                existing.adapter_id
            )
            if (
                frozen_artifact is not None
                and existing.adapter_id not in self._verified_reviewed_adapters
            ):
                try:
                    verify_reviewed_artifact_identity(
                        frozen_artifact,
                        observer=self.identity_observer,
                    )
                except (
                    ReviewedArtifactIdentityError,
                    ReviewedArtifactIdentityUnverifiable,
                ) as exc:
                    raise SourceActivationError(
                        "frozen reviewed artifact identity is no longer verifiable"
                    ) from exc
                self._verified_reviewed_adapters.add(existing.adapter_id)
            if self.index_registry.get_index_for_source(source_key) is not None:
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

        # Freeze authority at activation. Existing reservoirs keep their
        # durable contract; newly supplied registries cannot upgrade authority
        # in the middle of a lease or after a restart.
        verified_reviewed_identity = None
        reviewed_binding = None
        if existing is not None:
            contract = contract_from_adapter_id(existing.adapter_id)
            if contract is None:
                contract = resolve_source_evidence_contract(
                    existing.root_locator,
                    parser_kind=parser_kind_from_locator(existing.root_locator),
                )
            adapter_id = existing.adapter_id
        else:
            actual_parser = parser_kind_from_locator(
                stored.canonical_entrypoint
            )
            reviewed_binding = self.reviewed_contracts.get_exact(
                stored.canonical_entrypoint
            )
            if reviewed_binding is not None:
                try:
                    verified_reviewed_identity = (
                        verify_reviewed_artifact_identity(
                            reviewed_binding.artifact,
                            observer=self.identity_observer,
                        )
                    )
                except ReviewedArtifactIdentityUnverifiable:
                    # A reviewed semantic contract without verifiable artifact
                    # identity is scheduling information only. Freeze the new
                    # activation as discovery-only; later registry changes must
                    # not silently upgrade this reservoir.
                    contract = discovery_only_contract(actual_parser)
                    reviewed_binding = None
                except ReviewedArtifactIdentityError as exc:
                    raise SourceActivationError(
                        "reviewed artifact identity mismatch"
                    ) from exc
                else:
                    contract = reviewed_binding.contract
            else:
                explicit = self.evidence_contracts.get(
                    stored.canonical_entrypoint
                )
                if (
                    explicit is not None
                    and explicit.grants_direct_web_year
                    and actual_parser not in {"cdx", "cdxj"}
                ):
                    raise SourceActivationError(
                        "structured DIRECT_WEB_YEAR authority requires a "
                        "versioned reviewed contract registry"
                    )
                contract = resolve_source_evidence_contract(
                    stored.canonical_entrypoint,
                    explicit_contracts=self.evidence_contracts,
                    parser_kind=actual_parser,
                )

            if reviewed_binding is not None:
                base_adapter_id = bind_reviewed_artifact_to_adapter_id(
                    base_adapter_id,
                    reviewed_binding.artifact,
                )
            adapter_id = bind_contract_to_adapter_id(
                base_adapter_id,
                contract,
            )

        measurement = self.registry.get_scout_measurement(source_key)
        if measurement is None:
            raise SourceActivationError("ACTIVE candidate requires scout measurement")

        # Backfill the capability-aware index exactly once for legacy
        # activations or create it alongside a new activation.
        triage = self.registry.get_triage_observation(source_key)
        compiled_index_space = compile_candidate_index_space(
            stored,
            range_supported=(
                None
                if triage is None
                else triage.get("range_supported")
            ),
            content_length=(
                None
                if triage is None
                else triage.get("content_length")
            ),
            direct_evidence_authority=contract.grants_direct_web_year,
        )
        self.index_registry.register_index_space(compiled_index_space)
        if (
            reviewed_binding is not None
            and verified_reviewed_identity is not None
            and verified_reviewed_identity.kind == "etag+length"
        ):
            self.index_registry.bind_object_identity(
                compiled_index_space.index.index_key,
                HistoricalIndexObjectIdentity(
                    kind="remote",
                    content_length=verified_reviewed_identity.content_length,
                    etag=verified_reviewed_identity.value,
                ),
            )
        if (
            self.index_registry.get_synopsis(
                compiled_index_space.root_region.region_key
            )
            is None
        ):
            self.index_registry.record_synopsis(
                RegionSynopsis(
                    region_key=compiled_index_space.root_region.region_key,
                    sampled_records=measurement.sampled_records,
                    unique_hosts=measurement.unique_hosts,
                    novel_hosts=measurement.novel_hosts,
                    observed_host_year_pairs=measurement.observed_host_year_pairs,
                    novel_host_year_pairs=measurement.novel_host_year_pairs,
                    novel_eed=measurement.novel_eed_for_ranking,
                    bytes_read=measurement.bytes_read,
                    requests=measurement.requests,
                    measurement_mode=measurement.measurement_mode,
                    minhash_values=measurement.minhash_values,
                    confidence=stored.confidence,
                    complete=False,
                )
            )
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
                    contract.binding_digest,
                    (
                        ""
                        if reviewed_binding is None
                        else reviewed_binding.binding_digest
                    ),
                )
            ).encode("utf-8")
        ).hexdigest()

        if existing is not None:
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
            evidence_mode=contract.evidence_mode,
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
