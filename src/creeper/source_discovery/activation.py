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
from creeper.source_discovery.models import (
    SourceCandidate,
    SourceState,
)
from creeper.sources.domains import DomainState, SourceDomain
from creeper.sources.format_binding import (
    SourceFormatObservation,
    bind_format_to_adapter_id,
    format_from_adapter_id,
)
from creeper.sources.locator import format_path_from_locator
from creeper.sources.layout_binding import (
    SourceRecordLayout,
    bind_layout_to_adapter_id,
    layout_from_adapter_id,
)
from creeper.sources.schema_binding import (
    SourceRecordSchema,
    bind_schema_to_adapter_id,
    schema_from_adapter_id,
)
from creeper.sources.reservoirs import Reservoir, ReservoirState
from creeper.storage.control_store import ControlStore
from creeper.source_discovery.registry import SourceDiscoveryRegistry


class SourceActivationError(ValueError):
    """Raised when a discovered source cannot be safely activated."""

    def __init__(self, message: str, *, permanent: bool = False) -> None:
        super().__init__(message)
        self.permanent = bool(permanent)


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


def _adapter_kind(
    entrypoint: str,
    *,
    parser_kind: str | None = None,
) -> tuple[str, str]:
    explicit_parser = parser_kind is not None
    parser_kind = (
        parser_kind_from_locator(entrypoint)
        if parser_kind is None
        else parser_kind.strip().lower()
    )
    if parser_kind == "ftp_sitelist_zip":
        return "ftp_sitelist", "structured_records"
    if parser_kind == "sbi_bbs_zip":
        return "sbi_bbs", "structured_records"
    if parser_kind == "warc_arc":
        return "warc_arc", "archive_records"
    if explicit_parser and parser_kind in {
        "cdx",
        "cdxj",
        "jsonl",
        "delimited",
        "lines",
        "mbox_urls",
        "squid_access",
        "dmoz_rdf_urls",
    }:
        # A trusted format observation is an execution fact, not evidence
        # authority. It is sufficient to select an already-mature parser even
        # when the transport locator has no useful filename suffix.
        return "structured", "structured_records"

    path = PurePosixPath(format_path_from_locator(entrypoint))
    name = path.name
    structured = (
        ".cdxj", ".cdxj.gz", ".cdx", ".cdx.gz",
        ".jsonl", ".jsonl.gz",
        ".csv", ".csv.gz", ".tsv", ".tsv.gz",
        ".txt", ".txt.gz", ".list", ".list.gz", ".urls", ".urls.gz",
    )
    if name.endswith(structured) or parser_kind in {
        "mbox_urls",
        "squid_access",
        "dmoz_rdf_urls",
    }:
        return "structured", "structured_records"
    raise SourceActivationError(
        f"unsupported adapter for discovered source: {entrypoint}",
        permanent=True,
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
        if candidate.state not in {SourceState.ACTIVE, SourceState.ACTIVATING}:
            raise SourceActivationError("only ACTIVE or ACTIVATING candidates can be activated")
        stored = self.registry.get_candidate(candidate.source_key)
        if stored is None:
            raise SourceActivationError("candidate is not registered in discovery registry")
        if stored.state not in {SourceState.ACTIVE, SourceState.ACTIVATING}:
            raise SourceActivationError("only ACTIVE or ACTIVATING candidates can be activated")
        source_key = candidate.source_key
        domain_id = f"domain:{source_key.removeprefix('src:')}"
        reservoir_id = f"reservoir:{source_key.removeprefix('src:')}"
        existing = self.control_store.get_reservoir(reservoir_id)

        format_observation = self.registry.get_format_observation(source_key)
        trusted_format: SourceFormatObservation | None = None
        if existing is not None:
            trusted_format = format_from_adapter_id(existing.adapter_id)
        if (
            trusted_format is None
            and format_observation is not None
            and format_observation.confidence >= 0.90
        ):
            trusted_format = format_observation

        layout_observation = self.registry.get_layout_observation(source_key)
        trusted_layout: SourceRecordLayout | None = None
        if existing is not None:
            trusted_layout = layout_from_adapter_id(existing.adapter_id)
        if (
            trusted_layout is None
            and layout_observation is not None
            and layout_observation.confidence >= 0.90
        ):
            trusted_layout = layout_observation

        schema_observation = self.registry.get_schema_observation(source_key)
        trusted_schema: SourceRecordSchema | None = None
        if existing is not None:
            trusted_schema = schema_from_adapter_id(existing.adapter_id)
        if (
            trusted_schema is None
            and schema_observation is not None
            and schema_observation.confidence >= 0.90
        ):
            trusted_schema = schema_observation
        if (
            trusted_layout is not None
            and trusted_format is not None
            and trusted_layout.parser_kind != trusted_format.parser_kind
        ):
            raise SourceActivationError(
                "record layout parser_kind disagrees with frozen source format",
                permanent=True,
            )
        if (
            trusted_schema is not None
            and (
                trusted_format is not None
                and trusted_schema.parser_kind != trusted_format.parser_kind
                or trusted_layout is not None
                and (
                    trusted_schema.parser_kind != trusted_layout.parser_kind
                    or trusted_schema.hostname_field != trusted_layout.hostname_field
                )
            )
        ):
            raise SourceActivationError(
                "record schema disagrees with frozen source layout",
                permanent=True,
            )

        if existing is not None:
            adapter_kind = existing.adapter_id.split(":", 1)[0]
            enumeration_kind = existing.enumeration_kind
        else:
            adapter_kind, enumeration_kind = _adapter_kind(
                candidate.canonical_entrypoint,
                parser_kind=(
                    trusted_format.parser_kind
                    if trusted_format is not None
                    else (
                        trusted_layout.parser_kind
                        if trusted_layout is not None
                        else (
                            trusted_schema.parser_kind
                            if trusted_schema is not None
                            else None
                        )
                    )
                ),
            )
        base_adapter_id = f"{adapter_kind}:{source_key.removeprefix('src:')}"
        year_from = candidate.expected_year_from or 1996
        year_to = candidate.expected_year_to or 2001

        # Hot-path idempotence matters here: every producer worker refreshes
        # ACTIVE sources repeatedly, and the historical-index service does the
        # same. Once both the production reservoir and capability-aware index
        # exist, recompilation must be read-only. In particular, never replace a
        # real tomography synopsis with the older scout synopsis.
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
                        "frozen reviewed artifact identity is no longer verifiable",
                        permanent=True,
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
                    parser_kind=(
                        trusted_format.parser_kind
                        if trusted_format is not None
                        else (
                            trusted_layout.parser_kind
                            if trusted_layout is not None
                            else (
                                trusted_schema.parser_kind
                                if trusted_schema is not None
                                else parser_kind_from_locator(existing.root_locator)
                            )
                        )
                    ),
                )
            adapter_id = existing.adapter_id
        else:
            locator_parser = parser_kind_from_locator(
                stored.canonical_entrypoint
            )
            actual_parser = (
                trusted_format.parser_kind
                if trusted_format is not None
                else (
                    trusted_layout.parser_kind
                    if trusted_layout is not None
                    else (
                        trusted_schema.parser_kind
                        if trusted_schema is not None
                        else locator_parser
                    )
                )
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
                        "reviewed artifact identity mismatch",
                        permanent=True,
                    ) from exc
                else:
                    if reviewed_binding.contract.parser_kind != actual_parser:
                        raise SourceActivationError(
                            "reviewed contract parser_kind disagrees with "
                            "the frozen source format",
                            permanent=True,
                        )
                    if (
                        trusted_layout is not None
                        and reviewed_binding.contract.hostname_field is not None
                        and reviewed_binding.contract.hostname_field
                        != trusted_layout.hostname_field
                    ):
                        raise SourceActivationError(
                            "reviewed contract hostname mapping disagrees with "
                            "the frozen record layout",
                            permanent=True,
                        )
                    if (
                        trusted_schema is not None
                        and reviewed_binding.contract.hostname_field is not None
                        and (
                            reviewed_binding.contract.hostname_field
                            != trusted_schema.hostname_field
                            or reviewed_binding.contract.timestamp_field
                            != trusted_schema.timestamp_field
                        )
                    ):
                        raise SourceActivationError(
                            "reviewed contract field mapping disagrees with "
                            "the frozen record schema",
                            permanent=True,
                        )
                    contract = reviewed_binding.contract
            else:
                explicit = self.evidence_contracts.get(
                    stored.canonical_entrypoint
                )
                if explicit is not None:
                    if (
                        explicit.grants_direct_web_year
                        and actual_parser not in {"cdx", "cdxj"}
                    ):
                        if trusted_schema is None:
                            raise SourceActivationError(
                                "structured DIRECT_WEB_YEAR authority requires "
                                "a deterministic record schema",
                                permanent=True,
                            )
                        if (
                            explicit.hostname_field
                            != trusted_schema.hostname_field
                            or explicit.timestamp_field
                            != trusted_schema.timestamp_field
                        ):
                            raise SourceActivationError(
                                "explicit direct contract field mapping "
                                "disagrees with frozen record schema",
                                permanent=True,
                            )
                    contract = resolve_source_evidence_contract(
                        stored.canonical_entrypoint,
                        explicit_contracts=self.evidence_contracts,
                        parser_kind=actual_parser,
                    )
                elif (
                    trusted_schema is not None
                    and trusted_schema.direct_year_eligible
                ):
                    # Stable item-level hostname/time bindings become automatic
                    # annual evidence only when the time field has explicit
                    # web-observation semantics. Ambiguous year/date columns
                    # remain discovery hints unless a reviewed/explicit
                    # contract supplies the missing semantic authority.
                    contract = trusted_schema.direct_contract()
                else:
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
            if trusted_format is not None:
                base_adapter_id = bind_format_to_adapter_id(
                    base_adapter_id,
                    trusted_format,
                )
            if trusted_layout is not None:
                base_adapter_id = bind_layout_to_adapter_id(
                    base_adapter_id,
                    trusted_layout,
                )
            if trusted_schema is not None:
                base_adapter_id = bind_schema_to_adapter_id(
                    base_adapter_id,
                    trusted_schema,
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
            parser_kind=(
                trusted_format.parser_kind
                if trusted_format is not None
                else (
                    trusted_layout.parser_kind
                    if trusted_layout is not None
                    else (
                        trusted_schema.parser_kind
                        if trusted_schema is not None
                        else None
                    )
                )
            ),
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
                        if trusted_format is None
                        else trusted_format.binding_digest
                    ),
                    (
                        ""
                        if trusted_schema is None
                        else trusted_schema.binding_digest
                    ),
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
        if reviewed_binding is not None:
            self._verified_reviewed_adapters.add(adapter_id)
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
        """Compile activation claims independently and promote only successes."""
        if limit is not None and limit < 1:
            raise ValueError("limit must be positive")
        candidates = sorted(
            self.registry.list_candidates_in_states(
                (SourceState.ACTIVATING, SourceState.ACTIVE)
            ),
            key=lambda item: item.source_key,
        )
        if limit is not None:
            candidates = candidates[:limit]
        specs: list[ProductionSourceSpec] = []
        for candidate in candidates:
            try:
                spec = self.compile(candidate)
            except (SourceActivationError, ValueError, OSError) as exc:
                permanent = bool(getattr(exc, "permanent", False))
                self.registry.record_activation_failure(
                    candidate.source_key,
                    reason=f"activation {'permanent' if permanent else 'transient'} failure: {exc}",
                    permanent=permanent,
                )
                continue
            if candidate.state is SourceState.ACTIVATING:
                self.registry.transition(candidate.source_key, SourceState.ACTIVE)
            specs.append(spec)
        return specs
