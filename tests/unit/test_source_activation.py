from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from creeper.evidence.contracts import (
    EvidenceAuthority,
    SourceEvidenceContract,
    contract_from_adapter_id,
)
from creeper.evidence.contract_registry import (
    ReviewedArtifactBinding,
    ReviewedArtifactIdentity,
    ReviewedContractRegistry,
    ReviewedContractRegistryError,
    ReviewedSourceContractBinding,
    reviewed_artifact_from_adapter_id,
)
from creeper.source_discovery.activation import (
    SourceActivationCompiler,
    SourceActivationError,
)
from creeper.source_discovery.models import (
    ScoutMeasurement,
    SourceCandidate,
    SourceLevel,
    SourceState,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.sources.format_binding import (
    SourceFormatObservation,
    format_from_adapter_id,
)
from creeper.sources.layout_binding import (
    SourceRecordLayout,
    layout_from_adapter_id,
)
from creeper.sources.schema_binding import (
    SourceRecordSchema,
    schema_from_adapter_id,
)
from creeper.storage.control_store import ControlStore


def _candidate(url: str, *, state: SourceState = SourceState.ACTIVE) -> SourceCandidate:
    return SourceCandidate(
        canonical_entrypoint=url,
        source_family="BULK_ARTIFACT",
        level=SourceLevel.SOURCE,
        discovered_by="test",
        discovery_strategy="fixture",
        expected_year_from=1996,
        expected_year_to=2001,
        expected_volume=100_000,
        enumerability_prior=0.95,
        confidence=0.9,
        state=state,
    )


def _reviewed_binding(
    locator: str,
    *,
    contract_id: str = "reviewed-structured-v1",
    parser_kind: str = "jsonl",
    hostname_field: str = "url",
    timestamp_field: str = "timestamp",
    identity: ReviewedArtifactIdentity | None = None,
) -> ReviewedSourceContractBinding:
    contract = SourceEvidenceContract(
        contract_id=contract_id,
        authority=EvidenceAuthority.DIRECT_WEB_YEAR,
        parser_kind=parser_kind,
        temporal_semantics="reviewed_web_observation_timestamp",
        evidence_type="reviewed_historical_web_record",
        hostname_field=hostname_field,
        timestamp_field=timestamp_field,
        policy_version="reviewed-structured-policy-v1",
    )
    return ReviewedSourceContractBinding(
        artifact=ReviewedArtifactBinding(
            locator=locator,
            source_identity=identity or ReviewedArtifactIdentity(
                kind="immutable_locator",
                value=locator,
            ),
            custodian="test-custodian",
            edition="test-edition",
        ),
        contract=contract,
        review_note="test reviewed source",
    )


class SourceActivationCompilerTests(unittest.TestCase):
    @staticmethod
    def _registry(control: ControlStore, candidate: SourceCandidate) -> SourceDiscoveryRegistry:
        registry = SourceDiscoveryRegistry(control)
        registry.register_proposal(candidate)
        registry.record_scout_measurement(
            candidate.source_key,
            ScoutMeasurement(
                sampled_records=256,
                unique_hosts=256,
                novel_hosts=200,
                direct_host_years=0,
                requests=1,
                bytes_read=1024,
                elapsed_seconds=1.0,
                novel_eed=100.0,
            ),
        )
        return registry

    def test_active_warc_candidate_compiles_to_durable_reservoir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate("https://archive.example/collection.warc.gz")
                registry = self._registry(control, candidate)
                spec = SourceActivationCompiler(control, registry=registry).compile(candidate)

                self.assertEqual(spec.adapter_kind, "warc_arc")
                self.assertEqual(spec.root_locator, "https://archive.example/collection.warc.gz")
                self.assertEqual(spec.temporal_scope, (1996, 2001))
                self.assertEqual(spec.capacity_lower, 256)
                self.assertIsNotNone(control.get_domain(spec.domain_id))
                reservoir = control.get_reservoir(spec.reservoir_id)
                self.assertIsNotNone(reservoir)
                self.assertEqual(reservoir.adapter_id, spec.adapter_id)
                self.assertEqual(reservoir.state.value, "READY")
                activation = control.get_activation(spec.source_key)
                self.assertEqual(activation["reservoir_id"], spec.reservoir_id)
            finally:
                control.close()

    def test_hostname_only_layout_is_frozen_without_temporal_authority(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://repo.example/api/download?id=hostname-only"
                )
                registry = self._registry(control, candidate)
                fmt = SourceFormatObservation(
                    parser_kind="jsonl",
                    compression="none",
                    detection_method="llm_declarative_validated",
                    confidence=1.0,
                    content_type="application/octet-stream",
                    policy_version="source-format-llm-layout-v2",
                )
                layout = SourceRecordLayout(
                    parser_kind="jsonl",
                    hostname_field="endpoint",
                    delimiter=None,
                    detection_method="llm_declarative_validated",
                    confidence=1.0,
                    sample_records=8,
                    matched_records=8,
                    policy_version="record-layout-llm-v1",
                )
                registry.record_format_observation(candidate.source_key, fmt)
                registry.record_layout_observation(candidate.source_key, layout)

                spec = SourceActivationCompiler(
                    control,
                    registry=registry,
                ).compile(candidate)

                self.assertEqual(spec.adapter_kind, "structured")
                self.assertEqual(spec.evidence_mode, "discovery_only")
                reservoir = control.get_reservoir(spec.reservoir_id)
                self.assertIsNotNone(reservoir)
                assert reservoir is not None
                self.assertEqual(
                    format_from_adapter_id(reservoir.adapter_id),
                    fmt,
                )
                self.assertEqual(
                    layout_from_adapter_id(reservoir.adapter_id),
                    layout,
                )
                self.assertIsNone(
                    schema_from_adapter_id(reservoir.adapter_id)
                )
                contract = contract_from_adapter_id(reservoir.adapter_id)
                self.assertIsNotNone(contract)
                assert contract is not None
                self.assertFalse(contract.grants_direct_web_year)
            finally:
                control.close()

    def test_custom_hostname_layout_with_capture_schema_activates_direct_year(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://repo.example/api/download?id=custom-dated-jsonl"
                )
                registry = self._registry(control, candidate)
                fmt = SourceFormatObservation(
                    parser_kind="jsonl",
                    compression="none",
                    detection_method="content_signature",
                    confidence=0.97,
                    content_type="application/octet-stream",
                )
                layout = SourceRecordLayout(
                    parser_kind="jsonl",
                    hostname_field="endpoint",
                    delimiter=None,
                    detection_method="stable_json_host_field",
                    confidence=1.0,
                    sample_records=8,
                    matched_records=8,
                )
                schema = SourceRecordSchema(
                    parser_kind="jsonl",
                    hostname_field="endpoint",
                    timestamp_field="capture_timestamp",
                    delimiter=None,
                    detection_method="stable_json_layout_time_field",
                    confidence=1.0,
                    sample_records=8,
                    matched_records=8,
                    direct_year_eligible=True,
                )
                registry.record_format_observation(candidate.source_key, fmt)
                registry.record_layout_observation(candidate.source_key, layout)
                registry.record_schema_observation(candidate.source_key, schema)

                spec = SourceActivationCompiler(
                    control,
                    registry=registry,
                ).compile(candidate)

                self.assertEqual(spec.evidence_mode, "direct_year")
                reservoir = control.get_reservoir(spec.reservoir_id)
                self.assertIsNotNone(reservoir)
                assert reservoir is not None
                self.assertEqual(
                    layout_from_adapter_id(reservoir.adapter_id),
                    layout,
                )
                self.assertEqual(
                    schema_from_adapter_id(reservoir.adapter_id),
                    schema,
                )
                contract = contract_from_adapter_id(reservoir.adapter_id)
                self.assertIsNotNone(contract)
                assert contract is not None
                self.assertTrue(contract.grants_direct_web_year)
                self.assertEqual(contract.hostname_field, "endpoint")
                self.assertEqual(
                    contract.timestamp_field,
                    "capture_timestamp",
                )
            finally:
                control.close()

    def test_unknown_locator_activates_from_trusted_format_observation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://repo.example/api/download?id=opaque"
                )
                registry = self._registry(control, candidate)
                observation = SourceFormatObservation(
                    parser_kind="jsonl",
                    compression="gzip",
                    detection_method="content_signature",
                    confidence=0.97,
                    content_type="application/octet-stream",
                )
                registry.record_format_observation(
                    candidate.source_key,
                    observation,
                )

                spec = SourceActivationCompiler(
                    control,
                    registry=registry,
                ).compile(candidate)

                self.assertEqual(spec.adapter_kind, "structured")
                self.assertEqual(spec.enumeration_kind, "structured_records")
                self.assertEqual(spec.evidence_mode, "discovery_only")
                reservoir = control.get_reservoir(spec.reservoir_id)
                self.assertIsNotNone(reservoir)
                assert reservoir is not None
                self.assertEqual(
                    format_from_adapter_id(reservoir.adapter_id),
                    observation,
                )
                contract = contract_from_adapter_id(reservoir.adapter_id)
                self.assertIsNotNone(contract)
                assert contract is not None
                self.assertEqual(contract.parser_kind, "jsonl")
                index_row = control.connection.execute(
                    """
                    SELECT source_format
                    FROM source_indexes_v1
                    WHERE source_key = ?
                    """,
                    (candidate.source_key,),
                ).fetchone()
                self.assertIsNotNone(index_row)
                self.assertEqual(index_row["source_format"], "JSONL")
            finally:
                control.close()

    def test_stable_jsonl_record_schema_auto_grants_direct_year(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://repo.example/api/download?id=dated-jsonl"
                )
                registry = self._registry(control, candidate)
                fmt = SourceFormatObservation(
                    parser_kind="jsonl",
                    compression="none",
                    detection_method="content_signature",
                    confidence=0.97,
                    content_type="application/octet-stream",
                )
                schema = SourceRecordSchema(
                    parser_kind="jsonl",
                    hostname_field="url",
                    timestamp_field="capture_year",
                    delimiter=None,
                    detection_method="stable_json_fields",
                    confidence=1.0,
                    sample_records=8,
                    matched_records=8,
                    direct_year_eligible=True,
                )
                registry.record_format_observation(candidate.source_key, fmt)
                registry.record_schema_observation(candidate.source_key, schema)

                spec = SourceActivationCompiler(
                    control,
                    registry=registry,
                ).compile(candidate)

                self.assertEqual(spec.adapter_kind, "structured")
                self.assertEqual(spec.evidence_mode, "direct_year")
                reservoir = control.get_reservoir(spec.reservoir_id)
                self.assertIsNotNone(reservoir)
                assert reservoir is not None
                self.assertEqual(
                    format_from_adapter_id(reservoir.adapter_id),
                    fmt,
                )
                self.assertEqual(
                    schema_from_adapter_id(reservoir.adapter_id),
                    schema,
                )
                contract = contract_from_adapter_id(reservoir.adapter_id)
                self.assertIsNotNone(contract)
                assert contract is not None
                self.assertTrue(contract.grants_direct_web_year)
                self.assertEqual(contract.hostname_field, "url")
                self.assertEqual(contract.timestamp_field, "capture_year")
                row = control.connection.execute(
                    """
                    SELECT direct_evidence_authority
                    FROM source_indexes_v1
                    WHERE source_key = ?
                    """,
                    (candidate.source_key,),
                ).fetchone()
                self.assertEqual(row["direct_evidence_authority"], 1)
            finally:
                control.close()

    def test_ambiguous_year_schema_stays_discovery_only_without_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://repo.example/api/download?id=ambiguous-jsonl"
                )
                registry = self._registry(control, candidate)
                fmt = SourceFormatObservation(
                    parser_kind="jsonl",
                    compression="none",
                    detection_method="content_signature",
                    confidence=0.97,
                    content_type="application/octet-stream",
                )
                schema = SourceRecordSchema(
                    parser_kind="jsonl",
                    hostname_field="url",
                    timestamp_field="year",
                    delimiter=None,
                    detection_method="stable_json_fields",
                    confidence=1.0,
                    sample_records=8,
                    matched_records=8,
                    direct_year_eligible=False,
                )
                registry.record_format_observation(candidate.source_key, fmt)
                registry.record_schema_observation(candidate.source_key, schema)

                spec = SourceActivationCompiler(
                    control,
                    registry=registry,
                ).compile(candidate)

                self.assertEqual(spec.adapter_kind, "structured")
                self.assertEqual(spec.evidence_mode, "discovery_only")
                reservoir = control.get_reservoir(spec.reservoir_id)
                self.assertIsNotNone(reservoir)
                assert reservoir is not None
                frozen_schema = schema_from_adapter_id(reservoir.adapter_id)
                self.assertEqual(frozen_schema, schema)
                contract = contract_from_adapter_id(reservoir.adapter_id)
                self.assertIsNotNone(contract)
                assert contract is not None
                self.assertFalse(contract.grants_direct_web_year)
            finally:
                control.close()

    def test_content_detected_cdxj_grants_direct_authority_from_record_semantics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://repo.example/api/download?id=opaque-cdxj"
                )
                registry = self._registry(control, candidate)
                registry.record_format_observation(
                    candidate.source_key,
                    SourceFormatObservation(
                        parser_kind="cdxj",
                        compression="none",
                        detection_method="content_signature",
                        confidence=0.98,
                        content_type="application/octet-stream",
                    ),
                )

                spec = SourceActivationCompiler(
                    control,
                    registry=registry,
                ).compile(candidate)

                self.assertEqual(spec.adapter_kind, "structured")
                self.assertEqual(spec.evidence_mode, "direct_year")
                reservoir = control.get_reservoir(spec.reservoir_id)
                self.assertIsNotNone(reservoir)
                assert reservoir is not None
                contract = contract_from_adapter_id(reservoir.adapter_id)
                self.assertIsNotNone(contract)
                assert contract is not None
                self.assertEqual(contract.parser_kind, "cdxj")
                self.assertTrue(contract.grants_direct_web_year)
                index_row = control.connection.execute(
                    """
                    SELECT direct_evidence_authority
                    FROM source_indexes_v1
                    WHERE source_key = ?
                    """,
                    (candidate.source_key,),
                ).fetchone()
                self.assertIsNotNone(index_row)
                self.assertEqual(index_row["direct_evidence_authority"], 1)
            finally:
                control.close()

    def test_low_confidence_unknown_format_does_not_auto_activate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://repo.example/api/download?id=weak"
                )
                registry = self._registry(control, candidate)
                registry.record_format_observation(
                    candidate.source_key,
                    SourceFormatObservation(
                        parser_kind="lines",
                        compression="none",
                        detection_method="content_signature",
                        confidence=0.88,
                    ),
                )

                with self.assertRaisesRegex(
                    SourceActivationError,
                    "unsupported adapter",
                ):
                    SourceActivationCompiler(
                        control,
                        registry=registry,
                    ).compile(candidate)
            finally:
                control.close()

    def test_compressed_url_list_activates_as_discovery_only_structured_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate("https://archive.example/webbase-2001.urls.gz")
                registry = self._registry(control, candidate)
                spec = SourceActivationCompiler(
                    control,
                    registry=registry,
                ).compile(candidate)

                self.assertEqual(spec.adapter_kind, "structured")
                self.assertEqual(spec.enumeration_kind, "structured_records")
                self.assertEqual(spec.evidence_mode, "discovery_only")
            finally:
                control.close()

    def test_audited_ftp_sitelist_activates_with_direct_year_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://ftpmirror1.infania.net/pub/simtelnet/msdos/info/"
                    "ftp-list.zip"
                )
                registry = self._registry(control, candidate)
                spec = SourceActivationCompiler(
                    control,
                    registry=registry,
                ).compile(candidate)

                self.assertEqual(spec.adapter_kind, "ftp_sitelist")
                self.assertEqual(spec.enumeration_kind, "structured_records")
                self.assertEqual(spec.evidence_mode, "direct_year")
                reservoir = control.get_reservoir(spec.reservoir_id)
                self.assertIsNotNone(reservoir)
                assert reservoir is not None
                contract = contract_from_adapter_id(reservoir.adapter_id)
                self.assertIsNotNone(contract)
                assert contract is not None
                self.assertEqual(contract.parser_kind, "ftp_sitelist_zip")
                self.assertTrue(contract.grants_direct_web_year)
            finally:
                control.close()

    def test_audited_sbi_bbs_activates_with_direct_year_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://files.mpoli.fi/software/TEXTS/MISC/SBI0197.ZIP"
                )
                registry = self._registry(control, candidate)
                spec = SourceActivationCompiler(
                    control,
                    registry=registry,
                ).compile(candidate)

                self.assertEqual(spec.adapter_kind, "sbi_bbs")
                self.assertEqual(spec.enumeration_kind, "structured_records")
                self.assertEqual(spec.evidence_mode, "direct_year")
                reservoir = control.get_reservoir(spec.reservoir_id)
                self.assertIsNotNone(reservoir)
                assert reservoir is not None
                contract = contract_from_adapter_id(reservoir.adapter_id)
                self.assertIsNotNone(contract)
                assert contract is not None
                self.assertEqual(contract.parser_kind, "sbi_bbs_zip")
                self.assertTrue(contract.grants_direct_web_year)
                index_row = control.connection.execute(
                    """
                    SELECT source_format, direct_evidence_authority
                    FROM source_indexes_v1
                    WHERE source_key = ?
                    """,
                    (candidate.source_key,),
                ).fetchone()
                self.assertIsNotNone(index_row)
                self.assertEqual(index_row["source_format"], "SBI_BBS")
                self.assertEqual(index_row["direct_evidence_authority"], 1)
            finally:
                control.close()

    def test_audited_finnish_bbs_activates_with_direct_year_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://files.mpoli.fi/software/TEXTS/MISC/FI980225.ZIP"
                )
                registry = self._registry(control, candidate)
                spec = SourceActivationCompiler(
                    control,
                    registry=registry,
                ).compile(candidate)

                self.assertEqual(spec.adapter_kind, "finnish_bbs")
                self.assertEqual(spec.enumeration_kind, "structured_records")
                self.assertEqual(spec.evidence_mode, "direct_year")
                reservoir = control.get_reservoir(spec.reservoir_id)
                self.assertIsNotNone(reservoir)
                assert reservoir is not None
                contract = contract_from_adapter_id(reservoir.adapter_id)
                self.assertIsNotNone(contract)
                assert contract is not None
                self.assertEqual(contract.parser_kind, "finnish_bbs_zip")
                self.assertTrue(contract.grants_direct_web_year)
                index_row = control.connection.execute(
                    """
                    SELECT source_format, direct_evidence_authority
                    FROM source_indexes_v1
                    WHERE source_key = ?
                    """,
                    (candidate.source_key,),
                ).fetchone()
                self.assertIsNotNone(index_row)
                self.assertEqual(index_row["source_format"], "FINNISH_BBS")
                self.assertEqual(index_row["direct_evidence_authority"], 1)
            finally:
                control.close()

    def test_ircache_trace_activates_with_direct_year_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://mirror.example/Traces/"
                    "uc.sanitized-access.20000312.gz"
                )
                registry = self._registry(control, candidate)
                spec = SourceActivationCompiler(
                    control,
                    registry=registry,
                ).compile(candidate)

                self.assertEqual(spec.adapter_kind, "structured")
                self.assertEqual(spec.enumeration_kind, "structured_records")
                self.assertEqual(spec.evidence_mode, "direct_year")
                reservoir = control.get_reservoir(spec.reservoir_id)
                self.assertIsNotNone(reservoir)
                assert reservoir is not None
                contract = contract_from_adapter_id(reservoir.adapter_id)
                self.assertIsNotNone(contract)
                assert contract is not None
                self.assertEqual(contract.parser_kind, "squid_access")
                self.assertTrue(contract.grants_direct_web_year)
            finally:
                control.close()

    def test_dmoz_content_dump_activates_as_discovery_only_structured_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://mirror.example/dmoz/2001-01-22/content.rdf.u8.gz"
                )
                registry = self._registry(control, candidate)
                spec = SourceActivationCompiler(
                    control,
                    registry=registry,
                ).compile(candidate)

                self.assertEqual(spec.adapter_kind, "structured")
                self.assertEqual(spec.enumeration_kind, "structured_records")
                self.assertEqual(spec.evidence_mode, "discovery_only")
            finally:
                control.close()

    def test_compressed_cdxj_activates_with_direct_year_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate("https://archive.example/index.cdxj.gz")
                registry = self._registry(control, candidate)
                spec = SourceActivationCompiler(
                    control,
                    registry=registry,
                ).compile(candidate)

                self.assertEqual(spec.adapter_kind, "structured")
                self.assertEqual(spec.evidence_mode, "direct_year")
                index_row = control.connection.execute(
                    """
                    SELECT source_format, access_mode,
                           direct_evidence_authority
                    FROM source_indexes_v1
                    WHERE source_key = ?
                    """,
                    (candidate.source_key,),
                ).fetchone()
                self.assertIsNotNone(index_row)
                self.assertEqual(index_row["source_format"], "CDXJ")
                self.assertEqual(index_row["access_mode"], "SORTED_INDEX")
                self.assertEqual(index_row["direct_evidence_authority"], 1)
                synopsis_row = control.connection.execute(
                    """
                    SELECT measurement_mode, novel_hosts, novel_eed,
                           sampled_records, complete
                    FROM source_region_synopses_v1
                    """
                ).fetchone()
                self.assertIsNotNone(synopsis_row)
                # Capability and measurement authority stay separate. This
                # shared fixture used a HOST_ONLY scout measurement, so
                # activation must not invent year-aware scout evidence.
                self.assertEqual(synopsis_row["measurement_mode"], "HOST_ONLY")
                self.assertEqual(synopsis_row["novel_hosts"], 200)
                self.assertEqual(synopsis_row["novel_eed"], 100.0)
                self.assertEqual(synopsis_row["sampled_records"], 256)
                self.assertEqual(synopsis_row["complete"], 0)
            finally:
                control.close()

    def test_recompile_preserves_real_tomography_synopsis_and_is_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate("https://archive.example/observed.cdxj")
                registry = self._registry(control, candidate)
                compiler = SourceActivationCompiler(
                    control,
                    registry=registry,
                )
                first = compiler.compile(candidate)
                index = compiler.index_registry.get_index_for_source(
                    candidate.source_key
                )
                self.assertIsNotNone(index)
                assert index is not None
                regions = compiler.index_registry.list_regions(index.index_key)
                root = next(region for region in regions if region.depth == 0)
                scout = compiler.index_registry.get_synopsis(root.region_key)
                self.assertIsNotNone(scout)
                assert scout is not None

                measured = replace(
                    scout,
                    sampled_records=scout.sampled_records + 17,
                    novel_hosts=scout.novel_hosts + 3,
                    novel_eed=scout.novel_eed + 321.5,
                    bytes_read=scout.bytes_read + 4096,
                    confidence=1.0,
                    complete=True,
                )
                compiler.index_registry.record_synopsis(measured)
                before = control.connection.total_changes

                second = compiler.compile(candidate)

                self.assertEqual(second.reservoir_id, first.reservoir_id)
                self.assertEqual(control.connection.total_changes, before)
                self.assertEqual(
                    compiler.index_registry.get_synopsis(root.region_key),
                    measured,
                )
            finally:
                control.close()

    def test_compile_is_idempotent_for_same_source_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate("https://archive.example/one.cdxj")
                registry = self._registry(control, candidate)
                compiler = SourceActivationCompiler(control, registry=registry)
                first = compiler.compile(candidate)
                self.assertEqual(first.evidence_mode, "direct_year")
                reservoir = control.get_reservoir(first.reservoir_id)
                assert reservoir is not None
                control.connection.execute(
                    "UPDATE reservoirs SET cursor = ?, state = ? WHERE reservoir_id = ?",
                    ("warc-byte:128", "LEASED", first.reservoir_id),
                )
                control.connection.commit()
                second = compiler.compile(candidate)

                self.assertEqual(first.source_key, second.source_key)
                self.assertEqual(first.reservoir_id, second.reservoir_id)
                self.assertEqual(control.get_reservoir(first.reservoir_id).cursor, "warc-byte:128")
                self.assertEqual(
                    control.connection.execute("SELECT COUNT(*) FROM reservoirs").fetchone()[0],
                    1,
                )
            finally:
                control.close()


    def test_exact_reviewed_csv_locator_activates_direct_year(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://trusted.example/history/links.csv"
                )
                registry = self._registry(control, candidate)
                reviewed = _reviewed_binding(
                    candidate.canonical_entrypoint,
                    contract_id="reviewed-linkgraph-csv-v1",
                    parser_kind="delimited",
                    hostname_field="column:0",
                    timestamp_field="column:1",
                )
                spec = SourceActivationCompiler(
                    control,
                    registry=registry,
                    reviewed_contracts=ReviewedContractRegistry(
                        {candidate.canonical_entrypoint: reviewed}
                    ),
                ).compile(candidate)

                self.assertEqual(spec.evidence_mode, "direct_year")
                bound = contract_from_adapter_id(spec.adapter_id)
                self.assertEqual(bound, reviewed.contract)
                frozen_artifact = reviewed_artifact_from_adapter_id(
                    spec.adapter_id
                )
                self.assertEqual(frozen_artifact, reviewed.artifact)
                row = control.connection.execute(
                    """
                    SELECT direct_evidence_authority
                    FROM source_indexes_v1
                    WHERE source_key = ?
                    """,
                    (candidate.source_key,),
                ).fetchone()
                self.assertEqual(row["direct_evidence_authority"], 1)
            finally:
                control.close()

    def test_jsonl_without_reviewed_contract_remains_discovery_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://untrusted.example/random.jsonl"
                )
                registry = self._registry(control, candidate)
                spec = SourceActivationCompiler(
                    control,
                    registry=registry,
                ).compile(candidate)
                self.assertEqual(spec.evidence_mode, "discovery_only")
            finally:
                control.close()

    def test_raw_direct_contract_requires_deterministic_record_schema(self) -> None:
        direct = SourceEvidenceContract(
            contract_id="unsafe-jsonl-v1",
            authority=EvidenceAuthority.DIRECT_WEB_YEAR,
            parser_kind="jsonl",
            temporal_semantics="claimed_timestamp",
            evidence_type="claimed_web_record",
            hostname_field="url",
            timestamp_field="year",
            policy_version="unsafe-v1",
        )
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://untrusted.example/random.jsonl"
                )
                registry = self._registry(control, candidate)
                with self.assertRaisesRegex(
                    SourceActivationError,
                    "deterministic record schema",
                ):
                    SourceActivationCompiler(
                        control,
                        registry=registry,
                        evidence_contracts={
                            candidate.canonical_entrypoint: direct,
                        },
                    ).compile(candidate)
            finally:
                control.close()

    def test_reviewed_parser_kind_mismatch_fails_at_activation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://trusted.example/history/records.jsonl"
                )
                registry = self._registry(control, candidate)
                reviewed = _reviewed_binding(
                    candidate.canonical_entrypoint,
                    parser_kind="delimited",
                )

                with self.assertRaisesRegex(
                    SourceActivationError,
                    "parser_kind disagrees",
                ):
                    SourceActivationCompiler(
                        control,
                        registry=registry,
                        reviewed_contracts=ReviewedContractRegistry(
                            {candidate.canonical_entrypoint: reviewed}
                        ),
                    ).compile(candidate)
            finally:
                control.close()

    def test_reviewed_registry_is_exact_locator_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                reviewed_locator = (
                    "https://trusted.example/history/reviewed.jsonl"
                )
                candidate = _candidate(
                    "https://trusted.example/history/sibling.jsonl"
                )
                registry = self._registry(control, candidate)
                reviewed = _reviewed_binding(reviewed_locator)
                spec = SourceActivationCompiler(
                    control,
                    registry=registry,
                    reviewed_contracts=ReviewedContractRegistry(
                        {reviewed_locator: reviewed}
                    ),
                ).compile(candidate)

                self.assertEqual(spec.evidence_mode, "discovery_only")
                self.assertIsNone(
                    reviewed_artifact_from_adapter_id(spec.adapter_id)
                )
            finally:
                control.close()

    def test_reviewed_immutable_locator_mismatch_fails(self) -> None:
        with self.assertRaisesRegex(
            ReviewedContractRegistryError,
            "does not match",
        ):
            _reviewed_binding(
                "https://trusted.example/history/records.jsonl",
                identity=ReviewedArtifactIdentity(
                    kind="immutable_locator",
                    value="https://trusted.example/history/other.jsonl",
                ),
            )

    def test_reviewed_artifact_identity_mismatch_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://trusted.example/history/records.jsonl"
                )
                registry = self._registry(control, candidate)
                reviewed = _reviewed_binding(
                    candidate.canonical_entrypoint,
                    identity=ReviewedArtifactIdentity(
                        kind="etag+length",
                        value='"reviewed-etag"',
                        content_length=100,
                    ),
                )

                def changed_identity(_artifact):
                    return ReviewedArtifactIdentity(
                        kind="etag+length",
                        value='"replacement-etag"',
                        content_length=100,
                    )

                with self.assertRaisesRegex(
                    SourceActivationError,
                    "identity mismatch",
                ):
                    SourceActivationCompiler(
                        control,
                        registry=registry,
                        reviewed_contracts=ReviewedContractRegistry(
                            {candidate.canonical_entrypoint: reviewed}
                        ),
                        identity_observer=changed_identity,
                    ).compile(candidate)
                self.assertEqual(
                    control.connection.execute(
                        "SELECT COUNT(*) FROM reservoirs"
                    ).fetchone()[0],
                    0,
                )
            finally:
                control.close()

    def test_unverifiable_reviewed_identity_downgrades_to_discovery_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://trusted.example/history/records.jsonl"
                )
                registry = self._registry(control, candidate)
                reviewed = _reviewed_binding(
                    candidate.canonical_entrypoint,
                    identity=ReviewedArtifactIdentity(
                        kind="etag+length",
                        value='"reviewed-etag"',
                        content_length=100,
                    ),
                )
                spec = SourceActivationCompiler(
                    control,
                    registry=registry,
                    reviewed_contracts=ReviewedContractRegistry(
                        {candidate.canonical_entrypoint: reviewed}
                    ),
                    identity_observer=lambda _artifact: None,
                ).compile(candidate)
                self.assertEqual(spec.evidence_mode, "discovery_only")
                self.assertNotIn(":rsi1:", spec.adapter_id)
            finally:
                control.close()

    def test_changed_registry_cannot_silently_upgrade_existing_reservoir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://trusted.example/history/records.jsonl"
                )
                registry = self._registry(control, candidate)
                first = SourceActivationCompiler(
                    control,
                    registry=registry,
                ).compile(candidate)
                self.assertEqual(first.evidence_mode, "discovery_only")

                reviewed = _reviewed_binding(candidate.canonical_entrypoint)
                second = SourceActivationCompiler(
                    control,
                    registry=registry,
                    reviewed_contracts=ReviewedContractRegistry(
                        {candidate.canonical_entrypoint: reviewed}
                    ),
                ).compile(candidate)

                self.assertEqual(second.adapter_id, first.adapter_id)
                self.assertEqual(second.evidence_mode, "discovery_only")
                self.assertNotIn(":rsi1:", second.adapter_id)
            finally:
                control.close()

    def test_agent_prior_cannot_grant_csv_direct_authority(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = replace(
                    _candidate("https://untrusted.example/random.csv"),
                    direct_evidence_prior=1.0,
                    temporal_semantics_prior=1.0,
                )
                registry = self._registry(control, candidate)
                spec = SourceActivationCompiler(
                    control,
                    registry=registry,
                ).compile(candidate)

                self.assertEqual(spec.evidence_mode, "discovery_only")
                bound = contract_from_adapter_id(spec.adapter_id)
                self.assertIsNotNone(bound)
                self.assertEqual(
                    bound.authority,
                    EvidenceAuthority.DISCOVERY_ONLY,
                )
                row = control.connection.execute(
                    """
                    SELECT direct_evidence_authority
                    FROM source_indexes_v1
                    WHERE source_key = ?
                    """,
                    (candidate.source_key,),
                ).fetchone()
                self.assertEqual(row["direct_evidence_authority"], 0)
            finally:
                control.close()

    def test_existing_activation_keeps_frozen_contract_across_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://trusted.example/history/records.jsonl"
                )
                registry = self._registry(control, candidate)
                reviewed = _reviewed_binding(
                    candidate.canonical_entrypoint,
                    contract_id="restart-stable-jsonl-v1",
                    timestamp_field="year",
                )
                first = SourceActivationCompiler(
                    control,
                    registry=registry,
                    reviewed_contracts=ReviewedContractRegistry(
                        {candidate.canonical_entrypoint: reviewed}
                    ),
                ).compile(candidate)
                before = control.connection.total_changes

                # Simulate restart with no registry loaded. The durable semantic
                # contract and reviewed artifact identity remain authoritative.
                second = SourceActivationCompiler(
                    control,
                    registry=registry,
                ).compile(candidate)

                self.assertEqual(first.adapter_id, second.adapter_id)
                self.assertEqual(second.evidence_mode, "direct_year")
                self.assertEqual(
                    contract_from_adapter_id(second.adapter_id),
                    reviewed.contract,
                )
                self.assertEqual(
                    reviewed_artifact_from_adapter_id(second.adapter_id),
                    reviewed.artifact,
                )
                self.assertEqual(control.connection.total_changes, before)
            finally:
                control.close()

    def test_unsupported_active_candidate_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate("https://archive.example/catalog.html")
                registry = self._registry(control, candidate)
                with self.assertRaisesRegex(SourceActivationError, "unsupported adapter"):
                    SourceActivationCompiler(control, registry=registry).compile(candidate)
                self.assertEqual(
                    control.connection.execute("SELECT COUNT(*) FROM reservoirs").fetchone()[0],
                    0,
                )
            finally:
                control.close()

    def test_non_active_candidate_cannot_be_activated(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://archive.example/catalog.warc",
                    state=SourceState.WARM,
                )
                registry = self._registry(control, candidate)
                with self.assertRaisesRegex(SourceActivationError, "ACTIVE"):
                    SourceActivationCompiler(control, registry=registry).compile(candidate)
            finally:
                control.close()

    def test_active_candidate_without_scout_measurement_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate("https://archive.example/unmeasured.warc")
                registry = SourceDiscoveryRegistry(control)
                registry.register_proposal(candidate)
                with self.assertRaisesRegex(SourceActivationError, "measurement"):
                    SourceActivationCompiler(control, registry=registry).compile(candidate)
            finally:
                control.close()

    def test_exhausted_reservoir_releases_active_discovery_slot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate("https://archive.example/exhausted.cdxj")
                registry = self._registry(control, candidate)
                spec = SourceActivationCompiler(control, registry=registry).compile(candidate)
                control.connection.execute(
                    "UPDATE reservoirs SET state = ? WHERE reservoir_id = ?",
                    ("EXHAUSTED", spec.reservoir_id),
                )
                control.connection.commit()

                changed = registry.reconcile_exhausted_activations()

                self.assertEqual(changed, 1)
                self.assertEqual(
                    registry.get_candidate(candidate.source_key).state,
                    SourceState.EXHAUSTED,
                )
                self.assertEqual(registry.reconcile_exhausted_activations(), 0)
            finally:
                control.close()

    def test_compile_active_discovers_registered_active_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate("https://archive.example/active.warc")
                registry = self._registry(control, candidate)
                specs = SourceActivationCompiler(control, registry=registry).compile_active()
                self.assertEqual([spec.source_key for spec in specs], [candidate.source_key])
            finally:
                control.close()

    def test_compile_active_isolates_bad_source_and_promotes_good_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                bad = _candidate("https://archive.example/bad.html", state=SourceState.WARM)
                good = _candidate("https://archive.example/good.warc", state=SourceState.WARM)
                registry = SourceDiscoveryRegistry(control)
                self._registry(control, bad)
                self._registry(control, good)
                registry.begin_activation(bad.source_key)
                registry.begin_activation(good.source_key)

                specs = SourceActivationCompiler(control, registry=registry).compile_active()

                self.assertEqual([spec.source_key for spec in specs], [good.source_key])
                self.assertEqual(registry.get_candidate(bad.source_key).state, SourceState.REJECTED)
                self.assertIn("unsupported adapter", registry.get_candidate(bad.source_key).state_reason)
                self.assertEqual(registry.get_candidate(good.source_key).state, SourceState.ACTIVE)
            finally:
                control.close()

    def test_warm_candidate_enters_activating_before_compiler_promotes_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = _candidate(
                    "https://archive.example/warm.warc",
                    state=SourceState.WARM,
                )
                registry = self._registry(control, candidate)
                activating = registry.begin_activation(candidate.source_key)
                self.assertEqual(activating.state, SourceState.ACTIVATING)
                self.assertEqual(registry.get_candidate(candidate.source_key).state, SourceState.ACTIVATING)

                specs = SourceActivationCompiler(control, registry=registry).compile_active()

                self.assertEqual(len(specs), 1)
                self.assertEqual(registry.get_candidate(candidate.source_key).state, SourceState.ACTIVE)
            finally:
                control.close()


if __name__ == "__main__":
    unittest.main()
