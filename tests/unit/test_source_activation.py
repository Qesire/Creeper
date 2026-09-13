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

    def test_raw_direct_contract_cannot_bypass_reviewed_registry(self) -> None:
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
                    "reviewed contract registry",
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

    def test_reviewed_parser_kind_mismatch_fails(self) -> None:
        with self.assertRaisesRegex(
            ReviewedContractRegistryError,
            "parser_kind",
        ):
            _reviewed_binding(
                "https://trusted.example/history/records.jsonl",
                parser_kind="delimited",
            )

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


if __name__ == "__main__":
    unittest.main()
