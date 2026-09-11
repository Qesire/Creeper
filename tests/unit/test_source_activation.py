from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

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
