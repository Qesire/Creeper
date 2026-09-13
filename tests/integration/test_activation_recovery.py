import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.models import ScoutMeasurement, SourceCandidate, SourceLevel, SourceState
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.storage.control_store import ControlStore


class ActivationRecoveryTests(unittest.TestCase):
    def test_transient_activation_failure_is_durable_and_retries_after_backoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                candidate = SourceCandidate(
                    canonical_entrypoint="https://archive.example/retry.warc",
                    source_family="BULK_ARTIFACT",
                    level=SourceLevel.SOURCE,
                    discovered_by="test",
                    discovery_strategy="fixture",
                    state=SourceState.WARM,
                )
                registry = SourceDiscoveryRegistry(control, clock=lambda: 100.0)
                registry.register_proposal(candidate)
                registry.record_scout_measurement(
                    candidate.source_key,
                    ScoutMeasurement(
                        sampled_records=1, unique_hosts=1, novel_hosts=1,
                        direct_host_years=0, requests=1, bytes_read=1,
                        elapsed_seconds=1.0, novel_eed=1.0,
                    ),
                )
                registry.begin_activation(candidate.source_key)
                registry.record_activation_failure(
                    candidate.source_key,
                    reason="transient identity lookup failed",
                    permanent=False,
                    retry_seconds=10.0,
                )
                held = registry.get_candidate(candidate.source_key)
                self.assertEqual(held.state, SourceState.HOLD)
                self.assertEqual(held.activation_retry_at, 110.0)
                self.assertIn("transient identity", held.state_reason)

                self.assertEqual(registry.retry_due_activations(now=109.0), 0)
                self.assertEqual(registry.retry_due_activations(now=110.0), 1)
                self.assertEqual(registry.get_candidate(candidate.source_key).state, SourceState.WARM)
            finally:
                control.close()

    def test_stranded_activating_state_recovers_without_consuming_active_slot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "control.sqlite3"
            control = ControlStore(path)
            candidate = SourceCandidate(
                canonical_entrypoint="https://archive.example/stranded.warc",
                source_family="BULK_ARTIFACT",
                level=SourceLevel.SOURCE,
                discovered_by="test",
                discovery_strategy="fixture",
                state=SourceState.WARM,
            )
            registry = SourceDiscoveryRegistry(control)
            registry.register_proposal(candidate)
            registry.record_scout_measurement(
                candidate.source_key,
                ScoutMeasurement(
                    sampled_records=1, unique_hosts=1, novel_hosts=1,
                    direct_host_years=0, requests=1, bytes_read=1,
                    elapsed_seconds=1.0, novel_eed=1.0,
                ),
            )
            registry.begin_activation(candidate.source_key)
            control.close()

            restarted_control = ControlStore(path)
            try:
                restarted = SourceDiscoveryRegistry(restarted_control)
                self.assertEqual(restarted.recover_stranded_activations(), 1)
                recovered = restarted.get_candidate(candidate.source_key)
                self.assertEqual(recovered.state, SourceState.HOLD)
                self.assertIn("restart", recovered.state_reason)
            finally:
                restarted_control.close()


if __name__ == "__main__":
    unittest.main()
