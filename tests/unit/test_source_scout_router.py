from __future__ import annotations

import unittest

from creeper.source_discovery.coordinator import ScoutDisposition, ScoutResult
from creeper.source_discovery.models import ScoutMeasurement, SourceCandidate, SourceLevel
from creeper.source_discovery.scout_router import SourceScoutRouter


class SourceScoutRouterTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def candidate(name: str, *, family: str, level: SourceLevel) -> SourceCandidate:
        return SourceCandidate(
            canonical_entrypoint=f"https://example.com/{name}/",
            source_family=family,
            level=level,
            discovered_by="test",
            discovery_strategy="test",
            confidence=0.5,
        )

    @staticmethod
    def measurement() -> ScoutMeasurement:
        return ScoutMeasurement(
            sampled_records=10,
            unique_hosts=8,
            novel_hosts=3,
            direct_host_years=1,
            requests=2,
            bytes_read=1024,
            elapsed_seconds=0.5,
            novel_eed=2.0,
        )

    async def test_collection_routes_to_structural_executor_only(self) -> None:
        calls: list[str] = []

        async def structural(candidate: SourceCandidate) -> ScoutResult:
            calls.append(f"structural:{candidate.source_key}")
            return ScoutResult(ScoutDisposition.HOLD, reason="expanded")

        async def measured(candidate: SourceCandidate) -> ScoutResult:
            calls.append(f"measured:{candidate.source_key}")
            return ScoutResult(ScoutDisposition.WARM, self.measurement())

        candidate = self.candidate(
            "catalog",
            family="UNSEEN_COLLECTION_FAMILY",
            level=SourceLevel.COLLECTION,
        )
        result = await SourceScoutRouter(
            structural_executor=structural,
            measured_executor=measured,
        )(candidate)

        self.assertEqual(result.disposition, ScoutDisposition.HOLD)
        self.assertEqual(calls, [f"structural:{candidate.source_key}"])

    async def test_source_routes_to_measured_executor_and_may_become_warm(self) -> None:
        calls: list[str] = []

        async def structural(candidate: SourceCandidate) -> ScoutResult:
            calls.append(f"structural:{candidate.source_key}")
            return ScoutResult(ScoutDisposition.HOLD)

        async def measured(candidate: SourceCandidate) -> ScoutResult:
            calls.append(f"measured:{candidate.source_key}")
            return ScoutResult(ScoutDisposition.WARM, self.measurement())

        candidate = self.candidate(
            "bulk",
            family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
        )
        result = await SourceScoutRouter(
            structural_executor=structural,
            measured_executor=measured,
        )(candidate)

        self.assertEqual(result.disposition, ScoutDisposition.WARM)
        self.assertIsNotNone(result.measurement)
        self.assertEqual(calls, [f"measured:{candidate.source_key}"])

    async def test_missing_measured_adapter_holds_source_without_fake_measurement(self) -> None:
        async def structural(_candidate: SourceCandidate) -> ScoutResult:
            raise AssertionError("SOURCE candidate must not be structurally promoted")

        candidate = self.candidate(
            "unknown-source",
            family="UNKNOWN_DATASET",
            level=SourceLevel.SOURCE,
        )
        result = await SourceScoutRouter(structural_executor=structural)(candidate)

        self.assertEqual(result.disposition, ScoutDisposition.HOLD)
        self.assertIsNone(result.measurement)
        self.assertIn("no measured-yield scout adapter", result.reason)

    async def test_measured_family_override_prevents_structural_route(self) -> None:
        calls: list[str] = []

        async def structural(candidate: SourceCandidate) -> ScoutResult:
            calls.append(f"structural:{candidate.source_key}")
            return ScoutResult(ScoutDisposition.HOLD)

        async def measured(candidate: SourceCandidate) -> ScoutResult:
            calls.append(f"measured:{candidate.source_key}")
            return ScoutResult(ScoutDisposition.HOLD, reason="measured source")

        # Even if a malformed proposal labels a known bulk family as COLLECTION,
        # the explicit measured-family route wins. This avoids treating a bulk
        # artifact as a link directory solely because of a noisy level prior.
        candidate = self.candidate(
            "mislabelled-bulk",
            family="BULK_ARTIFACT",
            level=SourceLevel.COLLECTION,
        )
        result = await SourceScoutRouter(
            structural_executor=structural,
            measured_executor=measured,
        )(candidate)

        self.assertEqual(result.disposition, ScoutDisposition.HOLD)
        self.assertEqual(calls, [f"measured:{candidate.source_key}"])


if __name__ == "__main__":
    unittest.main()
