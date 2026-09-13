from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from creeper.platform_harvest_cli import admit_platform_year_once
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.sources.domains import DomainState, SourceDomain
from creeper.sources.reservoirs import Reservoir, ReservoirState
from creeper.storage.control_store import ControlStore


class PlatformAdmissionAutopilotIntegrationTests(unittest.TestCase):
    def test_admission_reads_durable_active_source_observations_and_converges(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            control = ControlStore(root / "control.sqlite3")
            domain = SourceDomain(
                domain_id="domain:admission",
                family="BULK_ARTIFACT",
                discovery_mechanism="fixture",
                temporal_scope=(1996, 2001),
                state=DomainState.EXPLORING,
            )
            reservoir = Reservoir(
                reservoir_id="reservoir:admission",
                domain_id=domain.domain_id,
                adapter_id="structured:admission",
                root_locator="https://source.example/index.cdxj",
                enumeration_kind="structured_records",
                capacity_lower=1,
                capacity_upper=10,
                evidence_mode="direct_year",
                state=ReservoirState.READY,
            )
            control.save_activation(
                source_key="source:admission",
                domain=domain,
                reservoir=reservoir,
                adapter_kind="structured",
                config_hash="config-v1",
            )
            SourceDiscoveryRegistry(control).set_scout_authority(
                baseline_signature="b" * 64,
                model_signature="m" * 64,
            )
            control.attribute_direct_origin_unit_host_years(
                [("example.com", 1997, "direct:fixture"),
                 ("www.example.com", 1998, "direct:fixture")],
                source_key="source:admission",
                reservoir_id=reservoir.reservoir_id,
                origin_kind="region_harvest",
                origin_unit_id="region:fixture",
            )
            control.close()

            first = admit_platform_year_once(
                root,
                endpoint="https://web.archive.org/cdx/search/cdx",
                budget=1,
            )
            self.assertEqual(first.admitted, 1)
            self.assertEqual(first.blocked, 1)
            self.assertEqual(len(first.tasks), 1)
            self.assertEqual(first.tasks[0].source_key, "source:admission")

            second = admit_platform_year_once(
                root,
                endpoint="https://web.archive.org/cdx/search/cdx",
                budget=1,
            )
            self.assertEqual(second.admitted, 1)
            self.assertEqual(second.idempotent, 1)

            third = admit_platform_year_once(
                root,
                endpoint="https://web.archive.org/cdx/search/cdx",
                budget=1,
            )
            self.assertEqual(third.admitted, 0)
            self.assertEqual(third.idempotent, 2)


if __name__ == "__main__":
    unittest.main()
