from __future__ import annotations

import tomllib
import unittest

from creeper.autopilot import load_autopilot_config
from pathlib import Path

from creeper.distributed.config import (
    load_authority_config,
    load_email_report_config,
    load_evidence_bridge_config,
    load_worker_config,
)
from creeper.source_discovery_service import load_source_discovery_config


class OciA1DeploymentConfigTests(unittest.TestCase):
    @property
    def root(self) -> Path:
        return Path(__file__).resolve().parents[2]/"deploy"/"oci-a1"

    def test_fabric_profile_is_loopback_postgres_and_email_only(self) -> None:
        path=self.root/"fabric.toml"
        authority=load_authority_config(path)
        bridge=load_evidence_bridge_config(path)
        report=load_email_report_config(path)

        self.assertEqual(authority.database,"postgresql:///creeper")
        self.assertEqual(authority.host,"127.0.0.1")
        self.assertEqual(authority.port,8088)
        self.assertFalse(authority.outbox_enabled)
        self.assertTrue(authority.provider_budgets)
        self.assertTrue(
            all(not item.require_qualified_region for item in authority.provider_budgets)
        )
        self.assertEqual(
            bridge.runtime_data_root,
            Path("/srv/creeper/data"),
        )
        self.assertEqual(report.timezone,"Asia/Singapore")
        self.assertTrue(report.starttls)
        discovery=load_source_discovery_config(self.root/"source-discovery.toml")
        self.assertTrue(discovery.fabric.enabled)
        self.assertFalse(discovery.fabric.outbox_enabled)
        self.assertEqual(discovery.pool.max_search_directives,0)

    def test_workers_are_colocated_and_have_disjoint_execution_roles(self) -> None:
        query=load_worker_config(self.root/"worker-query.toml")
        evidence=load_worker_config(self.root/"worker-evidence.toml")

        self.assertEqual(query.coordinator_url,"http://127.0.0.1:8088")
        self.assertEqual(evidence.coordinator_url,"http://127.0.0.1:8088")
        self.assertEqual(query.descriptor.architecture,"aarch64")
        self.assertEqual(evidence.descriptor.architecture,"aarch64")
        self.assertEqual(query.descriptor.producers,("ResidualQueryProducer",))
        self.assertEqual(evidence.descriptor.producers,("EvidenceQueryProducer",))
        self.assertNotEqual(query.descriptor.worker_id,evidence.descriptor.worker_id)

    def test_autopilot_resource_envelope_precedes_systemd_ceiling(self) -> None:
        config=load_autopilot_config(self.root/"autopilot.toml")
        self.assertFalse(config.evidence.enabled)
        self.assertFalse(config.evidence.platform_harvest_enabled)
        self.assertIsNotNone(config.resource_governor)
        assert config.resource_governor is not None
        gib=1024**3
        self.assertEqual(config.resource_governor.rss_throttle_bytes,5*gib)
        self.assertEqual(config.resource_governor.rss_stop_bytes,6*gib)

        unit=(self.root/"systemd"/"creeper-autopilot.service").read_text(
            encoding="utf-8"
        )
        self.assertIn("MemoryHigh=5G",unit)
        self.assertIn("MemoryMax=7G",unit)
        for name in (
            "creeper-fabric-authority.service",
            "creeper-fabric-worker@.service",
            "creeper-fabric-evidence-bridge.service",
        ):
            with self.subTest(unit=name):
                text=(self.root/"systemd"/name).read_text(encoding="utf-8")
                self.assertIn("MemoryMax=",text)
                self.assertIn("OOMPolicy=stop",text)

    def test_transient_gc_is_scheduled_and_alerted(self) -> None:
        service=(self.root/"systemd"/"creeper-fabric-gc.service").read_text(
            encoding="utf-8"
        )
        timer=(self.root/"systemd"/"creeper-fabric-gc.timer").read_text(
            encoding="utf-8"
        )
        install=(self.root/"install.sh").read_text(encoding="utf-8")
        self.assertIn("gc --retention-hours 24 --limit 50000",service)
        self.assertIn("OnFailure=creeper-email-alert@%n.service",service)
        self.assertIn("OnUnitActiveSec=1h",timer)
        self.assertIn("Persistent=true",timer)
        self.assertIn("enable --now creeper-fabric-gc.timer",install)

    def test_all_deployment_toml_files_parse(self) -> None:
        for path in self.root.glob("*.toml"):
            with self.subTest(path=path.name):
                with path.open("rb") as source:
                    value=tomllib.load(source)
                self.assertIsInstance(value,dict)


if __name__=="__main__":
    unittest.main()
