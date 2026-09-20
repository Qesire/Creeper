from __future__ import annotations

import tomllib
import unittest
from pathlib import Path

from creeper.distributed.config import (
    load_authority_config,
    load_email_report_config,
    load_evidence_bridge_config,
    load_worker_config,
)


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

    def test_all_deployment_toml_files_parse(self) -> None:
        for path in self.root.glob("*.toml"):
            with self.subTest(path=path.name):
                with path.open("rb") as source:
                    value=tomllib.load(source)
                self.assertIsInstance(value,dict)


if __name__=="__main__":
    unittest.main()
