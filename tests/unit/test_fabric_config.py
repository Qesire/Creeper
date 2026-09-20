from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.distributed.config import load_authority_config, load_worker_config


class FabricWorkerConfigTests(unittest.TestCase):
    def test_worker_instance_is_generated_per_config_load(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            config=root/"worker.toml"
            config.write_text(
                """
[worker]
coordinator_url = "https://authority.example"
worker_id = "worker-a"
runtime_class = "full"
region = "sg"
architecture = "x86_64"
memory_bytes = 8589934592
cpu_count = 4
network_class = "public"
capabilities = ["RESIDUAL_QUERY"]
producers = ["ResidualQueryProducer"]
allowed_providers = ["datacite", "zenodo", "harvard_dataverse", "internet_archive"]
spool_database = "spool.sqlite3"
secret_env = "CREEPER_TEST_SECRET"
""",
                encoding="utf-8",
            )

            first=load_worker_config(config)
            second=load_worker_config(config)

            self.assertEqual(first.descriptor.worker_id,"worker-a")
            self.assertNotEqual(
                first.descriptor.worker_instance_id,
                second.descriptor.worker_instance_id,
            )
            self.assertTrue(first.worker_instance_auto)
            self.assertTrue(second.worker_instance_auto)
            self.assertEqual(
                first.spool_database,
                (root/"spool.sqlite3").resolve(),
            )

    def test_explicit_instance_override_is_preserved_for_controlled_tests(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            config=root/"worker.toml"
            config.write_text(
                """
[worker]
coordinator_url = "https://authority.example"
worker_id = "worker-a"
worker_instance_id = "fixture-instance"
runtime_class = "full"
region = "sg"
architecture = "x86_64"
memory_bytes = 1024
cpu_count = 1
network_class = "public"
capabilities = ["RESIDUAL_QUERY"]
producers = ["ResidualQueryProducer"]
allowed_providers = ["datacite"]
spool_database = "spool.sqlite3"
""",
                encoding="utf-8",
            )
            loaded=load_worker_config(config)
            self.assertEqual(
                loaded.descriptor.worker_instance_id,
                "fixture-instance",
            )
            self.assertFalse(loaded.worker_instance_auto)

    def test_remote_upload_budget_is_loaded_without_changing_provider_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            config=root/"worker.toml"
            config.write_text(
                """
[worker]
coordinator_url = "http://10.77.0.1:8088"
worker_id = "gcp-query-01"
runtime_class = "remote-free-query"
region = "gcp-uscentral1"
architecture = "x86_64"
memory_bytes = 536870912
cpu_count = 1
network_class = "wireguard-public-egress"
capabilities = ["RESIDUAL_QUERY"]
producers = ["ResidualQueryProducer"]
allowed_providers = ["datacite"]
daily_egress_budget_bytes = 12345
coordinator_timeout_seconds = 45
coordinator_upload_budget_bytes_per_month = 700000000
coordinator_upload_overhead_bytes = 1536
spool_database = "spool.sqlite3"
""",
                encoding="utf-8",
            )
            loaded=load_worker_config(config)
            self.assertEqual(
                loaded.descriptor.daily_egress_budget_bytes,
                12345,
            )
            self.assertEqual(
                loaded.coordinator_upload_budget_bytes_per_month,
                700000000,
            )
            self.assertEqual(loaded.coordinator_timeout_seconds,45)
            self.assertEqual(loaded.coordinator_upload_overhead_bytes,1536)

    def test_authority_region_flags_are_strict_and_reprobe_ttl_loads(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            credentials=root/"workers.json"
            credentials.write_text("{}\n",encoding="utf-8")
            config=root/"fabric.toml"
            config.write_text(
                f"""
[authority]
database = "fabric.sqlite3"
credentials_file = "{credentials}"
outbox_enabled = false
max_request_body_bytes = 33554432

[provider_budgets.datacite]
requests_per_second = 1.0
max_global_inflight = 2
require_qualified_region = true
allow_unknown_region_probe = true
region_reprobe_after_seconds = 1234
""",
                encoding="utf-8",
            )
            loaded=load_authority_config(config)
            budget=loaded.provider_budgets[0]
            self.assertEqual(loaded.max_request_body_bytes,33554432)
            self.assertTrue(budget.require_qualified_region)
            self.assertTrue(budget.allow_unknown_region_probe)
            self.assertEqual(budget.region_reprobe_after_seconds,1234)

            config.write_text(
                config.read_text(encoding="utf-8").replace(
                    "allow_unknown_region_probe = true",
                    'allow_unknown_region_probe = "false"',
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError,"must be a boolean"):
                load_authority_config(config)



if __name__=="__main__":
    unittest.main()
