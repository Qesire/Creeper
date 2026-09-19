from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.distributed.config import load_worker_config


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


if __name__=="__main__":
    unittest.main()
