from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from creeper.distributed.authority_cli import _assert_safe_bind
from creeper.distributed.config import (
    load_authority_config,
    load_worker_config,
    load_worker_credentials,
)


class DistributedConfigTests(unittest.TestCase):
    def test_authority_config_defaults_to_loopback_and_qualified_regions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "authority.toml"
            config_path.write_text(
                """
[authority]
database = "/tmp/authority.sqlite3"
baseline_index = "/tmp/baseline.sqlite3"
credentials_file = "/tmp/credentials.json"

[provider_budgets.internet_archive]
requests_per_second = 0.5
max_global_inflight = 4
""".strip(),
                encoding="utf-8",
            )

            config = load_authority_config(config_path)

        self.assertEqual(config.host, "127.0.0.1")
        self.assertEqual(config.port, 8088)
        self.assertEqual(len(config.provider_budgets), 1)
        budget = config.provider_budgets[0]
        self.assertEqual(budget.name, "internet_archive")
        self.assertTrue(budget.require_qualified_region)

    def test_authority_public_bind_requires_explicit_override(self) -> None:
        _assert_safe_bind("127.0.0.1", allow_public_bind=False)
        _assert_safe_bind("::1", allow_public_bind=False)
        with self.assertRaises(RuntimeError):
            _assert_safe_bind("0.0.0.0", allow_public_bind=False)
        _assert_safe_bind("0.0.0.0", allow_public_bind=True)

    def test_worker_config_parses_real_cdx_provider_and_secret_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "worker.toml"
            config_path.write_text(
                """
[worker]
coordinator_url = "https://coord.example"
worker_id = "oci-01"
runtime_class = "vm"
region = "oci-home"
architecture = "aarch64"
memory_bytes = 12884901888
cpu_count = 2
network_class = "public"
capabilities = ["ONLINE_QUERY"]
allowed_providers = ["internet_archive"]
secret_env = "TEST_CREEPER_SECRET"
poll_seconds = 2
lease_seconds = 120

[[cdx_providers]]
name = "internet_archive"
endpoint = "https://web.archive.org/cdx/search/cdx"
dialect = "wayback"
row_limit = 150000
max_inflight = 4
max_connections = 8
max_keepalive_connections = 4
""".strip(),
                encoding="utf-8",
            )

            config = load_worker_config(config_path)

        self.assertEqual(config.descriptor.worker_id, "oci-01")
        self.assertEqual(config.descriptor.producers, ())
        self.assertEqual(
            config.descriptor.allowed_providers,
            ("internet_archive",),
        )
        self.assertEqual(config.cdx_providers[0].row_limit, 150_000)
        self.assertEqual(config.cdx_providers[0].dialect, "wayback")
        with patch.dict(os.environ, {"TEST_CREEPER_SECRET": "secret"}, clear=False):
            self.assertEqual(config.load_secret(), "secret")
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError):
                config.load_secret()

    def test_worker_config_rejects_provider_client_outside_allowlist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "worker-mismatch.toml"
            config_path.write_text(
                """
[worker]
coordinator_url = "https://coord.example"
worker_id = "gcp-01"
runtime_class = "vm"
region = "gcp-us"
architecture = "x86_64"
memory_bytes = 1073741824
cpu_count = 1
network_class = "public"
capabilities = ["ONLINE_QUERY"]
allowed_providers = ["arquivo_pt"]

[[cdx_providers]]
name = "internet_archive"
endpoint = "https://web.archive.org/cdx/search/cdx"
dialect = "wayback"
max_inflight = 1
max_connections = 2
max_keepalive_connections = 1
""".strip(),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ValueError,
                "configured CDX providers are not allowed",
            ):
                load_worker_config(config_path)

    def test_credentials_file_rejects_empty_or_non_string_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            valid = root / "valid.json"
            valid.write_text(
                json.dumps({"worker-a": "secret-a"}),
                encoding="utf-8",
            )
            self.assertEqual(
                load_worker_credentials(valid),
                {"worker-a": "secret-a"},
            )

            empty = root / "empty.json"
            empty.write_text("{}", encoding="utf-8")
            with self.assertRaises(ValueError):
                load_worker_credentials(empty)

            invalid = root / "invalid.json"
            invalid.write_text(
                json.dumps({"worker-a": 123}),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                load_worker_credentials(invalid)


if __name__ == "__main__":
    unittest.main()
