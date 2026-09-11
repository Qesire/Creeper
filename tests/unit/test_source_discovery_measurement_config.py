from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery_service import load_source_discovery_config


class SourceDiscoveryMeasurementConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.scrapy = self.root / "scrapy"
        self.scrapy.mkdir()
        (self.scrapy / "pyproject.toml").write_text("[project]\nname='sidecar'\nversion='0'\n", encoding="utf-8")
        (self.scrapy / "uv.lock").write_text("version = 1\n", encoding="utf-8")
        self.baseline = self.root / "baseline.sqlite3"
        self.baseline.write_bytes(b"")
        self.eed = self.root / "eed.json"
        self.eed.write_text("{}", encoding="utf-8")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _config(self) -> Path:
        path = self.root / "discovery.toml"
        path.write_text(
            '''runtime_data_root = "runtime"
scrapy_project_dir = "scrapy"

[pool]
active_min = 0
active_target = 0
warm_min = 0
warm_target = 0
cold_min = 1
cold_target = 1
triage_batch = 1
scout_parallelism = 1
max_search_directives = 1

[coordinator]
triage_parallelism = 1
scout_parallelism = 1
search_parallelism = 1
failure_retry_seconds = 1.0
search_cooldown_seconds = 1.0

[triage]
timeout_seconds = 1.0

[scrapy]
max_pages = 1
max_depth = 0
max_seconds = 1
max_memory_mb = 64
follow_query = false

[measurement]
baseline_index = "baseline.sqlite3"
eed_model = "eed.json"
max_records = 321
sample_windows = 7
min_novel_fraction = 0.125

[admission]
target_year_from = 1996
target_year_to = 2001
min_expected_volume = 100000
min_enumerability_prior = 0.5
min_confidence = 0.35
require_year_bounds = true

[agent]
command = ["python", "agent.py"]
backend = "test"
actor = "agent:test"
''',
            encoding="utf-8",
        )
        return path

    def test_optional_measurement_paths_and_limits_are_parsed(self) -> None:
        config = load_source_discovery_config(self._config())

        self.assertIsNotNone(config.measurement)
        assert config.measurement is not None
        self.assertEqual(config.measurement.baseline_index, self.baseline.resolve())
        self.assertEqual(config.measurement.eed_model, self.eed.resolve())
        self.assertEqual(config.measurement.policy.max_records, 321)
        self.assertEqual(config.measurement.policy.sample_windows, 7)
        self.assertEqual(config.measurement.policy.min_novel_fraction, 0.125)

    def test_missing_measurement_authority_file_fails_closed(self) -> None:
        self.baseline.unlink()
        with self.assertRaisesRegex(ValueError, "measurement.baseline_index does not exist"):
            load_source_discovery_config(self._config())

    def test_invalid_measurement_fraction_fails_closed(self) -> None:
        path = self._config()
        text = path.read_text(encoding="utf-8").replace(
            "min_novel_fraction = 0.125",
            "min_novel_fraction = 1.5",
        )
        path.write_text(text, encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "measurement.min_novel_fraction"):
            load_source_discovery_config(path)


if __name__ == "__main__":
    unittest.main()
