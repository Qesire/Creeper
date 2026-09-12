from __future__ import annotations

import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.historical_index_service import (
    HistoricalIndexOptimizerRuntime,
    load_historical_index_optimizer_config,
)
from creeper.source_discovery.index_optimization import (
    index_region_optimizer_eligible,
)
from creeper.source_discovery.models import (
    MeasurementMode,
    ScoutMeasurement,
    SourceCandidate,
    SourceLevel,
    SourceState,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.sources.reservoirs import ReservoirState
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class HistoricalIndexOptimizerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.runtime = self.root / "runtime"
        self.runtime.mkdir()
        task_root = self.root / "task"
        baseline_root = task_root / "merged260909-3"
        baseline_root.mkdir(parents=True)
        for year in range(1996, 2002):
            value = "known.com\n" if year == 1996 else ""
            (baseline_root / f"{year}.txt").write_text(
                value,
                encoding="utf-8",
            )
        (baseline_root / "candidate_pool.txt").write_text(
            "",
            encoding="utf-8",
        )
        self.baseline_path = self.root / "baseline.sqlite3"
        BaselineIndex.build(task_root, self.baseline_path).close()
        self.model_path = self.root / "eed-model.json"
        self.model_path.write_text(
            '{"tld":["com"],"lang":["eng"],"perc_of_tld":["100"]}',
            encoding="utf-8",
        )

        root = self.root

        class RangeHandler(BaseHTTPRequestHandler):
            def log_message(self, _format, *_args):
                return

            def _path(self):
                return root / self.path.split("?", 1)[0].lstrip("/")

            def _headers(self, *, status, size, start=None, end=None):
                self.send_response(status)
                self.send_header("Accept-Ranges", "bytes")
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(size))
                if start is not None and end is not None:
                    total = self._path().stat().st_size
                    self.send_header(
                        "Content-Range",
                        f"bytes {start}-{end}/{total}",
                    )
                self.end_headers()

            def do_HEAD(self):
                path = self._path()
                if not path.is_file():
                    self.send_error(404)
                    return
                self._headers(status=200, size=path.stat().st_size)

            def do_GET(self):
                path = self._path()
                if not path.is_file():
                    self.send_error(404)
                    return
                data = path.read_bytes()
                raw_range = self.headers.get("Range")
                if not raw_range:
                    self._headers(status=200, size=len(data))
                    self.wfile.write(data)
                    return
                value = raw_range.removeprefix("bytes=")
                left, _, right = value.partition("-")
                start = int(left)
                end = (
                    len(data) - 1
                    if not right
                    else min(len(data) - 1, int(right))
                )
                if start >= len(data) or end < start:
                    self.send_response(416)
                    self.send_header(
                        "Content-Range",
                        f"bytes */{len(data)}",
                    )
                    self.end_headers()
                    return
                payload = data[start:end + 1]
                self._headers(
                    status=206,
                    size=len(payload),
                    start=start,
                    end=end,
                )
                self.wfile.write(payload)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), RangeHandler)
        self.server_thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.server_thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=2)
        self.tmp.cleanup()

    def _config(
        self,
        *,
        enabled: bool = True,
        max_indexes: int = 4,
    ):
        path = self.root / "producer.toml"
        path.write_text(
            "\n".join(
                [
                    'source_mode = "activated"',
                    f'runtime_data_root = "{self.runtime}"',
                    f'baseline_index = "{self.baseline_path}"',
                    "",
                    "[historical_index]",
                    f"enabled = {'true' if enabled else 'false'}",
                    f"max_indexes_per_cycle = {max_indexes}",
                    "max_probe_actions_per_index = 4",
                    "probe_parallelism = 2",
                    "sample_bytes = 65536",
                    "sample_windows = 2",
                    "max_depth = 0",
                    "min_child_bytes = 64",
                    "min_observations_to_stop = 1",
                    "max_harvest_regions_per_cycle = 4",
                    "harvest_byte_budget = 1048576",
                    "harvest_max_records_per_lease = 1000",
                    "busy_poll_seconds = 0.01",
                    "idle_poll_seconds = 0.01",
                ]
            ),
            encoding="utf-8",
        )
        return load_historical_index_optimizer_config(
            path,
            eed_model=self.model_path,
        )

    def _register_active(
        self,
        locator: str,
        *,
        content_length: int | None = None,
    ) -> SourceCandidate:
        control = ControlStore(self.runtime / "control.sqlite3")
        try:
            registry = SourceDiscoveryRegistry(control)
            candidate = SourceCandidate(
                canonical_entrypoint=locator,
                source_family="BULK_ARTIFACT",
                level=SourceLevel.SOURCE,
                discovered_by="test",
                discovery_strategy="DIRECT_EVIDENCE_BULK",
                expected_year_from=1996,
                expected_year_to=2001,
                expected_volume=100,
                direct_evidence_prior=1.0,
                enumerability_prior=1.0,
                confidence=1.0,
                state=SourceState.ACTIVE,
            )
            registry.register_proposal(candidate)
            registry.record_scout_measurement(
                candidate.source_key,
                ScoutMeasurement(
                    sampled_records=2,
                    unique_hosts=2,
                    novel_hosts=2,
                    direct_host_years=2,
                    requests=1,
                    bytes_read=128,
                    elapsed_seconds=0.1,
                    novel_eed=2.0,
                    measurement_mode=MeasurementMode.HOST_YEAR,
                    observed_host_year_pairs=2,
                    novel_host_year_pairs=2,
                    novel_pair_eed=2.0,
                    minhash_values=(11, 13, 17, 19),
                ),
            )
            registry.record_triage_observation(
                candidate.source_key,
                status_code=200,
                method="HEAD",
                content_type="application/octet-stream",
                content_length=content_length,
                range_supported=True,
            )
            return candidate
        finally:
            control.close()

    async def test_full_cycle_closes_active_local_index_to_evidence_and_exhaustion(
        self,
    ) -> None:
        path = self.root / "index.cdxj"
        path.write_text(
            "".join(
                [
                    'com,known)/a 19960101000000 '
                    '{"url":"http://known.com/a"}\n',
                    'com,known)/b 19980101000000 '
                    '{"url":"http://known.com/b"}\n',
                    'com,novel)/a 19970101000000 '
                    '{"url":"http://novel.com/a"}\n',
                    'com,novel)/b 20010101000000 '
                    '{"url":"http://novel.com/b"}\n',
                ]
            ),
            encoding="utf-8",
        )
        candidate = self._register_active(
            f"{self.base_url}/{path.name}",
            content_length=path.stat().st_size,
        )
        config = self._config()

        async with HistoricalIndexOptimizerRuntime(
            config,
            owner="optimizer-test",
        ) as runtime:
            report = await runtime.run_once()

        self.assertEqual(report.compiled_active_sources, 1)
        self.assertEqual(report.compile_failures, 0)
        self.assertEqual(report.eligible_ready_indexes, 1)
        self.assertGreaterEqual(report.probes_succeeded, 1)
        self.assertEqual(report.probes_failed, 0)
        self.assertEqual(report.harvest_failed_regions, ())
        self.assertEqual(report.direct_capsules_inserted, 3)
        self.assertEqual(report.exhausted_reservoirs, 1)
        self.assertEqual(report.errors, ())

        control = ControlStore(self.runtime / "control.sqlite3")
        evidence = EvidenceStore(self.runtime / "evidence.sqlite3")
        try:
            activation = control.get_activation(candidate.source_key)
            self.assertIsNotNone(activation)
            reservoir = control.get_reservoir(
                str(activation["reservoir_id"])
            )
            self.assertEqual(reservoir.state, ReservoirState.EXHAUSTED)
            self.assertEqual(evidence.host_year_count(), 3)
            self.assertEqual(
                [item.year for item in evidence.for_hostname("known.com")],
                [1998],
            )
            self.assertEqual(
                [item.year for item in evidence.for_hostname("novel.com")],
                [1997, 2001],
            )
        finally:
            evidence.close()
            control.close()

    async def test_round_robin_checkpoint_rotates_probe_indexes(self) -> None:
        first = self.root / "a.cdxj"
        second = self.root / "b.cdxj"
        for path, host in ((first, "alpha"), (second, "beta")):
            path.write_text(
                f'com,{host})/ 19980101000000 '
                f'{{"url":"http://{host}.com/"}}\n',
                encoding="utf-8",
            )
            self._register_active(
                f"{self.base_url}/{path.name}",
                content_length=path.stat().st_size,
            )
        config = self._config(max_indexes=1)

        async with HistoricalIndexOptimizerRuntime(
            config,
            owner="rotation-test",
        ) as runtime:
            compiled, errors = runtime._compile_active_sources()
            self.assertEqual(compiled, 2)
            self.assertEqual(errors, [])
            eligible = runtime._eligible_ready_indexes()
            first_pick = runtime._rotate_indexes(eligible)
            second_pick = runtime._rotate_indexes(eligible)

        self.assertEqual(len(first_pick), 1)
        self.assertEqual(len(second_pick), 1)
        self.assertNotEqual(
            first_pick[0][0].index_key,
            second_pick[0][0].index_key,
        )

    async def test_compressed_direct_index_is_not_optimizer_eligible(self) -> None:
        path = self.root / "compressed.cdxj.gz"
        path.write_bytes(b"fixture")
        self._register_active(
            f"{self.base_url}/{path.name}",
            content_length=path.stat().st_size,
        )
        config = self._config()

        async with HistoricalIndexOptimizerRuntime(
            config,
            owner="compressed-test",
        ) as runtime:
            runtime._compile_active_sources()
            indexes = runtime.index_registry.list_indexes()
            self.assertEqual(len(indexes), 1)
            self.assertFalse(index_region_optimizer_eligible(indexes[0]))
            self.assertEqual(runtime._eligible_ready_indexes(), [])

    def test_disabled_config_is_explicit_and_non_destructive(self) -> None:
        config = self._config(enabled=False)
        self.assertFalse(config.enabled)


if __name__ == "__main__":
    unittest.main()
