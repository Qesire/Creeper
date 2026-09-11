import tempfile
import unittest
from pathlib import Path

from creeper.storage.telemetry_store import RuntimeTelemetryStore


class RuntimeTelemetryStoreTests(unittest.TestCase):
    def test_counters_accumulate_and_gauges_track_latest_extrema(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "telemetry.sqlite3"
            now = [100.0]
            store = RuntimeTelemetryStore(path, clock=lambda: now[0])
            try:
                store.add_counters({"requests": 3, "retryable": 1})
                store.add_counters({"requests": 2})
                store.set_gauges({"rss_bytes": 100})
                now[0] = 110.0
                store.set_gauges({"rss_bytes": 80})
                store.set_max_gauges({"peak_rss_bytes": 100})
                store.set_max_gauges({"peak_rss_bytes": 90})
                store.set_min_gauges({"min_disk_free_bytes": 500})
                store.set_min_gauges({"min_disk_free_bytes": 600})
                store.set_min_gauges({"min_disk_free_bytes": 400})

                snapshot = store.snapshot()

                self.assertEqual(snapshot.counters["requests"], 5)
                self.assertEqual(snapshot.counters["retryable"], 1)
                self.assertEqual(snapshot.gauges["rss_bytes"], 80.0)
                self.assertEqual(snapshot.gauges["peak_rss_bytes"], 100.0)
                self.assertEqual(snapshot.gauges["min_disk_free_bytes"], 400.0)
                self.assertEqual(snapshot.gauge_updated_at["rss_bytes"], 110.0)
            finally:
                store.close()

    def test_independent_connections_accumulate_without_lost_updates(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "telemetry.sqlite3"
            first = RuntimeTelemetryStore(path)
            second = RuntimeTelemetryStore(path)
            try:
                first.add_counters({"source_records": 7})
                second.add_counters({"source_records": 11})
                self.assertEqual(
                    first.snapshot().counters["source_records"],
                    18,
                )
            finally:
                second.close()
                first.close()

    def test_rejects_negative_counter_delta(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = RuntimeTelemetryStore(Path(tmp) / "telemetry.sqlite3")
            try:
                with self.assertRaisesRegex(ValueError, "non-negative"):
                    store.add_counters({"requests": -1})
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
