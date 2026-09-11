import tempfile
import unittest
from pathlib import Path

from creeper.scheduler.leases import WorkLease
from creeper.sources.local.static_dataset import StaticDatasetAdapter


class StaticDatasetAdapterTests(unittest.TestCase):
    def test_reuses_open_file_across_cursor_leases_and_closes_explicitly(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "hosts.txt"
            path.write_text("one.example\ntwo.example\n", encoding="utf-8")
            adapter = StaticDatasetAdapter(
                path,
                source_id="webbase",
                source_year=2001,
            )

            first = WorkLease.create(
                reservoir_id="webbase",
                cursor_start=None,
                max_records=1,
                max_requests=1,
                max_bytes=4096,
                max_seconds=30.0,
            )
            records, first_result = adapter.execute(first)
            self.assertEqual([record.payload for record in records], ["one.example"])
            self.assertIsNotNone(first_result.next_cursor)
            self.assertIsNotNone(adapter._source)
            assert adapter._source is not None
            descriptor = adapter._source.fileno()

            second = WorkLease.create(
                reservoir_id="webbase",
                cursor_start=first_result.next_cursor,
                max_records=1,
                max_requests=1,
                max_bytes=4096,
                max_seconds=30.0,
            )
            records, second_result = adapter.execute(second)
            self.assertEqual([record.payload for record in records], ["two.example"])
            self.assertIsNone(second_result.next_cursor)
            self.assertIsNotNone(adapter._source)
            assert adapter._source is not None
            self.assertEqual(adapter._source.fileno(), descriptor)

            adapter.close()
            self.assertIsNone(adapter._source)


if __name__ == "__main__":
    unittest.main()
