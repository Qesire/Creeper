import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.records.candidates import CandidateSourceScope
from creeper.sources.auxiliary_audit import audit_v3_auxiliary
from creeper.sources.local.auxiliary import V3AuxiliaryURLAdapter
from creeper.sources.local.static_dataset import StaticDatasetAdapter
from creeper.scheduler.leases import WorkLease


class AuxiliarySourceTests(unittest.TestCase):
    def _static_adapter_and_lease(self, contents, **limits):
        tmp = tempfile.TemporaryDirectory()
        path = Path(tmp.name) / "dataset.txt"
        path.write_bytes(contents)
        adapter = StaticDatasetAdapter(path, source_id="test_dataset")
        lease = WorkLease.create(
            reservoir_id="test_dataset",
            max_records=limits.get("max_records", 10),
            max_requests=limits.get("max_requests", 1),
            max_bytes=limits.get("max_bytes", 10_000),
            max_seconds=limits.get("max_seconds", 10),
            cursor_start=limits.get("cursor_start"),
            cursor_end=limits.get("cursor_end"),
        )
        return tmp, adapter, lease

    def test_static_dataset_execute_respects_inclusive_line_cursor_and_returns_next_cursor(self):
        tmp, adapter, lease = self._static_adapter_and_lease(
            b"one.example\ntwo.example\nthree.example\nfour.example\n",
            cursor_start="2",
            cursor_end="3",
        )
        try:
            records, result = adapter.execute(lease)
            records = list(records)
            self.assertEqual([record.payload for record in records], ["two.example", "three.example"])
            self.assertEqual([record.locator.rsplit(":", 1)[-1] for record in records], ["2", "3"])
            self.assertEqual(result.lease_id, lease.lease_id)
            self.assertEqual(result.records, 2)
            self.assertEqual(result.requests, 1)
            self.assertEqual(result.next_cursor, "4")
        finally:
            tmp.cleanup()

    def test_static_dataset_execute_stops_at_max_records(self):
        tmp, adapter, lease = self._static_adapter_and_lease(
            b"one.example\ntwo.example\nthree.example\n", max_records=2
        )
        try:
            records, result = adapter.execute(lease)
            self.assertEqual(len(list(records)), 2)
            self.assertEqual(result.records, 2)
            self.assertEqual(result.next_cursor, "3")
        finally:
            tmp.cleanup()

    def test_static_dataset_execute_stops_before_max_bytes(self):
        tmp, adapter, lease = self._static_adapter_and_lease(
            b"one.example\ntwo.example\n", max_bytes=len(b"one.example\n")
        )
        try:
            records, result = adapter.execute(lease)
            self.assertEqual([record.payload for record in records], ["one.example"])
            self.assertEqual(result.bytes_read, len(b"one.example\n"))
            self.assertEqual(result.next_cursor, "2")
        finally:
            tmp.cleanup()

    def test_static_dataset_execute_stops_at_max_seconds(self):
        tmp, adapter, lease = self._static_adapter_and_lease(
            b"one.example\n", max_seconds=0
        )
        try:
            records, result = adapter.execute(lease)
            self.assertEqual(list(records), [])
            self.assertEqual(result.records, 0)
            self.assertEqual(result.next_cursor, "1")
        finally:
            tmp.cleanup()

    def test_static_dataset_propagates_configured_source_year(self):
        tmp, adapter, lease = self._static_adapter_and_lease(b"one.example\n")
        adapter = StaticDatasetAdapter(adapter.path, source_id=adapter.source_id, source_year=1997)
        try:
            records, _result = adapter.execute(lease)
            record = next(records)
            observation = next(adapter.extract_hosts(record))
            self.assertEqual(record.source_year, 1997)
            self.assertEqual(observation.source_year, 1997)
        finally:
            tmp.cleanup()

    def test_enumerates_auxiliary_files_with_line_provenance(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            merged = root / "merged260909-3"
            merged.mkdir()
            (merged / "deduplicated_urls_1996-1997.txt").write_text(
                "new.example\nnot a hostname\n", encoding="utf-8"
            )
            (merged / "deduplicated_urls_2001-2002.txt").write_text(
                "other.example\n", encoding="utf-8"
            )
            adapter = V3AuxiliaryURLAdapter(root)
            records = list(adapter.enumerate())

            self.assertEqual(len(records), 3)
            self.assertEqual(records[0].scope, CandidateSourceScope.LOCAL_DISCOVERY)
            self.assertIn("deduplicated_urls_", records[0].source_id)
            self.assertTrue(records[0].locator.endswith(":1"))
            self.assertEqual(
                [item.hostname for record in records for item in adapter.extract_hosts(record)],
                ["new.example", "other.example"],
            )

    def test_audit_deduplicates_and_separates_authority_overlap(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            merged = root / "merged260909-3"
            merged.mkdir()
            for year in range(1996, 2002):
                (merged / f"{year}.txt").write_text(
                    "annual.example\n", encoding="utf-8"
                )
            (merged / "candidate_pool.txt").write_text(
                "candidate.example\n", encoding="utf-8"
            )
            (merged / "deduplicated_urls_1996-1997.txt").write_text(
                "new.example\ncandidate.example\nnew.example\ninvalid\n", encoding="utf-8"
            )
            (merged / "deduplicated_urls_2001-2002.txt").write_text(
                "annual.example\n", encoding="utf-8"
            )
            index = BaselineIndex.build(root, root / "index.sqlite3")
            index.close()

            report = audit_v3_auxiliary(
                root, root / "index.sqlite3", total_limit=10, limit_per_file=10
            )

            self.assertEqual(report["raw_lines"], 5)
            self.assertEqual(report["valid_hostname_lines"], 4)
            self.assertEqual(report["invalid_lines"], 1)
            self.assertEqual(report["duplicate_hostname_lines"], 1)
            self.assertEqual(report["unique_hostnames"], 3)
            self.assertEqual(report["annual_authority_overlap"], 1)
            self.assertEqual(report["official_candidate_overlap"], 1)
            self.assertEqual(report["potential_active_discoveries"], 2)


if __name__ == "__main__":
    unittest.main()
