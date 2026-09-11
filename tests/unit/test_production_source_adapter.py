from __future__ import annotations

import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch

from creeper.scheduler.leases import LeaseResult, WorkLease
from creeper.sources.archive.warc_source import WarcSourceLeaseResult, WarcSourceObservation
from creeper.sources.production import ProductionAdapterFactory
from creeper.sources.reservoirs import Reservoir, ReservoirState


class ProductionSourceAdapterTests(unittest.TestCase):
    def test_warc_reservoir_factory_preserves_hint_and_not_direct_evidence(self) -> None:
        reservoir = Reservoir(
            reservoir_id="reservoir:fixture",
            domain_id="domain:fixture",
            adapter_id="warc_arc:fixture",
            root_locator="https://archive.example/fixture.warc.gz",
            enumeration_kind="archive_records",
            capacity_lower=1,
            state=ReservoirState.READY,
        )
        archive_result = WarcSourceLeaseResult(
            observations=(
                WarcSourceObservation(
                    source_id=reservoir.reservoir_id,
                    locator="warc-byte:0:100",
                    target_uri="http://novel.com/",
                    year_hint=1998,
                    record_type="response",
                ),
            ),
            next_cursor="warc-byte:100",
            exhausted=False,
            scanned_records=1,
            bytes_advanced=100,
        )
        with patch(
            "creeper.sources.production.WarcSourceLeaseExecutor.execute",
            return_value=archive_result,
        ):
            adapter = ProductionAdapterFactory.open(reservoir)
            lease = WorkLease.create(
                reservoir_id=reservoir.reservoir_id,
                cursor_start=None,
                max_records=10,
                max_requests=1,
                max_bytes=1024,
                max_seconds=10,
            )
            records, result = adapter.execute(lease)
            record = next(records)
            observations = tuple(adapter.extract_hosts(record))

        self.assertEqual(result.next_cursor, "warc-byte:100")
        self.assertEqual(record.year_hint_mask, 1 << (1998 - 1996))
        self.assertEqual(record.direct_year_mask, 0)
        self.assertEqual(observations[0].hostname, "novel.com")
        self.assertEqual(observations[0].year_hint_mask, record.year_hint_mask)
        self.assertEqual(observations[0].direct_year_mask, 0)

    def test_structured_cdxj_adapter_uses_byte_cursor_and_direct_year_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "records.cdxj"
            path.write_text(
                'com 19980101000000 {"url":"http://novel.com/"}\n',
                encoding="utf-8",
            )
            reservoir = Reservoir(
                reservoir_id="reservoir:structured",
                domain_id="domain:structured",
                adapter_id="structured:structured",
                root_locator=str(path),
                enumeration_kind="structured_records",
                capacity_lower=1,
                state=ReservoirState.READY,
            )
            adapter = ProductionAdapterFactory.open(reservoir)
            lease = WorkLease.create(
                reservoir_id=reservoir.reservoir_id,
                max_records=10,
                max_requests=1,
                max_bytes=1024,
                max_seconds=10,
            )
            records, result = adapter.execute(lease)
            record = next(records)
            observation = next(iter(adapter.extract_hosts(record)))

        self.assertIsNone(result.next_cursor)
        self.assertEqual(observation.hostname, "novel.com")
        self.assertEqual(observation.year_hint_mask, 0)
        self.assertEqual(observation.direct_year_mask, 1 << (1998 - 1996))

    def test_structured_cdx_adapter_parses_jisc_style_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "1998.cdx"
            path.write_text(
                "com,novel)/ 19980101000000 http://novel.com/ text/html 200 digest 1 2 file.arc\n",
                encoding="utf-8",
            )
            reservoir = Reservoir(
                reservoir_id="reservoir:cdx",
                domain_id="domain:cdx",
                adapter_id="structured:cdx",
                root_locator=str(path),
                enumeration_kind="structured_records",
                capacity_lower=1,
                state=ReservoirState.READY,
                evidence_mode="direct_year",
            )
            adapter = ProductionAdapterFactory.open(reservoir)
            lease = WorkLease.create(
                reservoir_id=reservoir.reservoir_id,
                max_records=10,
                max_requests=1,
                max_bytes=1024,
                max_seconds=10,
            )
            records, _result = adapter.execute(lease)
            observation = next(iter(adapter.extract_hosts(next(records))))

        self.assertEqual(observation.hostname, "novel.com")
        self.assertEqual(observation.source_time, "19980101000000")
        self.assertEqual(observation.direct_year_mask, 1 << (1998 - 1996))


if __name__ == "__main__":
    unittest.main()
