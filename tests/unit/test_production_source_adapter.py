from __future__ import annotations

import gzip
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
            records, result = adapter.execute(lease)
            record = next(records)
            observation = next(iter(adapter.extract_hosts(record)))

        self.assertIsNone(result.next_cursor)
        self.assertEqual(observation.hostname, "novel.com")
        self.assertEqual(observation.year_hint_mask, 0)
        self.assertEqual(observation.direct_year_mask, 1 << (1998 - 1996))

    def test_compressed_cdx_reuses_decompressor_across_leases(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "records.cdx.gz"
            with gzip.open(path, "wt", encoding="utf-8") as target:
                target.write(
                    "com,first)/ 19980101000000 http://first.com/ text/html 200 digest 1 2 file.arc\n"
                )
                target.write(
                    "com,second)/ 19990101000000 http://second.com/ text/html 200 digest 1 2 file.arc\n"
                )
            reservoir = Reservoir(
                reservoir_id="reservoir:compressed-cdx",
                domain_id="domain:compressed-cdx",
                adapter_id="structured:compressed-cdx",
                root_locator=str(path),
                enumeration_kind="structured_records",
                capacity_lower=2,
                state=ReservoirState.READY,
                evidence_mode="direct_year",
            )
            adapter = ProductionAdapterFactory.open(reservoir)
            first_lease = WorkLease.create(
                reservoir_id=reservoir.reservoir_id,
                max_records=1,
                max_requests=1,
                max_bytes=4096,
                max_seconds=10,
            )
            first_records, first_result = adapter.execute(first_lease)
            first = next(iter(adapter.extract_hosts(next(first_records))))

            second_lease = WorkLease.create(
                reservoir_id=reservoir.reservoir_id,
                cursor_start=first_result.next_cursor,
                max_records=1,
                max_requests=1,
                max_bytes=4096,
                max_seconds=10,
            )
            second_records, second_result = adapter.execute(second_lease)
            second = next(iter(adapter.extract_hosts(next(second_records))))
            adapter.close()

        self.assertEqual(first.hostname, "first.com")
        self.assertEqual(first.direct_year_mask, 1 << (1998 - 1996))
        self.assertEqual(second.hostname, "second.com")
        self.assertEqual(second.direct_year_mask, 1 << (1999 - 1996))
        self.assertEqual(first_result.requests, 1)
        self.assertEqual(second_result.requests, 0)

    def test_compressed_cdx_can_resume_from_durable_logical_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "resume.cdx.gz"
            with gzip.open(path, "wt", encoding="utf-8") as target:
                target.write(
                    "com,first)/ 19980101000000 http://first.com/ text/html 200 digest 1 2 file.arc\n"
                )
                target.write(
                    "com,second)/ 19990101000000 http://second.com/ text/html 200 digest 1 2 file.arc\n"
                )
            reservoir = Reservoir(
                reservoir_id="reservoir:resume-cdx",
                domain_id="domain:resume-cdx",
                adapter_id="structured:resume-cdx",
                root_locator=str(path),
                enumeration_kind="structured_records",
                capacity_lower=2,
                state=ReservoirState.READY,
                evidence_mode="direct_year",
            )
            first_adapter = ProductionAdapterFactory.open(reservoir)
            lease = WorkLease.create(
                reservoir_id=reservoir.reservoir_id,
                max_records=1,
                max_requests=1,
                max_bytes=4096,
                max_seconds=10,
            )
            _records, first_result = first_adapter.execute(lease)
            first_adapter.close()

            recovered = ProductionAdapterFactory.open(reservoir)
            resumed_lease = WorkLease.create(
                reservoir_id=reservoir.reservoir_id,
                cursor_start=first_result.next_cursor,
                max_records=1,
                max_requests=1,
                max_bytes=4096,
                max_seconds=10,
            )
            records, result = recovered.execute(resumed_lease)
            observation = next(iter(recovered.extract_hosts(next(records))))
            recovered.close()

        self.assertEqual(observation.hostname, "second.com")
        self.assertEqual(observation.source_time, "19990101000000")
        self.assertEqual(result.requests, 1)

    def test_compressed_url_list_uses_single_year_as_hint_not_direct_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "webbase-2001.urls.gz"
            with gzip.open(path, "wt", encoding="utf-8") as target:
                target.write("http://novel.example/path\n")
            reservoir = Reservoir(
                reservoir_id="reservoir:webbase",
                domain_id="domain:webbase",
                adapter_id="structured:webbase",
                root_locator=str(path),
                enumeration_kind="structured_records",
                capacity_lower=1,
                state=ReservoirState.READY,
                evidence_mode="discovery_only",
            )
            adapter = ProductionAdapterFactory.open(
                reservoir,
                temporal_scope=(2001, 2001),
            )
            lease = WorkLease.create(
                reservoir_id=reservoir.reservoir_id,
                max_records=1,
                max_requests=1,
                max_bytes=4096,
                max_seconds=10,
            )
            records, _result = adapter.execute(lease)
            observation = next(iter(adapter.extract_hosts(next(records))))
            adapter.close()

        self.assertEqual(observation.hostname, "novel.example")
        self.assertEqual(observation.source_year, 2001)
        self.assertEqual(observation.year_hint_mask, 1 << (2001 - 1996))
        self.assertEqual(observation.direct_year_mask, 0)

    def test_jsonl_and_csv_explicit_years_become_hints(self) -> None:
        fixtures = (
            (
                "records.jsonl",
                '{"url":"https://json.example/a","year":1999}\n',
                "json.example",
                1999,
            ),
            (
                "records.csv",
                "https://csv.example/a,1998\n",
                "csv.example",
                1998,
            ),
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            observations = []
            for index, (name, content, hostname, year) in enumerate(fixtures):
                path = root / name
                path.write_text(content, encoding="utf-8")
                reservoir = Reservoir(
                    reservoir_id=f"reservoir:structured-{index}",
                    domain_id=f"domain:structured-{index}",
                    adapter_id=f"structured:structured-{index}",
                    root_locator=str(path),
                    enumeration_kind="structured_records",
                    capacity_lower=1,
                    state=ReservoirState.READY,
                    evidence_mode="discovery_only",
                )
                adapter = ProductionAdapterFactory.open(
                    reservoir,
                    temporal_scope=(1996, 2001),
                )
                lease = WorkLease.create(
                    reservoir_id=reservoir.reservoir_id,
                    max_records=1,
                    max_requests=1,
                    max_bytes=4096,
                    max_seconds=10,
                )
                records, _result = adapter.execute(lease)
                observation = next(iter(adapter.extract_hosts(next(records))))
                adapter.close()
                observations.append((observation, hostname, year))

        for observation, hostname, year in observations:
            self.assertEqual(observation.hostname, hostname)
            self.assertEqual(observation.source_year, year)
            self.assertEqual(observation.year_hint_mask, 1 << (year - 1996))
            self.assertEqual(observation.direct_year_mask, 0)

    def test_csv_title_with_brackets_and_slash_does_not_abort_url_extraction(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "pandora.csv"
            path.write_text(
                "tep_id,name,gathered_url,surt\n"
                "/tep/134347,2001 Local Government Elections [results] / "
                "Electoral Commission of Queensland,"
                "http://www.ecq.qld.gov.au/elections/local/LG2012/groupIndex.html,"
                '"au,gov,qld,ecq)/elections/local/lg2012/groupindex.html"\n',
                encoding="utf-8",
            )
            reservoir = Reservoir(
                reservoir_id="reservoir:pandora",
                domain_id="domain:pandora",
                adapter_id="structured:pandora",
                root_locator=str(path),
                enumeration_kind="structured_records",
                capacity_lower=1,
                state=ReservoirState.READY,
                evidence_mode="discovery_only",
            )
            adapter = ProductionAdapterFactory.open(reservoir)
            lease = WorkLease.create(
                reservoir_id=reservoir.reservoir_id,
                max_records=1,
                max_requests=1,
                max_bytes=4096,
                max_seconds=10,
            )
            records, _result = adapter.execute(lease)
            observation = next(iter(adapter.extract_hosts(next(records))))
            adapter.close()

        self.assertEqual(observation.hostname, "www.ecq.qld.gov.au")
        self.assertIsNone(observation.source_year)
        self.assertEqual(observation.year_hint_mask, 0)

    def test_csv_record_year_overrides_single_year_source_prior(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "webbase-2001.csv"
            path.write_text(
                "https://row.example/a,1998\n",
                encoding="utf-8",
            )
            reservoir = Reservoir(
                reservoir_id="reservoir:csv-priority",
                domain_id="domain:csv-priority",
                adapter_id="structured:csv-priority",
                root_locator=str(path),
                enumeration_kind="structured_records",
                capacity_lower=1,
                state=ReservoirState.READY,
                evidence_mode="discovery_only",
            )
            adapter = ProductionAdapterFactory.open(
                reservoir,
                temporal_scope=(2001, 2001),
            )
            lease = WorkLease.create(
                reservoir_id=reservoir.reservoir_id,
                max_records=1,
                max_requests=1,
                max_bytes=4096,
                max_seconds=10,
            )
            records, _result = adapter.execute(lease)
            observation = next(iter(adapter.extract_hosts(next(records))))
            adapter.close()

        self.assertEqual(observation.hostname, "row.example")
        self.assertEqual(observation.source_year, 1998)
        self.assertEqual(observation.source_time, "1998")
        self.assertEqual(observation.year_hint_mask, 1 << (1998 - 1996))
        self.assertEqual(observation.direct_year_mask, 0)

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
