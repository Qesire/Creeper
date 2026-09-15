from __future__ import annotations

import unittest
from unittest.mock import patch

from creeper.authority.baseline_index import YEAR_BITS
from creeper.evidence.contracts import (
    SQUID_ACCESS_DIRECT_CONTRACT,
    bind_contract_to_adapter_id,
)
from creeper.scheduler.leases import WorkLease
from creeper.sources.format_binding import (
    SourceFormatObservation,
    bind_format_to_adapter_id,
)
from creeper.sources.production import StructuredProductionAdapter
from creeper.sources.reservoirs import Reservoir, ReservoirState


class _OpenFile:
    def __init__(self, payload: bytes, *, seekable: bool) -> None:
        self._payload = payload
        self._offset = 0
        self._seekable = seekable
        self.closed = False

    def open(self):
        return self

    def tell(self):
        return self._offset

    def seek(self, offset: int):
        if not self._seekable and offset != self._offset:
            raise ValueError("The HTTP server doesn't appear to support range requests")
        self._offset = offset

    def read(self, size: int = -1):
        if size < 0:
            size = len(self._payload) - self._offset
        result = self._payload[self._offset:self._offset + size]
        self._offset += len(result)
        return result

    def readline(self, size: int = -1):
        remaining = self._payload[self._offset:]
        if size >= 0:
            remaining = remaining[:size]
        newline = remaining.find(b"\n")
        if newline >= 0:
            remaining = remaining[:newline + 1]
        self._offset += len(remaining)
        return remaining

    def close(self):
        self.closed = True


class StructuredProductionAdapterTests(unittest.TestCase):
    def test_cursor_parser_rejects_non_string_cursor(self):
        with self.assertRaisesRegex(ValueError, "expected byte"):
            StructuredProductionAdapter._cursor_value(1)

    def test_opaque_locator_uses_frozen_jsonl_gzip_format_binding(self):
        observation = SourceFormatObservation(
            parser_kind="jsonl",
            compression="gzip",
            detection_method="content_signature",
            confidence=0.97,
            content_type="application/octet-stream",
        )
        reservoir = Reservoir(
            reservoir_id="reservoir:opaque",
            domain_id="domain:opaque",
            adapter_id=bind_format_to_adapter_id(
                "structured:opaque",
                observation,
            ),
            root_locator="https://repo.example/api/download?id=opaque",
            enumeration_kind="structured_records",
            capacity_lower=0,
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )

        adapter = StructuredProductionAdapter(reservoir)

        self.assertEqual(adapter.kind, "jsonl")
        self.assertTrue(adapter.compressed)
        self.assertEqual(adapter.evidence_contract.parser_kind, "jsonl")
        self.assertFalse(adapter.evidence_contract.grants_direct_web_year)

    def test_mailbox_adapter_persists_only_urls_and_year_hints(self):
        reservoir = Reservoir(
            reservoir_id="reservoir:mbox",
            domain_id="domain:mbox",
            adapter_id="structured:mbox",
            root_locator=(
                "https://lists.gnu.org/archive/mbox/lynx-dev/1998-03"
            ),
            enumeration_kind="structured_records",
            capacity_lower=0,
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )
        adapter = StructuredProductionAdapter(
            reservoir,
            temporal_scope=(1998, 1998),
        )

        record = adapter._generic_record(
            (
                "From: Person <person@example.net> see "
                "http://old.example/a and https://www.example.org/b."
            ),
            locator="fixture:1",
        )
        self.assertIsNotNone(record)
        assert record is not None
        record = adapter._apply_contract_authority(record)

        self.assertNotIn("person@", record.payload)
        self.assertNotIn("From:", record.payload)
        self.assertEqual(
            record.payload.split("\t"),
            ["http://old.example/a", "https://www.example.org/b"],
        )
        self.assertEqual(record.direct_year_mask, 0)
        self.assertEqual(record.year_hint_mask, YEAR_BITS[1998])

        observations = tuple(adapter.extract_hosts(record))
        self.assertEqual(
            {item.hostname for item in observations},
            {"old.example", "www.example.org"},
        )
        self.assertTrue(
            all(item.direct_year_mask == 0 for item in observations)
        )
        self.assertTrue(
            all(item.year_hint_mask == YEAR_BITS[1998] for item in observations)
        )

    def test_dmoz_adapter_keeps_dump_year_as_hint_not_evidence(self):
        reservoir = Reservoir(
            reservoir_id="reservoir:dmoz",
            domain_id="domain:dmoz",
            adapter_id="structured:dmoz",
            root_locator=(
                "https://mirror.example/dmoz/2001-01-22/content.rdf.u8.gz"
            ),
            enumeration_kind="structured_records",
            capacity_lower=0,
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )
        adapter = StructuredProductionAdapter(
            reservoir,
            temporal_scope=(2001, 2001),
        )

        record = adapter._generic_record(
            '<ExternalPage about="http://directory.example/path">',
            locator="fixture:1",
        )
        self.assertIsNotNone(record)
        assert record is not None
        record = adapter._apply_contract_authority(record)

        self.assertEqual(record.payload, "http://directory.example/path")
        self.assertEqual(record.record_type, "CURATED_DIRECTORY_URL")
        self.assertEqual(record.source_year, 2001)
        self.assertEqual(record.direct_year_mask, 0)
        self.assertEqual(record.year_hint_mask, YEAR_BITS[2001])

        observations = tuple(adapter.extract_hosts(record))
        self.assertEqual(
            [item.hostname for item in observations],
            ["directory.example"],
        )
        self.assertEqual(observations[0].direct_year_mask, 0)
        self.assertEqual(observations[0].year_hint_mask, YEAR_BITS[2001])

    def test_squid_adapter_uses_access_timestamp_as_direct_evidence(self):
        reservoir = Reservoir(
            reservoir_id="reservoir:squid",
            domain_id="domain:squid",
            adapter_id=bind_contract_to_adapter_id(
                "structured:squid",
                SQUID_ACCESS_DIRECT_CONTRACT,
            ),
            root_locator="https://trace.example/data/old.squid.log",
            enumeration_kind="structured_records",
            capacity_lower=0,
            evidence_mode="direct_year",
            state=ReservoirState.READY,
        )
        adapter = StructuredProductionAdapter(
            reservoir,
            temporal_scope=(1996, 2001),
        )

        record = adapter._generic_record(
            (
                "915148800.123 42 192.0.2.9 TCP_MISS/200 1234 GET "
                "http://old.example/path - DIRECT/203.0.113.8 text/html"
            ),
            locator="fixture:1",
        )
        self.assertIsNotNone(record)
        assert record is not None
        record = adapter._apply_contract_authority(record)

        self.assertEqual(record.payload, "http://old.example/path")
        self.assertEqual(record.source_year, 1999)
        self.assertEqual(record.source_time, "915148800.123")
        self.assertEqual(record.direct_year_mask, YEAR_BITS[1999])
        self.assertEqual(record.year_hint_mask, 0)
        self.assertEqual(
            record.evidence_contract_id,
            SQUID_ACCESS_DIRECT_CONTRACT.contract_id,
        )

    def test_legacy_squid_reservoir_does_not_silently_gain_authority(self):
        reservoir = Reservoir(
            reservoir_id="reservoir:squid-legacy",
            domain_id="domain:squid-legacy",
            adapter_id="structured:squid-legacy",
            root_locator="https://trace.example/data/old.squid.log",
            enumeration_kind="structured_records",
            capacity_lower=0,
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )
        adapter = StructuredProductionAdapter(
            reservoir,
            temporal_scope=(1996, 2001),
        )
        record = adapter._generic_record(
            (
                "915148800.123 42 192.0.2.9 TCP_MISS/200 1234 GET "
                "http://old.example/path - DIRECT/203.0.113.8 text/html"
            ),
            locator="fixture:legacy",
        )
        self.assertIsNotNone(record)
        assert record is not None
        record = adapter._apply_contract_authority(record)
        self.assertEqual(record.direct_year_mask, 0)
        self.assertEqual(record.year_hint_mask, YEAR_BITS[1999])

    def test_ircache_sanitized_access_locator_uses_squid_semantics(self):
        reservoir = Reservoir(
            reservoir_id="reservoir:ircache",
            domain_id="domain:ircache",
            adapter_id=bind_contract_to_adapter_id(
                "structured:ircache",
                SQUID_ACCESS_DIRECT_CONTRACT,
            ),
            root_locator=(
                "https://mirror.example/Traces/"
                "uc.sanitized-access.20000312.gz"
            ),
            enumeration_kind="structured_records",
            capacity_lower=0,
            evidence_mode="direct_year",
            state=ReservoirState.READY,
        )
        adapter = StructuredProductionAdapter(
            reservoir,
            temporal_scope=(1996, 2001),
        )

        self.assertEqual(adapter.kind, "squid_access")
        record = adapter._generic_record(
            (
                "952819200.000 42 192.0.2.9 TCP_MISS/200 1234 GET "
                "http://old.example/path - DIRECT/203.0.113.8 text/html"
            ),
            locator="fixture:1",
        )
        self.assertIsNotNone(record)
        assert record is not None
        record = adapter._apply_contract_authority(record)

        self.assertEqual(record.payload, "http://old.example/path")
        self.assertEqual(record.source_year, 2000)
        self.assertEqual(record.source_time, "952819200.000")
        self.assertEqual(record.direct_year_mask, YEAR_BITS[2000])
        self.assertEqual(record.year_hint_mask, 0)

    def test_wrapped_gzip_locator_preserves_parser_and_compression(self):
        reservoir = Reservoir(
            reservoir_id="reservoir:wrapped",
            domain_id="domain:wrapped",
            adapter_id="structured:wrapped",
            root_locator=(
                "https://repo.example/api/records/7/files/"
                "hosts.csv.gz/content"
            ),
            enumeration_kind="structured_records",
            capacity_lower=0,
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )

        adapter = StructuredProductionAdapter(reservoir)

        self.assertEqual(adapter.kind, "delimited")
        self.assertTrue(adapter.compressed)

    def test_non_range_http_source_reopens_as_stream_and_skips_cursor(self):
        payload = b"ignored.example\nkept.example\n"
        files = []

        def fake_open(_source, _mode, **options):
            file = _OpenFile(payload, seekable=options["block_size"] == 0)
            files.append(file)
            return file

        reservoir = Reservoir(
            reservoir_id="reservoir:stream",
            domain_id="domain:stream",
            adapter_id="structured:stream",
            root_locator="https://example.test/seeds.txt",
            enumeration_kind="structured_records",
            capacity_lower=2,
            capacity_upper=None,
            cursor="byte:16",
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )
        adapter = StructuredProductionAdapter(reservoir)
        lease = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            cursor_start="byte:16",
            max_records=1,
            max_requests=1,
            max_bytes=1024,
            max_seconds=30,
        ).grant(owner="test")

        records = []
        with patch("creeper.sources.production.fsspec.open", side_effect=fake_open):
            result = adapter.execute_stream(lease, records.append)

        self.assertEqual(len(files), 2)
        self.assertEqual(files[0]._seekable, False)
        self.assertEqual(files[1]._seekable, True)
        self.assertEqual([record.payload for record in records], ["kept.example"])
        self.assertEqual(result.requests, 2)
        self.assertEqual(result.next_cursor, "byte:29")


if __name__ == "__main__":
    unittest.main()
