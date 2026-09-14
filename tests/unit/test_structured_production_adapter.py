from __future__ import annotations

import unittest
from unittest.mock import patch

from creeper.scheduler.leases import WorkLease
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
    def test_dmoz_rdf_external_page_becomes_discovery_url_record(self):
        reservoir = Reservoir(
            reservoir_id="reservoir:dmoz",
            domain_id="domain:dmoz",
            adapter_id="structured:dmoz",
            root_locator="https://download.example/content.rdf.u8.gz",
            enumeration_kind="structured_records",
            capacity_lower=1,
            capacity_upper=None,
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )
        adapter = StructuredProductionAdapter(reservoir)

        record = adapter._generic_record(
            '<ExternalPage about="http://Example.COM/path?a=1&amp;b=2">',
            locator="fixture:1",
        )

        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.payload, "http://Example.COM/path?a=1&b=2")
        self.assertEqual(record.record_type, "STRUCTURED_RDF_EXTERNAL_PAGE")
        self.assertEqual(record.direct_year_mask, 0)
        self.assertEqual(
            [item.hostname for item in adapter.extract_hosts(record)],
            ["example.com"],
        )

    def test_dmoz_rdf_topic_line_is_not_emitted_twice(self):
        reservoir = Reservoir(
            reservoir_id="reservoir:dmoz",
            domain_id="domain:dmoz",
            adapter_id="structured:dmoz",
            root_locator="https://download.example/content.rdf.u8.gz",
            enumeration_kind="structured_records",
            capacity_lower=1,
            capacity_upper=None,
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )
        adapter = StructuredProductionAdapter(reservoir)

        self.assertIsNone(
            adapter._generic_record(
                '<link r:resource="http://example.com/"/>',
                locator="fixture:topic",
            )
        )

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
