from __future__ import annotations

import importlib.util
import io
import types
import unittest
from pathlib import Path

MODULE = Path(__file__).resolve().parents[2] / 'src/creeper/sources/archive/warc.py'
spec = importlib.util.spec_from_file_location('creeper_warc_under_test', MODULE)
warc = importlib.util.module_from_spec(spec)
assert spec.loader is not None
import sys
sys.modules[spec.name] = warc
spec.loader.exec_module(warc)


class Headers:
    def __init__(self, values):
        self.values = values
    def get_header(self, key):
        return self.values.get(key)


class Record:
    def __init__(self, kind, uri, date):
        self.rec_type = kind
        self.rec_headers = Headers({'WARC-Target-URI': uri, 'WARC-Date': date})


RECORDS = [
    (0, 10, Record('response', 'http://a.example/x', '1999-01-01T00:00:00Z')),
    (12, 8, Record('request', 'http://ignored.example/x', '1999-01-01T00:00:00Z')),
    (22, 9, Record('resource', 'http://b.example/x', '2001-02-03T00:00:00Z')),
    (33, 7, Record('revisit', 'http://late.example/x', '2005-01-01T00:00:00Z')),
]


class FakeIterator:
    def __init__(self, stream):
        start = stream.tell()
        self.items = [row for row in RECORDS if row[0] >= start]
        self.pos = -1
    def __iter__(self):
        return self
    def __next__(self):
        self.pos += 1
        if self.pos >= len(self.items):
            raise StopIteration
        return self.items[self.pos][2]
    def get_record_offset(self):
        return self.items[self.pos][0]
    def get_record_length(self):
        return self.items[self.pos][1]


def fake_archive_iterator(stream):
    return FakeIterator(stream)


class UnknownSizeStream:
    """Seekable stream that forbids seek-from-end to model remote range files."""

    def __init__(self, payload: bytes):
        self._stream = io.BytesIO(payload)

    def seekable(self):
        return True

    def tell(self):
        return self._stream.tell()

    def read(self, size=-1):
        return self._stream.read(size)

    def seek(self, offset, whence=0):
        if whence == 2:
            raise AssertionError("production reader must not probe remote EOF")
        return self._stream.seek(offset, whence)


class InvalidLengthIterator(FakeIterator):
    def get_record_length(self):
        return 0


class OversizedRecord(Record):
    def __init__(self, kind, uri, date, length):
        super().__init__(kind, uri, date)
        self.length = length


class OversizedIterator:
    def __init__(self, stream):
        self.stream = stream
        self.record = OversizedRecord(
            'response',
            'http://oversized.example/x',
            '1999-01-01T00:00:00Z',
            128 * 1024 * 1024,
        )
        self.done = False
        self.offset_requested = False
        self.length_requested = False

    def __iter__(self):
        return self

    def __next__(self):
        if self.done:
            raise StopIteration
        self.done = True
        return self.record

    def get_record_offset(self):
        self.offset_requested = True
        raise AssertionError('oversized record must be rejected before offset finalization')

    def get_record_length(self):
        self.length_requested = True
        raise AssertionError('oversized record must be rejected before payload finalization')


class WarcCursorTests(unittest.TestCase):
    def setUp(self):
        # Record gaps are only CR/LF separators. Bytes at record starts are non-whitespace.
        data = bytearray(b'X' * 40)
        data[10:12] = b'\r\n'
        data[20:22] = b'\r\n'
        data[31:33] = b'\r\n'
        self.stream = io.BytesIO(bytes(data))
        self.old = warc._archive_iterator
        warc._archive_iterator = fake_archive_iterator

    def tearDown(self):
        warc._archive_iterator = self.old

    def test_two_leases_resume_without_replaying_from_zero(self):
        first = warc.read_warc_metadata_lease(
            self.stream,
            max_scanned_records=2,
            max_archive_bytes=1000,
        )
        self.assertFalse(first.exhausted)
        self.assertEqual(first.scanned_records, 2)
        self.assertEqual([r.target_uri for r in first.records], ['http://a.example/x'])
        self.assertEqual(first.next_cursor, 'warc-byte:20')
        self.assertEqual(first.end_offset, 20)
        self.assertEqual(first.bytes_advanced, 20)

        second = warc.read_warc_metadata_lease(
            self.stream,
            cursor=first.next_cursor,
            max_scanned_records=10,
            max_archive_bytes=1000,
        )
        self.assertTrue(second.exhausted)
        self.assertEqual(second.start_offset, 22)
        self.assertEqual(second.scanned_records, 2)
        self.assertEqual(second.end_offset, 40)
        self.assertEqual(second.bytes_advanced, 18)
        self.assertEqual([r.target_uri for r in second.records], ['http://b.example/x'])
        self.assertIsNone(second.next_cursor)

    def test_archive_byte_budget_stops_after_record_boundary(self):
        result = warc.read_warc_metadata_lease(
            self.stream,
            max_scanned_records=100,
            max_archive_bytes=15,
        )
        self.assertFalse(result.exhausted)
        self.assertEqual(result.scanned_records, 2)
        self.assertEqual(result.next_offset, 20)

    def test_cursor_fails_closed(self):
        with self.assertRaises(warc.WarcCursorError):
            warc.decode_warc_cursor('12')
        with self.assertRaises(warc.WarcCursorError):
            warc.read_warc_metadata_lease(
                self.stream,
                cursor='warc-byte:999',
                max_scanned_records=1,
                max_archive_bytes=1,
            )

    def test_non_target_year_does_not_become_output(self):
        result = warc.read_warc_metadata_lease(
            self.stream,
            max_scanned_records=100,
            max_archive_bytes=1000,
        )
        self.assertEqual([r.source_year for r in result.records], [1999, 2001])
        self.assertEqual([r.record_type for r in result.records], ['response', 'resource'])

    def test_unknown_size_seekable_stream_is_not_probed_at_eof(self):
        stream = UnknownSizeStream(self.stream.getvalue())
        result = warc.read_warc_metadata_lease(
            stream,
            max_scanned_records=1,
            max_archive_bytes=1000,
        )
        self.assertFalse(result.exhausted)
        self.assertEqual(result.next_cursor, 'warc-byte:10')

    def test_non_positive_record_length_fails_closed(self):
        old = warc._archive_iterator
        warc._archive_iterator = lambda stream: InvalidLengthIterator(stream)
        try:
            with self.assertRaisesRegex(warc.WarcFormatError, 'non-monotonic or non-positive'):
                warc.read_warc_metadata_lease(
                    self.stream,
                    max_scanned_records=1,
                    max_archive_bytes=1000,
                )
        finally:
            warc._archive_iterator = old

    def test_oversized_declared_record_is_rejected_before_payload_finalization(self):
        holder = {}
        old = warc._archive_iterator

        def oversized(stream):
            iterator = OversizedIterator(stream)
            holder['iterator'] = iterator
            return iterator

        warc._archive_iterator = oversized
        try:
            with self.assertRaisesRegex(warc.WarcResourceLimitError, 'declared content length'):
                warc.read_warc_metadata_lease(
                    self.stream,
                    max_scanned_records=1,
                    max_archive_bytes=1000,
                    max_record_content_bytes=1024 * 1024,
                )
        finally:
            warc._archive_iterator = old

        iterator = holder['iterator']
        self.assertFalse(iterator.offset_requested)
        self.assertFalse(iterator.length_requested)

    def test_parser_eof_is_normalized_to_format_error(self):
        old = warc._archive_iterator

        def broken(_stream):
            raise EOFError("truncated record")

        warc._archive_iterator = broken
        try:
            with self.assertRaisesRegex(warc.WarcFormatError, "truncated record"):
                warc.read_warc_metadata_lease(
                    self.stream,
                    max_scanned_records=1,
                    max_archive_bytes=1000,
                )
        finally:
            warc._archive_iterator = old


if __name__ == '__main__':
    unittest.main()
