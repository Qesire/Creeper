from __future__ import annotations

import gzip
import unittest

from creeper.sources.format_binding import (
    SourceFormatObservation,
    bind_format_to_adapter_id,
    format_from_adapter_id,
)
from creeper.sources.layout_binding import SourceRecordLayout
from creeper.sources.schema_binding import (
    SourceRecordSchema,
    bind_schema_to_adapter_id,
    schema_from_adapter_id,
)
from creeper.sources.schema_detection import detect_record_schema


class RecordSchemaDetectionTests(unittest.TestCase):
    def test_jsonl_stable_host_and_timestamp_fields_compile_direct_schema(self) -> None:
        payload = (
            b'{"target":"https://one.example/a","capture_year":1998}\n'
            b'{"target":"https://two.example/b","capture_year":1999}\n'
            b'{"target":"https://three.example/c","capture_year":2000}\n'
        )
        # Deterministic inference intentionally recognizes standard semantic
        # keys. Use "url" here; unknown custom keys are reserved for the LLM
        # adapter compiler rather than guessed.
        payload = payload.replace(b'"target"', b'"url"')
        fmt = SourceFormatObservation(
            parser_kind="jsonl",
            compression="none",
            detection_method="content_signature",
            confidence=0.97,
        )

        schema = detect_record_schema(
            payload=payload,
            format_observation=fmt,
        )

        self.assertIsNotNone(schema)
        assert schema is not None
        self.assertEqual(schema.parser_kind, "jsonl")
        self.assertEqual(schema.hostname_field, "url")
        self.assertEqual(schema.timestamp_field, "capture_year")
        self.assertEqual(schema.matched_records, 3)
        self.assertEqual(schema.sample_records, 3)
        self.assertEqual(schema.confidence, 1.0)
        self.assertTrue(schema.direct_year_eligible)
        contract = schema.direct_contract()
        self.assertTrue(contract.grants_direct_web_year)
        self.assertEqual(contract.hostname_field, "url")
        self.assertEqual(contract.timestamp_field, "capture_year")

    def test_layout_allows_custom_json_hostname_with_capture_timestamp(self) -> None:
        payload = (
            b'{"endpoint":"https://one.example/a","capture_timestamp":"19980101000000"}\n'
            b'{"endpoint":"https://two.example/b","capture_timestamp":"19990101000000"}\n'
            b'{"endpoint":"https://three.example/c","capture_timestamp":"20000101000000"}\n'
        )
        fmt = SourceFormatObservation(
            parser_kind="jsonl",
            compression="none",
            detection_method="content_signature",
            confidence=0.97,
        )
        layout = SourceRecordLayout(
            parser_kind="jsonl",
            hostname_field="endpoint",
            delimiter=None,
            detection_method="stable_json_host_field",
            confidence=1.0,
            sample_records=3,
            matched_records=3,
        )

        schema = detect_record_schema(
            payload=payload,
            format_observation=fmt,
            layout_observation=layout,
        )

        self.assertIsNotNone(schema)
        assert schema is not None
        self.assertEqual(schema.hostname_field, "endpoint")
        self.assertEqual(schema.timestamp_field, "capture_timestamp")
        self.assertEqual(
            schema.detection_method,
            "stable_json_layout_time_field",
        )
        self.assertTrue(schema.direct_year_eligible)
        contract = schema.direct_contract()
        self.assertEqual(contract.hostname_field, "endpoint")
        self.assertEqual(contract.timestamp_field, "capture_timestamp")

    def test_llm_hostname_layout_cannot_gain_direct_year_indirectly(self) -> None:
        payload = (
            b'{"endpoint":"https://one.example/a","capture_timestamp":"19980101000000"}\n'
            b'{"endpoint":"https://two.example/b","capture_timestamp":"19990101000000"}\n'
            b'{"endpoint":"https://three.example/c","capture_timestamp":"20000101000000"}\n'
        )
        fmt = SourceFormatObservation(
            parser_kind="jsonl",
            compression="none",
            detection_method="content_signature",
            confidence=0.97,
        )
        layout = SourceRecordLayout(
            parser_kind="jsonl",
            hostname_field="endpoint",
            delimiter=None,
            detection_method="llm_declarative_validated",
            confidence=1.0,
            sample_records=3,
            matched_records=3,
            policy_version="record-layout-llm-v1",
        )

        schema = detect_record_schema(
            payload=payload,
            format_observation=fmt,
            layout_observation=layout,
        )

        self.assertIsNotNone(schema)
        assert schema is not None
        self.assertEqual(schema.hostname_field, "endpoint")
        self.assertEqual(schema.timestamp_field, "capture_timestamp")
        self.assertFalse(schema.direct_year_eligible)
        with self.assertRaisesRegex(ValueError, "direct-year"):
            schema.direct_contract()

    def test_layout_custom_json_hostname_with_plain_year_stays_hint_only(self) -> None:
        payload = (
            b'{"endpoint":"https://one.example/a","year":1998}\n'
            b'{"endpoint":"https://two.example/b","year":1999}\n'
            b'{"endpoint":"https://three.example/c","year":2000}\n'
        )
        fmt = SourceFormatObservation(
            parser_kind="jsonl",
            compression="none",
            detection_method="content_signature",
            confidence=0.97,
        )
        layout = SourceRecordLayout(
            parser_kind="jsonl",
            hostname_field="endpoint",
            delimiter=None,
            detection_method="stable_json_host_field",
            confidence=1.0,
            sample_records=3,
            matched_records=3,
        )

        schema = detect_record_schema(
            payload=payload,
            format_observation=fmt,
            layout_observation=layout,
        )

        self.assertIsNotNone(schema)
        assert schema is not None
        self.assertEqual(schema.timestamp_field, "year")
        self.assertFalse(schema.direct_year_eligible)

    def test_layout_constrains_delimited_host_column_and_preserves_time_semantics(self) -> None:
        payload = (
            b"id,endpoint,timestamp\n"
            b"1,https://one.example/a,19980101000000\n"
            b"2,https://two.example/b,19990101000000\n"
            b"3,https://three.example/c,20000101000000\n"
        )
        fmt = SourceFormatObservation(
            parser_kind="delimited",
            compression="none",
            detection_method="content_signature",
            confidence=0.97,
            delimiter=",",
        )
        layout = SourceRecordLayout(
            parser_kind="delimited",
            hostname_field="column:1",
            delimiter=",",
            detection_method="stable_delimited_host_column",
            confidence=1.0,
            sample_records=3,
            matched_records=3,
        )

        schema = detect_record_schema(
            payload=payload,
            format_observation=fmt,
            layout_observation=layout,
        )

        self.assertIsNotNone(schema)
        assert schema is not None
        self.assertEqual(schema.hostname_field, "column:1")
        self.assertEqual(schema.timestamp_field, "column:2")
        self.assertTrue(schema.direct_year_eligible)

    def test_headerless_semicolon_table_infers_stable_columns(self) -> None:
        payload = (
            b"https://one.example/a;1998;foo\n"
            b"https://two.example/b;1999;bar\n"
            b"https://three.example/c;2000;baz\n"
        )
        fmt = SourceFormatObservation(
            parser_kind="delimited",
            compression="none",
            detection_method="content_signature",
            confidence=0.92,
        )

        schema = detect_record_schema(
            payload=payload,
            format_observation=fmt,
        )

        self.assertIsNotNone(schema)
        assert schema is not None
        self.assertEqual(schema.hostname_field, "column:0")
        self.assertEqual(schema.timestamp_field, "column:1")
        self.assertEqual(schema.delimiter, ";")
        self.assertEqual(schema.confidence, 1.0)
        self.assertFalse(schema.direct_year_eligible)
        with self.assertRaisesRegex(ValueError, "direct-year"):
            schema.direct_contract()

    def test_gzip_schema_detection_uses_bounded_decompressed_prefix(self) -> None:
        payload = gzip.compress(
            (
                "url,timestamp\n"
                "https://one.example/a,19980101000000\n"
                "https://two.example/b,19990101000000\n"
                "https://three.example/c,20000101000000\n"
            ).encode("utf-8")
        )
        fmt = SourceFormatObservation(
            parser_kind="delimited",
            compression="gzip",
            detection_method="content_signature",
            confidence=0.92,
        )

        schema = detect_record_schema(
            payload=payload,
            format_observation=fmt,
        )

        self.assertIsNotNone(schema)
        assert schema is not None
        self.assertEqual(schema.delimiter, ",")
        self.assertEqual(schema.hostname_field, "column:0")
        self.assertEqual(schema.timestamp_field, "column:1")
        self.assertTrue(schema.direct_year_eligible)

    def test_stable_but_ambiguous_year_field_is_not_auto_direct(self) -> None:
        payload = (
            b'{"url":"https://one.example/a","year":1998}\n'
            b'{"url":"https://two.example/b","year":1999}\n'
            b'{"url":"https://three.example/c","year":2000}\n'
        )
        fmt = SourceFormatObservation(
            parser_kind="jsonl",
            compression="none",
            detection_method="content_signature",
            confidence=0.97,
        )

        schema = detect_record_schema(
            payload=payload,
            format_observation=fmt,
        )

        self.assertIsNotNone(schema)
        assert schema is not None
        self.assertEqual(schema.timestamp_field, "year")
        self.assertFalse(schema.direct_year_eligible)
        with self.assertRaisesRegex(ValueError, "direct-year"):
            schema.direct_contract()

    def test_unstable_field_pairs_do_not_gain_direct_schema(self) -> None:
        payload = (
            b'{"url":"https://one.example/a","year":1998}\n'
            b'{"host":"two.example","timestamp":"19990101000000"}\n'
            b'{"domain":"three.example","date":"2000-01-01"}\n'
        )
        fmt = SourceFormatObservation(
            parser_kind="jsonl",
            compression="none",
            detection_method="content_signature",
            confidence=0.97,
        )

        schema = detect_record_schema(
            payload=payload,
            format_observation=fmt,
        )

        self.assertIsNone(schema)

    def test_schema_token_composes_after_format_token(self) -> None:
        fmt = SourceFormatObservation(
            parser_kind="delimited",
            compression="none",
            detection_method="content_signature",
            confidence=0.92,
        )
        schema = SourceRecordSchema(
            parser_kind="delimited",
            hostname_field="column:0",
            timestamp_field="column:1",
            delimiter="|",
            detection_method="stable_delimited_columns",
            confidence=1.0,
            sample_records=4,
            matched_records=4,
        )
        adapter_id = bind_format_to_adapter_id("structured:fixture", fmt)
        adapter_id = bind_schema_to_adapter_id(adapter_id, schema)

        self.assertEqual(format_from_adapter_id(adapter_id), fmt)
        self.assertEqual(schema_from_adapter_id(adapter_id), schema)


if __name__ == "__main__":
    unittest.main()
