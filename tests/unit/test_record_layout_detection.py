from __future__ import annotations

import unittest
from decimal import Decimal

from creeper.source_discovery.measured_scout import (
    MeasuredYieldScoutExecutor,
    MeasuredYieldScoutPolicy,
    SampleDownload,
)
from creeper.source_discovery.models import SourceCandidate, SourceLevel
from creeper.source_discovery.unknown_format import parse_unknown_format_reason
from creeper.sources.format_binding import SourceFormatObservation
from creeper.sources.layout_detection import detect_record_layout


class _EmptyBaseline:
    def resolve_batch(self, hosts):
        return {hostname: (0, False) for hostname in hosts}


class RecordLayoutDetectionTests(unittest.TestCase):
    @staticmethod
    def _format(parser_kind: str, *, delimiter: str | None = None):
        return SourceFormatObservation(
            parser_kind=parser_kind,
            compression="none",
            detection_method="fixture",
            confidence=1.0,
            content_type="application/octet-stream",
            delimiter=delimiter,
        )

    def test_jsonl_detects_nonstandard_hostname_field(self) -> None:
        payload = (
            b'{"endpoint":"https://a.example.com/x","label":"one"}\n'
            b'{"endpoint":"https://b.example.com/y","label":"two"}\n'
            b'{"endpoint":"https://c.example.com/z","label":"three"}\n'
        )
        layout = detect_record_layout(
            payload=payload,
            format_observation=self._format("jsonl"),
        )
        self.assertIsNotNone(layout)
        assert layout is not None
        self.assertEqual(layout.hostname_field, "endpoint")
        self.assertEqual(layout.matched_records, 3)
        self.assertEqual(layout.sample_records, 3)

    def test_jsonl_ambiguous_host_fields_fail_closed(self) -> None:
        payload = (
            b'{"left":"https://a.example.com/","right":"https://x.example.net/"}\n'
            b'{"left":"https://b.example.com/","right":"https://y.example.net/"}\n'
            b'{"left":"https://c.example.com/","right":"https://z.example.net/"}\n'
        )
        self.assertIsNone(
            detect_record_layout(
                payload=payload,
                format_observation=self._format("jsonl"),
            )
        )

    def test_delimited_detects_nonstandard_hostname_column(self) -> None:
        payload = (
            b"id,endpoint,label\n"
            b"1,https://a.example.com/x,one\n"
            b"2,https://b.example.com/y,two\n"
            b"3,https://c.example.com/z,three\n"
        )
        layout = detect_record_layout(
            payload=payload,
            format_observation=self._format("delimited", delimiter=","),
        )
        self.assertIsNotNone(layout)
        assert layout is not None
        self.assertEqual(layout.hostname_field, "column:1")
        self.assertEqual(layout.delimiter, ",")


class StructuredLayoutScoutRoutingTests(unittest.TestCase):
    @staticmethod
    def candidate() -> SourceCandidate:
        return SourceCandidate(
            canonical_entrypoint="https://data.example/opaque",
            source_family="LAYOUT_TEST",
            level=SourceLevel.SOURCE,
            discovered_by="fixture",
            discovery_strategy="fixture",
            expected_volume=1000,
            confidence=0.8,
        )

    @staticmethod
    def format_observation() -> SourceFormatObservation:
        return SourceFormatObservation(
            parser_kind="jsonl",
            compression="none",
            detection_method="fixture",
            confidence=1.0,
            content_type="application/octet-stream",
        )

    def executor(self) -> MeasuredYieldScoutExecutor:
        return MeasuredYieldScoutExecutor(
            None,  # _evaluate_download is pure with respect to the HTTP client.
            _EmptyBaseline(),
            {"com": Decimal("1")},
            policy=MeasuredYieldScoutPolicy(
                min_unique_hosts=3,
                min_novel_hosts=3,
                min_novel_fraction=1.0,
                min_novel_eed=3.0,
            ),
            format_resolver=lambda _key: self.format_observation(),
            clock=lambda: 1.0,
        )

    def test_stable_custom_field_is_resolved_without_llm(self) -> None:
        payload = (
            b'{"endpoint":"https://a.example.com/x","label":"one"}\n'
            b'{"endpoint":"https://b.example.com/y","label":"two"}\n'
            b'{"endpoint":"https://c.example.com/z","label":"three"}\n'
        )
        result = self.executor()._evaluate_download(
            self.candidate(),
            SampleDownload(
                payload=payload,
                content_type="application/octet-stream",
                truncated=False,
                requests=1,
                bytes_read=len(payload),
            ),
            started=0.0,
        )
        self.assertEqual(result.disposition.value, "WARM")
        self.assertIsNotNone(result.layout_observation)
        assert result.layout_observation is not None
        self.assertEqual(result.layout_observation.hostname_field, "endpoint")
        self.assertIsNone(result.schema_observation)
        self.assertIsNone(parse_unknown_format_reason(result.reason))

    def test_custom_host_with_capture_timestamp_becomes_host_year(self) -> None:
        payload = (
            b'{"endpoint":"https://a.example.com/x","capture_timestamp":"19980101000000"}\n'
            b'{"endpoint":"https://b.example.com/y","capture_timestamp":"19990101000000"}\n'
            b'{"endpoint":"https://c.example.com/z","capture_timestamp":"20000101000000"}\n'
        )
        result = self.executor()._evaluate_download(
            self.candidate(),
            SampleDownload(
                payload=payload,
                content_type="application/octet-stream",
                truncated=False,
                requests=1,
                bytes_read=len(payload),
            ),
            started=0.0,
        )
        self.assertEqual(result.disposition.value, "WARM")
        self.assertIsNotNone(result.layout_observation)
        self.assertIsNotNone(result.schema_observation)
        assert result.schema_observation is not None
        self.assertEqual(result.schema_observation.hostname_field, "endpoint")
        self.assertEqual(
            result.schema_observation.timestamp_field,
            "capture_timestamp",
        )
        self.assertTrue(result.schema_observation.direct_year_eligible)
        self.assertIsNotNone(result.measurement)
        assert result.measurement is not None
        self.assertEqual(result.measurement.measurement_mode.value, "HOST_YEAR")
        self.assertEqual(result.measurement.observed_host_year_pairs, 3)

    def test_ambiguous_custom_fields_enter_unknown_format_loop(self) -> None:
        payload = (
            b'{"left":"https://a.example.com/","right":"https://x.example.net/"}\n'
            b'{"left":"https://b.example.com/","right":"https://y.example.net/"}\n'
            b'{"left":"https://c.example.com/","right":"https://z.example.net/"}\n'
        )
        result = self.executor()._evaluate_download(
            self.candidate(),
            SampleDownload(
                payload=payload,
                content_type="application/octet-stream",
                truncated=False,
                requests=1,
                bytes_read=len(payload),
            ),
            started=0.0,
        )
        self.assertEqual(result.disposition.value, "HOLD")
        self.assertIsNone(result.layout_observation)
        self.assertIsNotNone(parse_unknown_format_reason(result.reason))


if __name__ == "__main__":
    unittest.main()
