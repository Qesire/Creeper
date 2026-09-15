from __future__ import annotations

import gzip
import unittest

from creeper.evidence.contracts import (
    bind_contract_to_adapter_id,
    contract_from_adapter_id,
    discovery_only_contract,
)
from creeper.sources.format_binding import (
    SourceFormatObservation,
    bind_format_to_adapter_id,
    format_from_adapter_id,
)
from creeper.sources.format_detection import detect_source_format


class SourceFormatDetectionTests(unittest.TestCase):
    def test_unknown_jsonl_locator_is_detected_from_content_signature(self) -> None:
        payload = (
            b'{"url":"https://one.example/a","year":1998}\n'
            b'{"url":"https://two.example/b","year":1999}\n'
            b'{"url":"https://three.example/c","year":2000}\n'
        )

        observed = detect_source_format(
            locator="https://repo.example/api/download?id=17",
            payload=payload,
            content_type="text/plain",
        )

        self.assertIsNotNone(observed)
        assert observed is not None
        self.assertEqual(observed.parser_kind, "jsonl")
        self.assertEqual(observed.compression, "none")
        self.assertEqual(observed.detection_method, "content_signature")
        self.assertGreaterEqual(observed.confidence, 0.95)

    def test_unknown_gzip_tabular_locator_detects_compression_and_parser(self) -> None:
        payload = gzip.compress(
            (
                "host,year\n"
                "one.example,1998\n"
                "two.example,1999\n"
                "three.example,2000\n"
            ).encode("utf-8")
        )

        observed = detect_source_format(
            locator="https://repo.example/api/blob/123",
            payload=payload,
            content_type="application/octet-stream",
        )

        self.assertIsNotNone(observed)
        assert observed is not None
        self.assertEqual(observed.parser_kind, "delimited")
        self.assertEqual(observed.compression, "gzip")
        self.assertGreaterEqual(observed.confidence, 0.90)

    def test_unknown_cdxj_locator_is_detected_from_record_parser(self) -> None:
        payload = (
            b'com,one)/ 19980101000000 {"url":"http://one.com/a"}\n'
            b'com,two)/ 19990101000000 {"url":"http://two.com/b"}\n'
            b'com,three)/ 20000101000000 {"url":"http://three.com/c"}\n'
        )

        observed = detect_source_format(
            locator="https://repo.example/object/opaque",
            payload=payload,
            content_type="application/octet-stream",
        )

        self.assertIsNotNone(observed)
        assert observed is not None
        self.assertEqual(observed.parser_kind, "cdxj")
        self.assertEqual(observed.compression, "none")

    def test_random_prose_does_not_compile_into_a_source_parser(self) -> None:
        observed = detect_source_format(
            locator="https://repo.example/object/opaque",
            payload=(
                b"This is a paper about the history of the web.\n"
                b"It discusses datasets and archives conceptually.\n"
                b"No machine-readable host records are present.\n"
            ),
            content_type="text/plain",
        )
        self.assertIsNone(observed)

    def test_format_binding_round_trips_with_evidence_contract_binding(self) -> None:
        observation = SourceFormatObservation(
            parser_kind="jsonl",
            compression="gzip",
            detection_method="content_signature",
            confidence=0.97,
            content_type="application/octet-stream",
        )
        adapter_id = bind_format_to_adapter_id(
            "structured:fixture",
            observation,
        )
        adapter_id = bind_contract_to_adapter_id(
            adapter_id,
            discovery_only_contract("jsonl"),
        )

        self.assertEqual(format_from_adapter_id(adapter_id), observation)
        contract = contract_from_adapter_id(adapter_id)
        self.assertIsNotNone(contract)
        assert contract is not None
        self.assertEqual(contract.parser_kind, "jsonl")

    def test_format_binding_can_be_inserted_before_existing_evidence_token(self) -> None:
        contract = discovery_only_contract("delimited")
        adapter_id = bind_contract_to_adapter_id("structured:fixture", contract)
        observation = SourceFormatObservation(
            parser_kind="delimited",
            compression="none",
            detection_method="content_signature",
            confidence=0.92,
        )

        rebound = bind_format_to_adapter_id(adapter_id, observation)

        self.assertEqual(format_from_adapter_id(rebound), observation)
        self.assertEqual(contract_from_adapter_id(rebound), contract)


if __name__ == "__main__":
    unittest.main()
