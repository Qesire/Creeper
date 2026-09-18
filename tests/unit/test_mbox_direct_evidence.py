from __future__ import annotations

import io
import unittest
from unittest.mock import patch

from creeper.authority.baseline_index import YEAR_BITS
from creeper.evidence.contracts import (
    EvidenceAuthority,
    GNU_MBOX_DIRECT_CONTRACT,
    bind_contract_to_adapter_id,
    resolve_source_evidence_contract,
)
from creeper.evidence.planner import EvidencePlanner
from creeper.scheduler.leases import WorkLease
from creeper.sources.mailbox_records import (
    is_audited_gnu_mbox_locator,
    parse_mbox_messages,
)
from creeper.sources.production import (
    MboxMessageProductionAdapter,
    ProductionAdapterError,
)
from creeper.sources.reservoirs import Reservoir, ReservoirState


GNU_LOCATOR = "https://lists.gnu.org/archive/mbox/lynx-dev/1998-03"


def mbox_message(date: str | None, body: str, *, attachment: str | None = None) -> bytes:
    lines = [
        "From sender@example.org Wed Mar 25 10:38:28 1998",
        "From: Sender <sender@example.org>",
        "Subject: historical link",
    ]
    if date is not None:
        lines.append(f"Date: {date}")
    if attachment is None:
        lines.extend(["", body, ""])
    else:
        lines.extend(
            [
                "MIME-Version: 1.0",
                'Content-Type: multipart/mixed; boundary="BOUND"',
                "",
                "--BOUND",
                "Content-Type: text/plain; charset=utf-8",
                "",
                body,
                "--BOUND",
                "Content-Type: text/plain; charset=utf-8",
                "Content-Disposition: attachment; filename=links.txt",
                "",
                attachment,
                "--BOUND--",
                "",
            ]
        )
    return ("\n".join(lines)).encode()


class _OpenFile:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def open(self):
        return io.BytesIO(self.payload)


class GnuMboxDirectEvidenceTests(unittest.TestCase):
    def test_audited_locator_gets_direct_contract(self) -> None:
        self.assertTrue(is_audited_gnu_mbox_locator(GNU_LOCATOR))
        self.assertFalse(
            is_audited_gnu_mbox_locator(
                "https://example.test/archive/mbox/lynx-dev/1998-03"
            )
        )
        self.assertFalse(
            is_audited_gnu_mbox_locator(GNU_LOCATOR + "?download=1")
        )
        contract = resolve_source_evidence_contract(GNU_LOCATOR)
        self.assertEqual(contract, GNU_MBOX_DIRECT_CONTRACT)
        self.assertEqual(contract.authority, EvidenceAuthority.DIRECT_WEB_YEAR)

    def test_parser_keeps_message_dates_and_urls_separate(self) -> None:
        payload = (
            mbox_message(
                "Wed, 25 Mar 1998 10:38:28 -0600",
                "First https://first.example/a",
            )
            + mbox_message(
                "Thu, 26 Mar 1998 11:00:00 -0600",
                "Second http://second.example/b",
            )
        )
        rows = parse_mbox_messages(payload, locator=GNU_LOCATOR)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].urls, ("https://first.example/a",))
        self.assertEqual(rows[0].year, 1998)
        self.assertIn("25 Mar 1998", rows[0].source_time)
        self.assertEqual(rows[1].urls, ("http://second.example/b",))
        self.assertIn("26 Mar 1998", rows[1].source_time)

    def test_missing_bad_or_wrong_year_date_fails_closed(self) -> None:
        payload = (
            mbox_message(None, "https://missing.example/")
            + mbox_message("not a date", "https://bad.example/")
            + mbox_message(
                "Wed, 25 Mar 1999 10:38:28 -0600",
                "https://wrong-year.example/",
            )
        )
        self.assertEqual(parse_mbox_messages(payload, locator=GNU_LOCATOR), ())

    def test_parser_ignores_attachments_and_non_http_schemes(self) -> None:
        payload = mbox_message(
            "Wed, 25 Mar 1998 10:38:28 -0600",
            "See https://body.example/a ftp://ftp.example/pub gopher://g.example/1",
            attachment="https://attachment.example/hidden",
        )
        rows = parse_mbox_messages(payload, locator=GNU_LOCATOR)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].urls, ("https://body.example/a",))

    def test_parser_skips_nested_message_attachment_subtree(self) -> None:
        payload = (
            b"From sender@example.org Wed Mar 25 10:38:28 1998\n"
            b"Date: Wed, 25 Mar 1998 10:38:28 -0600\n"
            b"MIME-Version: 1.0\n"
            b'Content-Type: multipart/mixed; boundary="OUTER"\n'
            b"\n"
            b"--OUTER\n"
            b"Content-Type: text/plain; charset=utf-8\n"
            b"\n"
            b"https://body.example/live\n"
            b"--OUTER\n"
            b"Content-Type: message/rfc822\n"
            b"Content-Disposition: attachment; filename=forwarded.eml\n"
            b"\n"
            b"From: Nested <nested@example.org>\n"
            b"Date: Tue, 24 Mar 1998 10:00:00 -0600\n"
            b"Content-Type: text/plain\n"
            b"\n"
            b"https://attachment.example/hidden\n"
            b"--OUTER--\n"
        )

        rows = parse_mbox_messages(payload, locator=GNU_LOCATOR)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].urls, ("https://body.example/live",))

    def test_parser_caps_each_message_at_64_urls(self) -> None:
        urls = " ".join(f"https://h{i}.example/x" for i in range(80))
        payload = mbox_message("Wed, 25 Mar 1998 10:38:28 -0600", urls)
        rows = parse_mbox_messages(payload, locator=GNU_LOCATOR)
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(rows[0].urls), 64)
        with self.assertRaisesRegex(ValueError, "within \\[1, 64\\]"):
            parse_mbox_messages(
                payload,
                locator=GNU_LOCATOR,
                max_urls_per_message=65,
            )

    def test_truncated_prefix_drops_only_unbounded_tail_message(self) -> None:
        first = mbox_message(
            "Wed, 25 Mar 1998 10:38:28 -0600",
            "https://complete.example/a",
        )
        second = mbox_message(
            "Thu, 26 Mar 1998 11:00:00 -0600",
            "https://tail.example/b",
        )
        rows = parse_mbox_messages(
            first + second[:80],
            locator=GNU_LOCATOR,
            allow_truncated_tail=True,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].urls, ("https://complete.example/a",))

    def test_production_streams_messages_across_durable_byte_cursors(self) -> None:
        first = mbox_message(
            "Wed, 25 Mar 1998 10:38:28 -0600",
            "See https://direct.example/path",
        )
        second = mbox_message(
            "Thu, 26 Mar 1998 11:00:00 -0600",
            "See https://second.example/path",
        )
        payload = first + second
        reservoir = Reservoir(
            reservoir_id="reservoir:gnu",
            domain_id="domain:gnu",
            adapter_id=bind_contract_to_adapter_id(
                "mbox_messages:gnu",
                GNU_MBOX_DIRECT_CONTRACT,
            ),
            root_locator=GNU_LOCATOR,
            enumeration_kind="structured_records",
            capacity_lower=2,
            evidence_mode="direct_year",
            state=ReservoirState.READY,
        )
        adapter = MboxMessageProductionAdapter(reservoir)

        first_lease = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            max_records=1,
            max_requests=1,
            max_bytes=len(payload) + 128,
            max_seconds=10,
        ).grant(owner="test")
        with patch(
            "creeper.sources.production.fsspec.open",
            return_value=_OpenFile(payload),
        ):
            first_records, first_result = adapter.execute(first_lease)
            first_record = next(first_records)

            self.assertEqual(first_result.next_cursor, f"byte:{len(first)}")
            self.assertIn(":byte:0", first_record.locator)
            first_observation = next(iter(adapter.extract_hosts(first_record)))
            self.assertEqual(first_observation.hostname, "direct.example")
            self.assertEqual(first_observation.direct_year_mask, YEAR_BITS[1998])

            second_lease = WorkLease.create(
                reservoir_id=reservoir.reservoir_id,
                cursor_start=first_result.next_cursor,
                max_records=1,
                max_requests=1,
                max_bytes=len(second) + 128,
                max_seconds=10,
            ).grant(owner="test")
            second_records, second_result = adapter.execute(second_lease)
            second_record = next(second_records)

        self.assertIsNone(second_result.next_cursor)
        self.assertIn(f":byte:{len(first)}", second_record.locator)
        second_observation = next(iter(adapter.extract_hosts(second_record)))
        self.assertEqual(second_observation.hostname, "second.example")
        self.assertEqual(second_observation.direct_year_mask, YEAR_BITS[1998])

        plan = EvidencePlanner().plan(
            first_observation,
            official_mask=0,
            local_mask=0,
            provider="wayback",
            policy_version="runtime-policy",
            allow_direct=True,
        )
        self.assertEqual(plan.external_keys, ())
        self.assertEqual(len(plan.direct_capsules), 1)
        capsule = plan.direct_capsules[0]
        self.assertEqual(capsule.year, 1998)
        self.assertEqual(capsule.original_url, "https://direct.example/path")
        self.assertIn("25 Mar 1998", capsule.evidence_timestamp)
        self.assertIn(":byte:0", capsule.record_locator)

    def test_production_does_not_commit_partial_message(self) -> None:
        payload = mbox_message(
            "Wed, 25 Mar 1998 10:38:28 -0600",
            "See https://oversized.example/path " + ("x" * 512),
        )
        reservoir = Reservoir(
            reservoir_id="reservoir:gnu-budget",
            domain_id="domain:gnu-budget",
            adapter_id=bind_contract_to_adapter_id(
                "mbox_messages:gnu-budget",
                GNU_MBOX_DIRECT_CONTRACT,
            ),
            root_locator=GNU_LOCATOR,
            enumeration_kind="structured_records",
            capacity_lower=1,
            evidence_mode="direct_year",
            state=ReservoirState.READY,
        )
        adapter = MboxMessageProductionAdapter(reservoir)
        lease = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            max_records=1,
            max_requests=1,
            max_bytes=128,
            max_seconds=10,
        ).grant(owner="test")

        with patch(
            "creeper.sources.production.fsspec.open",
            return_value=_OpenFile(payload),
        ):
            with self.assertRaisesRegex(
                ProductionAdapterError,
                "exceeds lease max_bytes",
            ):
                adapter.execute(lease)

    def test_legacy_discovery_reservoir_does_not_silently_upgrade(self) -> None:
        payload = mbox_message(
            "Wed, 25 Mar 1998 10:38:28 -0600",
            "https://legacy.example/path",
        )
        reservoir = Reservoir(
            reservoir_id="reservoir:gnu-legacy",
            domain_id="domain:gnu-legacy",
            adapter_id="mbox_messages:gnu-legacy",
            root_locator=GNU_LOCATOR,
            enumeration_kind="structured_records",
            capacity_lower=1,
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )
        adapter = MboxMessageProductionAdapter(reservoir)
        lease = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            max_records=1,
            max_requests=1,
            max_bytes=len(payload) + 128,
            max_seconds=10,
        ).grant(owner="test")
        with patch(
            "creeper.sources.production.fsspec.open",
            return_value=_OpenFile(payload),
        ):
            records, _result = adapter.execute(lease)
            record = next(records)
        self.assertEqual(record.direct_year_mask, 0)
        self.assertEqual(record.year_hint_mask, YEAR_BITS[1998])


if __name__ == "__main__":
    unittest.main()
