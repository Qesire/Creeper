from __future__ import annotations

import io
import tempfile
import unittest
from pathlib import Path
import zipfile

from creeper.authority.baseline_index import YEAR_BITS
from creeper.evidence.contracts import (
    EvidenceAuthority,
    SBI_BBS_DIRECT_CONTRACT,
    bind_contract_to_adapter_id,
    parser_kind_from_locator,
    resolve_source_evidence_contract,
)
from creeper.scheduler.leases import WorkLease
from creeper.sources.production import SbiBbsProductionAdapter
from creeper.sources.reservoirs import Reservoir, ReservoirState
from creeper.sources.sbi_bbs import (
    AUDITED_SBI_BBS_LOCATORS,
    is_audited_sbi_bbs_locator,
    is_sbi_bbs_locator,
    parse_sbi_bbs_zip,
    parse_sbi_quick_list_text,
)


AUDITED_URL = next(iter(AUDITED_SBI_BBS_LOCATORS))


def sbi_text(*, rev_date: str = "01/01/97") -> str:
    return f"""
"QUICK" GUIDE TO INTERNET BBS's (SBI QUICK LIST)
SBIQ0197.LST (rev date: {rev_date})

System Name Telnet/Client Address
Web-Only BBSs start with http://
===============================================================================
A Clockwork Online clockwork.com
Construction OnLine 199.166.4.9 3004
Insight Media insight-media.co.uk 1961
Myndz Eye Dreams BBS http://aol.com/csrv/downloa
A Truncated Host cal022011.student.utwente.n
A Marker-Prefixed Host @excal.infoconex.com 1961
A Duplicate clockwork.com
TOTAL SYSTEMS LISTED: 7
[END OF LIST]
"""


def sbi_zip(text: str | None = None) -> bytes:
    target = io.BytesIO()
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("README.DOC", "Mirror time must not create evidence.")
        archive.writestr("SBIQ0197.LST", text or sbi_text())
    return target.getvalue()


class SbiBbsParserTests(unittest.TestCase):
    def test_quick_list_binds_hosts_to_internal_revision_date(self) -> None:
        rows = parse_sbi_quick_list_text(
            sbi_text(),
            member_name="SBIQ0197.LST",
        )

        self.assertEqual(
            [(row.hostname, row.source_time, row.year) for row in rows],
            [
                ("clockwork.com", "1997-01-01", 1997),
                ("insight-media.co.uk", "1997-01-01", 1997),
                ("aol.com", "1997-01-01", 1997),
            ],
        )
        self.assertTrue(all(row.member_name == "SBIQ0197.LST" for row in rows))

    def test_ip_addresses_and_duplicate_hosts_are_not_emitted(self) -> None:
        rows = parse_sbi_quick_list_text(
            sbi_text(),
            member_name="SBIQ0197.LST",
        )

        self.assertNotIn("199.166.4.9", {row.hostname for row in rows})
        self.assertEqual(
            sum(row.hostname == "clockwork.com" for row in rows),
            1,
        )

    def test_truncated_and_marker_prefixed_hostnames_fail_closed(self) -> None:
        rows = parse_sbi_quick_list_text(
            sbi_text(),
            member_name="SBIQ0197.LST",
        )
        hosts = {row.hostname for row in rows}
        self.assertNotIn("cal022011.student.utwente.n", hosts)
        self.assertNotIn("@excal.infoconex.com", hosts)
        self.assertNotIn("excal.infoconex.com", hosts)

    def test_quick_list_requires_complete_footer(self) -> None:
        truncated = sbi_text().replace("[END OF LIST]", "")
        self.assertEqual(
            parse_sbi_quick_list_text(
                truncated,
                member_name="SBIQ0197.LST",
            ),
            (),
        )
        missing_total = sbi_text().replace("TOTAL SYSTEMS LISTED: 7", "")
        self.assertEqual(
            parse_sbi_quick_list_text(
                missing_total,
                member_name="SBIQ0197.LST",
            ),
            (),
        )

    def test_internal_revision_date_must_match_member_edition(self) -> None:
        self.assertEqual(
            parse_sbi_quick_list_text(
                sbi_text(rev_date="02/01/97"),
                member_name="SBIQ0197.LST",
            ),
            (),
        )
        self.assertEqual(
            parse_sbi_quick_list_text(
                sbi_text(),
                member_name="SBIQ0297.LST",
            ),
            (),
        )

    def test_zip_parser_ignores_non_quick_members_and_enforces_budget(self) -> None:
        payload = sbi_zip()
        rows = parse_sbi_bbs_zip(payload)
        self.assertEqual(len(rows), 3)

        with self.assertRaisesRegex(ValueError, "decompressed byte budget"):
            parse_sbi_bbs_zip(payload, max_decompressed_bytes=8)

    def test_locator_format_and_authority_are_separate(self) -> None:
        unreviewed = "https://unreviewed.example/SBI0197.ZIP"

        self.assertTrue(is_sbi_bbs_locator(AUDITED_URL))
        self.assertTrue(is_sbi_bbs_locator(unreviewed))
        self.assertTrue(is_audited_sbi_bbs_locator(AUDITED_URL))
        self.assertFalse(is_audited_sbi_bbs_locator(unreviewed))
        self.assertEqual(parser_kind_from_locator(AUDITED_URL), "sbi_bbs_zip")

        audited = resolve_source_evidence_contract(AUDITED_URL)
        unknown = resolve_source_evidence_contract(unreviewed)
        self.assertEqual(audited, SBI_BBS_DIRECT_CONTRACT)
        self.assertEqual(audited.authority, EvidenceAuthority.DIRECT_WEB_YEAR)
        self.assertEqual(unknown.authority, EvidenceAuthority.DISCOVERY_ONLY)

    def test_production_adapter_emits_internal_date_as_direct_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "SBI0197.ZIP"
            payload = sbi_zip()
            path.write_bytes(payload)
            reservoir = Reservoir(
                reservoir_id="reservoir:sbi",
                domain_id="domain:sbi",
                adapter_id=bind_contract_to_adapter_id(
                    "sbi_bbs:test",
                    SBI_BBS_DIRECT_CONTRACT,
                ),
                root_locator=str(path),
                enumeration_kind="structured_records",
                capacity_lower=3,
                evidence_mode="direct_year",
                state=ReservoirState.READY,
            )
            adapter = SbiBbsProductionAdapter(reservoir)
            lease = WorkLease.create(
                reservoir_id=reservoir.reservoir_id,
                max_records=10,
                max_requests=1,
                max_bytes=len(payload) + 1,
                max_seconds=10,
            ).grant(owner="test")

            records, result = adapter.execute(lease)
            record = next(records)
            observation = next(iter(adapter.extract_hosts(record)))

        self.assertEqual(result.requests, 1)
        self.assertEqual(record.source_time, "1997-01-01")
        self.assertEqual(record.direct_year_mask, YEAR_BITS[1997])
        self.assertEqual(record.year_hint_mask, 0)
        self.assertEqual(observation.hostname, "clockwork.com")
        self.assertEqual(observation.source_time, "1997-01-01")
        self.assertEqual(observation.direct_year_mask, YEAR_BITS[1997])
        self.assertEqual(
            observation.evidence_contract_id,
            SBI_BBS_DIRECT_CONTRACT.contract_id,
        )


if __name__ == "__main__":
    unittest.main()
