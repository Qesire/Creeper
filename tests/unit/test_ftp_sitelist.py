from __future__ import annotations

import io
import unittest
import zipfile

from creeper.evidence.contracts import (
    EvidenceAuthority,
    FTP_SITELIST_DIRECT_CONTRACT,
    parser_kind_from_locator,
    resolve_source_evidence_contract,
)
from creeper.sources.ftp_sitelist import (
    AUDITED_FTP_SITELIST_LOCATORS,
    is_audited_ftp_sitelist_locator,
    is_ftp_sitelist_locator,
    parse_ftp_sitelist_text,
    parse_ftp_sitelist_zip,
)


AUDITED_URL = next(iter(AUDITED_FTP_SITELIST_LOCATORS))


def sitelist_zip(*members: tuple[str, str]) -> bytes:
    target = io.BytesIO()
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, text in members:
            archive.writestr(name, text)
    return target.getvalue()


class FtpSitelistParserTests(unittest.TestCase):
    def test_text_parser_binds_site_to_record_date(self) -> None:
        rows = parse_ftp_sitelist_text(
            """
Site   : ftp.alpha.example
Date   : 19-Nov-96
Source : maintainer@example.net

Site   : ftp.beta.org
Date   : 03-Jan-97
Source : maintainer@example.net

Site   : ftp.missing-date.example
Source : ignored@example.net
""",
            member_name="ftp-list.01",
        )

        self.assertEqual(
            [(row.hostname, row.source_time, row.year) for row in rows],
            [
                ("ftp.alpha.example", "1996-11-19", 1996),
                ("ftp.beta.org", "1997-01-03", 1997),
            ],
        )
        self.assertEqual(rows[0].member_name, "ftp-list.01")

    def test_zip_parser_deduplicates_same_host_date_across_members(self) -> None:
        payload = sitelist_zip(
            (
                "part01.txt",
                "Site: ftp.alpha.example\nDate: 19-Nov-96\n",
            ),
            (
                "part02.txt",
                "Site: ftp.alpha.example\nDate: 19-Nov-96\n"
                "Site: ftp.beta.org\nDate: 03-Jan-97\n",
            ),
        )

        rows = parse_ftp_sitelist_zip(payload)

        self.assertEqual(len(rows), 2)
        self.assertEqual(
            {(row.hostname, row.year) for row in rows},
            {("ftp.alpha.example", 1996), ("ftp.beta.org", 1997)},
        )

    def test_zip_parser_enforces_decompressed_budget(self) -> None:
        payload = sitelist_zip(
            ("part01.txt", "Site: ftp.alpha.example\nDate: 19-Nov-96\n"),
        )
        with self.assertRaisesRegex(ValueError, "decompressed byte budget"):
            parse_ftp_sitelist_zip(payload, max_decompressed_bytes=8)

    def test_locator_classification_separates_format_from_authority(self) -> None:
        mirror = "https://unreviewed.example/archive/ftp-list.zip"

        self.assertTrue(is_ftp_sitelist_locator(AUDITED_URL))
        self.assertTrue(is_ftp_sitelist_locator(mirror))
        self.assertTrue(is_audited_ftp_sitelist_locator(AUDITED_URL))
        self.assertFalse(is_audited_ftp_sitelist_locator(mirror))
        self.assertEqual(parser_kind_from_locator(AUDITED_URL), "ftp_sitelist_zip")

        audited = resolve_source_evidence_contract(AUDITED_URL)
        unreviewed = resolve_source_evidence_contract(mirror)
        self.assertEqual(audited, FTP_SITELIST_DIRECT_CONTRACT)
        self.assertEqual(audited.authority, EvidenceAuthority.DIRECT_WEB_YEAR)
        self.assertTrue(audited.grants_direct_web_year)
        self.assertEqual(unreviewed.authority, EvidenceAuthority.DISCOVERY_ONLY)
        self.assertFalse(unreviewed.grants_direct_web_year)


if __name__ == "__main__":
    unittest.main()
