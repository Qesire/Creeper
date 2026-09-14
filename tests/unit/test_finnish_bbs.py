from __future__ import annotations

import io
import tempfile
import unittest
from pathlib import Path
import zipfile

from creeper.authority.baseline_index import YEAR_BITS
from creeper.evidence.contracts import (
    EvidenceAuthority,
    FINNISH_BBS_DIRECT_CONTRACT,
    bind_contract_to_adapter_id,
    parser_kind_from_locator,
    resolve_source_evidence_contract,
)
from creeper.scheduler.leases import WorkLease
from creeper.sources.finnish_bbs import (
    AUDITED_FINNISH_BBS_LOCATORS,
    is_audited_finnish_bbs_locator,
    is_finnish_bbs_locator,
    parse_finnish_bbs_text,
    parse_finnish_bbs_zip,
)
from creeper.sources.production import FinnishBbsProductionAdapter
from creeper.sources.reservoirs import Reservoir, ReservoirState


AUDITED_URL = "https://files.mpoli.fi/software/TEXTS/MISC/FI980225.ZIP"


def finnish_text(*, date: str = "25.2.1998", include_appendix: bool = True) -> str:
    appendix = """
      Net-osoitteet:
      Maintainer: juhmakin@cs.helsinki.fi
      http://appendix.example.invalid/not-a-bbs-record
""" if include_appendix else ""
    return f"""
                      Elektroniset 24h postilaatikot Suomessa
                      =======================================

                               Tilanne  : {date}

       nimi/softa            numero       modeemi(t)     net/node / sysop
    -----------------------------------------------------------------------------
    BRAHMAN               09-498 797   32b/HST+ 42/M  brahman.nullnet.fi
    AXsh                                         2 4  Jani Poijärvi
    -----------------------------------------------------------------------------
    DRAGON'S CAVE BBS     09-853 4258  34+ 42/M       info@dcbbs.nullnet.fi
    PCBoard                                           Sysop
    -----------------------------------------------------------------------------
    EMPIRE BBS            09-851 4274  34 42/M        (2:220/273)
    BBBS/2
    Uusimmat OS/2 tulevat tänne, myös telnet://janipa-i.pp.kolumbus.fi (ei 24h)
    -----------------------------------------------------------------------------
    METROPOLI             09-621 4233  8x34 42/M      mpoli.fi
    PCBoard
    telnet: mpoli.fi (pcboard), http://www.mpoli.fi/, ftp://mpoli.fi:21
    -----------------------------------------------------------------------------
    IMAGE WORLD BBS       017-550 6349 2x34+ 42/M     bbs.iwn.fi
    Daydream
    telnet:bbs.iwn.fi, ftp://bbs.iwn.fi:666
    -----------------------------------------------------------------------------
    IP ONLY               09-111 2222 34 42/M         192.0.2.44
    -----------------------------------------------------------------------------
{appendix}
"""


def finnish_zip(text: str | None = None) -> bytes:
    target = io.BytesIO()
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "fi980225.txt",
            (text or finnish_text()).encode("cp437", errors="replace"),
        )
        archive.writestr("README.TXT", b"mirror metadata is not evidence")
    return target.getvalue()


class FinnishBbsParserTests(unittest.TestCase):
    def test_main_table_binds_hosts_to_internal_edition_date(self) -> None:
        rows = parse_finnish_bbs_text(
            finnish_text(),
            member_name="fi980225.txt",
        )
        hosts = {row.hostname for row in rows}

        self.assertEqual(
            hosts,
            {
                "brahman.nullnet.fi",
                "janipa-i.pp.kolumbus.fi",
                "mpoli.fi",
                "www.mpoli.fi",
                "bbs.iwn.fi",
            },
        )
        self.assertTrue(all(row.source_time == "1998-02-25" for row in rows))
        self.assertTrue(all(row.year == 1998 for row in rows))

    def test_email_fido_ip_and_appendix_urls_are_not_evidence(self) -> None:
        rows = parse_finnish_bbs_text(
            finnish_text(),
            member_name="fi980225.txt",
        )
        hosts = {row.hostname for row in rows}

        self.assertNotIn("dcbbs.nullnet.fi", hosts)
        self.assertNotIn("cs.helsinki.fi", hosts)
        self.assertNotIn("appendix.example.invalid", hosts)
        self.assertNotIn("192.0.2.44", hosts)

    def test_missing_structural_boundary_fails_closed(self) -> None:
        self.assertEqual(
            parse_finnish_bbs_text(
                finnish_text(include_appendix=False),
                member_name="fi980225.txt",
            ),
            (),
        )
        self.assertEqual(
            parse_finnish_bbs_text(
                finnish_text(date="25.2.2008"),
                member_name="fi980225.txt",
            ),
            (),
        )

    def test_zip_parser_enforces_complete_bounded_artifact(self) -> None:
        payload = finnish_zip()
        rows = parse_finnish_bbs_zip(payload)
        self.assertGreaterEqual(len(rows), 5)

        with self.assertRaisesRegex(ValueError, "decompressed byte budget"):
            parse_finnish_bbs_zip(payload, max_decompressed_bytes=16)

    def test_locator_format_and_authority_are_separate(self) -> None:
        unknown = "https://unreviewed.example/FI980225.ZIP"

        self.assertTrue(is_finnish_bbs_locator(AUDITED_URL))
        self.assertTrue(is_finnish_bbs_locator(unknown))
        self.assertTrue(is_audited_finnish_bbs_locator(AUDITED_URL))
        self.assertFalse(is_audited_finnish_bbs_locator(unknown))
        self.assertEqual(parser_kind_from_locator(AUDITED_URL), "finnish_bbs_zip")

        audited = resolve_source_evidence_contract(AUDITED_URL)
        unreviewed = resolve_source_evidence_contract(unknown)
        self.assertEqual(audited, FINNISH_BBS_DIRECT_CONTRACT)
        self.assertEqual(audited.authority, EvidenceAuthority.DIRECT_WEB_YEAR)
        self.assertEqual(unreviewed.authority, EvidenceAuthority.DISCOVERY_ONLY)

    def test_production_emits_internal_date_as_direct_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "FI980225.ZIP"
            payload = finnish_zip()
            path.write_bytes(payload)
            reservoir = Reservoir(
                reservoir_id="reservoir:finnish-bbs",
                domain_id="domain:finnish-bbs",
                adapter_id=bind_contract_to_adapter_id(
                    "finnish_bbs:test",
                    FINNISH_BBS_DIRECT_CONTRACT,
                ),
                root_locator=str(path),
                enumeration_kind="structured_records",
                capacity_lower=5,
                evidence_mode="direct_year",
                state=ReservoirState.READY,
            )
            adapter = FinnishBbsProductionAdapter(
                reservoir,
                evidence_contract=FINNISH_BBS_DIRECT_CONTRACT,
            )
            lease = WorkLease.create(
                reservoir_id=reservoir.reservoir_id,
                max_records=20,
                max_requests=1,
                max_bytes=len(payload) + 1,
                max_seconds=10,
            ).grant(owner="test")

            records, result = adapter.execute(lease)
            record = next(records)
            observation = next(iter(adapter.extract_hosts(record)))

        self.assertEqual(result.requests, 1)
        self.assertEqual(record.source_time, "1998-02-25")
        self.assertEqual(record.direct_year_mask, YEAR_BITS[1998])
        self.assertEqual(record.year_hint_mask, 0)
        self.assertEqual(observation.source_time, "1998-02-25")
        self.assertEqual(observation.direct_year_mask, YEAR_BITS[1998])
        self.assertEqual(
            observation.evidence_contract_id,
            FINNISH_BBS_DIRECT_CONTRACT.contract_id,
        )

    def test_all_audited_locators_are_target_1998_editions(self) -> None:
        self.assertEqual(
            AUDITED_FINNISH_BBS_LOCATORS,
            {
                "https://files.mpoli.fi/software/TEXTS/MISC/FI980225.ZIP",
                "https://files.mpoli.fi/software/TEXTS/MISC/030698.ZIP",
                "https://files.mpoli.fi/software/TEXTS/COMPUTER/FI980701.ZIP",
                "https://files.mpoli.fi/software/TEXTS/COMPUTER/FI980916.ZIP",
            },
        )


if __name__ == "__main__":
    unittest.main()
