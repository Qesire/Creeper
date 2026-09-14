from __future__ import annotations

import io
import unittest
import zipfile
from unittest.mock import patch

from creeper.authority.baseline_index import YEAR_BITS
from creeper.evidence.planner import EvidencePlanner
from creeper.evidence.contracts import (
    FTP_SITELIST_DIRECT_CONTRACT,
    bind_contract_to_adapter_id,
    discovery_only_contract,
)
from creeper.scheduler.leases import WorkLease
from creeper.sources.ftp_sitelist import AUDITED_FTP_SITELIST_LOCATORS
from creeper.sources.production import FtpSitelistProductionAdapter
from creeper.sources.reservoirs import Reservoir, ReservoirState


AUDITED_URL = next(iter(AUDITED_FTP_SITELIST_LOCATORS))


def _payload() -> bytes:
    target = io.BytesIO()
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "part01.txt",
            (
                "Site: ftp.old.example\nDate: 10-Nov-95\n"
                "Site: ftp.first.example\nDate: 19-Nov-96\n"
                "Site: ftp.second.org\nDate: 03-Jan-97\n"
            ),
        )
    return target.getvalue()


class _OpenBytes:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def open(self):
        return io.BytesIO(self.payload)


def _lease(reservoir: Reservoir, *, cursor: str | None = None) -> WorkLease:
    return WorkLease.create(
        reservoir_id=reservoir.reservoir_id,
        cursor_start=cursor,
        max_records=1,
        max_requests=1,
        max_bytes=1024 * 1024,
        max_seconds=30,
    ).grant(owner="test")


class FtpSitelistProductionAdapterTests(unittest.TestCase):
    def test_audited_sitelist_emits_direct_year_and_resumable_cursor(self) -> None:
        reservoir = Reservoir(
            reservoir_id="reservoir:ftp-list",
            domain_id="domain:ftp-list",
            adapter_id=bind_contract_to_adapter_id(
                "ftp_sitelist:test",
                FTP_SITELIST_DIRECT_CONTRACT,
            ),
            root_locator=AUDITED_URL,
            enumeration_kind="structured_records",
            capacity_lower=0,
            evidence_mode="direct_year",
            state=ReservoirState.READY,
        )
        adapter = FtpSitelistProductionAdapter(reservoir)
        payload = _payload()

        first_records = []
        with patch(
            "creeper.sources.production.fsspec.open",
            return_value=_OpenBytes(payload),
        ):
            first_result = adapter.execute_stream(
                _lease(reservoir),
                first_records.append,
            )

        self.assertEqual(first_result.requests, 1)
        self.assertEqual(first_result.bytes_read, len(payload))
        self.assertEqual(first_result.next_cursor, "record:2")
        self.assertEqual(len(first_records), 1)
        first = first_records[0]
        self.assertEqual(first.payload, "ftp.first.example")
        self.assertEqual(first.source_time, "1996-11-19")
        self.assertEqual(first.direct_year_mask, YEAR_BITS[1996])
        self.assertEqual(first.year_hint_mask, 0)
        self.assertEqual(
            first.evidence_contract_id,
            FTP_SITELIST_DIRECT_CONTRACT.contract_id,
        )

        second_records = []
        second_result = adapter.execute_stream(
            _lease(reservoir, cursor=first_result.next_cursor),
            second_records.append,
        )
        self.assertEqual(second_result.requests, 0)
        self.assertEqual(second_result.bytes_read, 0)
        self.assertIsNone(second_result.next_cursor)
        self.assertEqual(len(second_records), 1)
        second = second_records[0]
        self.assertEqual(second.payload, "ftp.second.org")
        self.assertEqual(second.source_time, "1997-01-03")
        self.assertEqual(second.direct_year_mask, YEAR_BITS[1997])

        observation = next(iter(adapter.extract_hosts(second)))
        self.assertEqual(observation.hostname, "ftp.second.org")
        self.assertEqual(observation.source_time, "1997-01-03")
        self.assertEqual(observation.direct_year_mask, YEAR_BITS[1997])

        plan = EvidencePlanner().plan(
            observation,
            official_mask=0,
            local_mask=0,
            provider="wayback",
            policy_version="runtime-policy",
            allow_direct=True,
        )
        self.assertEqual(plan.external_keys, ())
        self.assertEqual(len(plan.direct_capsules), 1)
        capsule = plan.direct_capsules[0]
        self.assertEqual(capsule.hostname, "ftp.second.org")
        self.assertEqual(capsule.year, 1997)
        self.assertEqual(capsule.evidence_timestamp, "1997-01-03")
        self.assertEqual(
            capsule.evidence_type,
            FTP_SITELIST_DIRECT_CONTRACT.evidence_type,
        )
        self.assertIn("part01.txt", capsule.record_locator)

    def test_unreviewed_same_name_stays_discovery_only(self) -> None:
        contract = discovery_only_contract("ftp_sitelist_zip")
        reservoir = Reservoir(
            reservoir_id="reservoir:ftp-list-unreviewed",
            domain_id="domain:ftp-list-unreviewed",
            adapter_id=bind_contract_to_adapter_id(
                "ftp_sitelist:unreviewed",
                contract,
            ),
            root_locator="https://unreviewed.example/ftp-list.zip",
            enumeration_kind="structured_records",
            capacity_lower=0,
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )
        adapter = FtpSitelistProductionAdapter(reservoir)
        records = []
        with patch(
            "creeper.sources.production.fsspec.open",
            return_value=_OpenBytes(_payload()),
        ):
            adapter.execute_stream(_lease(reservoir), records.append)

        self.assertEqual(records[0].direct_year_mask, 0)
        self.assertEqual(records[0].year_hint_mask, YEAR_BITS[1996])
        self.assertEqual(records[0].evidence_contract_id, "")


if __name__ == "__main__":
    unittest.main()
