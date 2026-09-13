from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.evidence.contracts import (
    EvidenceAuthority,
    SourceEvidenceContract,
    bind_contract_to_adapter_id,
    contract_from_adapter_id,
)
from creeper.evidence.planner import EvidencePlanner
from creeper.scheduler.leases import WorkLease
from creeper.sources.production import ProductionAdapterFactory
from creeper.sources.reservoirs import Reservoir, ReservoirState


def _lease(reservoir: Reservoir, *, cursor: str | None = None) -> WorkLease:
    return WorkLease.create(
        reservoir_id=reservoir.reservoir_id,
        cursor_start=cursor,
        max_records=1,
        max_requests=1,
        max_bytes=4096,
        max_seconds=10,
    )


def _direct_contract(
    parser_kind: str,
    *,
    hostname_field: str,
    timestamp_field: str | None,
    contract_id: str,
) -> SourceEvidenceContract:
    return SourceEvidenceContract(
        contract_id=contract_id,
        authority=EvidenceAuthority.DIRECT_WEB_YEAR,
        parser_kind=parser_kind,
        temporal_semantics="reviewed_web_observation_timestamp",
        evidence_type="reviewed_historical_web_record",
        hostname_field=hostname_field,
        timestamp_field=timestamp_field,
        policy_version="reviewed-source-policy-v1",
    )


class DirectEvidenceContractTests(unittest.TestCase):
    def test_trusted_jsonl_contract_produces_direct_year_and_capsule_provenance(self) -> None:
        contract = _direct_contract(
            "jsonl",
            hostname_field="url",
            timestamp_field="capture_year",
            contract_id="trusted-jsonl-v1",
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "records.jsonl"
            path.write_text(
                '{"url":"https://direct.example/a","capture_year":1998}\n',
                encoding="utf-8",
            )
            adapter_id = bind_contract_to_adapter_id(
                "structured:trusted-jsonl",
                contract,
            )
            reservoir = Reservoir(
                reservoir_id="reservoir:trusted-jsonl",
                domain_id="domain:trusted-jsonl",
                adapter_id=adapter_id,
                root_locator=str(path),
                enumeration_kind="structured_records",
                capacity_lower=1,
                evidence_mode="direct_year",
                state=ReservoirState.READY,
            )
            adapter = ProductionAdapterFactory.open(
                reservoir,
                temporal_scope=(1996, 2001),
            )
            records, _result = adapter.execute(_lease(reservoir))
            observation = next(iter(adapter.extract_hosts(next(records))))
            adapter.close()

        self.assertEqual(observation.hostname, "direct.example")
        self.assertEqual(observation.source_year, 1998)
        self.assertEqual(observation.direct_year_mask, 1 << (1998 - 1996))
        self.assertEqual(observation.year_hint_mask, 0)
        self.assertEqual(
            observation.evidence_contract_id,
            contract.contract_id,
        )
        self.assertEqual(
            observation.evidence_contract_version,
            contract.policy_version,
        )

        plan = EvidencePlanner().plan(
            observation,
            official_mask=0,
            local_mask=0,
            provider="wayback",
            policy_version="runtime-policy-v9",
            allow_direct=True,
        )
        self.assertEqual(plan.external_keys, ())
        self.assertEqual(len(plan.direct_capsules), 1)
        capsule = plan.direct_capsules[0]
        self.assertEqual(capsule.source_id, reservoir.reservoir_id)
        self.assertEqual(capsule.original_url, "https://direct.example/a")
        self.assertIn(":byte:0", capsule.record_locator)
        self.assertEqual(
            capsule.temporal_semantics,
            contract.temporal_semantics,
        )
        self.assertEqual(capsule.evidence_type, contract.evidence_type)
        self.assertEqual(capsule.policy_version, "runtime-policy-v9")
        self.assertIn(contract.contract_id, capsule.extraction_method)
        self.assertIn(contract.policy_version, capsule.extraction_method)

    def test_trusted_csv_contract_uses_explicit_columns_for_direct_year(self) -> None:
        contract = _direct_contract(
            "delimited",
            hostname_field="column:0",
            timestamp_field="column:1",
            contract_id="trusted-csv-v1",
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "records.csv"
            path.write_text(
                "https://csv-direct.example/a,1999,ignored\n",
                encoding="utf-8",
            )
            reservoir = Reservoir(
                reservoir_id="reservoir:trusted-csv",
                domain_id="domain:trusted-csv",
                adapter_id=bind_contract_to_adapter_id(
                    "structured:trusted-csv",
                    contract,
                ),
                root_locator=str(path),
                enumeration_kind="structured_records",
                capacity_lower=1,
                evidence_mode="direct_year",
                state=ReservoirState.READY,
            )
            adapter = ProductionAdapterFactory.open(reservoir)
            records, _result = adapter.execute(_lease(reservoir))
            observation = next(iter(adapter.extract_hosts(next(records))))
            adapter.close()

        self.assertEqual(observation.hostname, "csv-direct.example")
        self.assertEqual(observation.source_year, 1999)
        self.assertEqual(observation.direct_year_mask, 1 << (1999 - 1996))
        self.assertEqual(observation.year_hint_mask, 0)

    def test_arbitrary_dated_csv_remains_hint_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "random.csv"
            path.write_text(
                "https://random.example/a,1998\n",
                encoding="utf-8",
            )
            reservoir = Reservoir(
                reservoir_id="reservoir:random",
                domain_id="domain:random",
                adapter_id="structured:random",
                root_locator=str(path),
                enumeration_kind="structured_records",
                capacity_lower=1,
                evidence_mode="discovery_only",
                state=ReservoirState.READY,
            )
            adapter = ProductionAdapterFactory.open(reservoir)
            records, _result = adapter.execute(_lease(reservoir))
            observation = next(iter(adapter.extract_hosts(next(records))))
            adapter.close()

        self.assertEqual(observation.direct_year_mask, 0)
        self.assertEqual(observation.year_hint_mask, 1 << (1998 - 1996))

    def test_dns_observation_contract_never_grants_annual_web_capsule(self) -> None:
        contract = SourceEvidenceContract(
            contract_id="isc-dns-observation-v1",
            authority=EvidenceAuthority.DNS_OBSERVATION,
            parser_kind="delimited",
            temporal_semantics="dns_observation_timestamp",
            evidence_type="dns_observation",
            hostname_field="column:0",
            timestamp_field="column:1",
            policy_version="isc-dns-v1",
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "isc.csv"
            path.write_text("dns.example,1998\n", encoding="utf-8")
            reservoir = Reservoir(
                reservoir_id="reservoir:isc",
                domain_id="domain:isc",
                adapter_id=bind_contract_to_adapter_id(
                    "structured:isc",
                    contract,
                ),
                root_locator=str(path),
                enumeration_kind="structured_records",
                capacity_lower=1,
                evidence_mode="discovery_only",
                state=ReservoirState.READY,
            )
            adapter = ProductionAdapterFactory.open(reservoir)
            records, _result = adapter.execute(_lease(reservoir))
            observation = next(iter(adapter.extract_hosts(next(records))))
            adapter.close()

        self.assertEqual(observation.direct_year_mask, 0)
        self.assertEqual(observation.year_hint_mask, 1 << (1998 - 1996))
        plan = EvidencePlanner().plan(
            observation,
            official_mask=0,
            local_mask=0,
            provider="wayback",
            policy_version="runtime-policy",
            allow_direct=True,
        )
        self.assertEqual(plan.direct_capsules, ())
        self.assertNotEqual(plan.external_keys, ())

    def test_missing_or_malformed_contract_year_never_becomes_direct(self) -> None:
        contract = _direct_contract(
            "jsonl",
            hostname_field="url",
            timestamp_field="year",
            contract_id="strict-year-jsonl-v1",
        )
        fixtures = (
            '{"url":"https://missing.example/a"}\n',
            '{"url":"https://bad.example/a","year":"not-a-year"}\n',
            '{"url":"https://outside.example/a","year":2005}\n',
        )
        with tempfile.TemporaryDirectory() as tmp:
            for index, payload in enumerate(fixtures):
                path = Path(tmp) / f"records-{index}.jsonl"
                path.write_text(payload, encoding="utf-8")
                reservoir = Reservoir(
                    reservoir_id=f"reservoir:strict-{index}",
                    domain_id=f"domain:strict-{index}",
                    adapter_id=bind_contract_to_adapter_id(
                        f"structured:strict-{index}",
                        contract,
                    ),
                    root_locator=str(path),
                    enumeration_kind="structured_records",
                    capacity_lower=1,
                    evidence_mode="direct_year",
                    state=ReservoirState.READY,
                )
                adapter = ProductionAdapterFactory.open(
                    reservoir,
                    temporal_scope=(1996, 2001),
                )
                records, _result = adapter.execute(_lease(reservoir))
                observation = next(iter(adapter.extract_hosts(next(records))))
                adapter.close()
                self.assertEqual(observation.direct_year_mask, 0)

    def test_contract_binding_is_stable_across_adapter_reopen_and_leases(self) -> None:
        contract = _direct_contract(
            "jsonl",
            hostname_field="url",
            timestamp_field="year",
            contract_id="stable-lease-jsonl-v1",
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "stable.jsonl"
            path.write_text(
                '{"url":"https://first.example/","year":1998}\n'
                '{"url":"https://second.example/","year":1999}\n',
                encoding="utf-8",
            )
            adapter_id = bind_contract_to_adapter_id(
                "structured:stable",
                contract,
            )
            reservoir = Reservoir(
                reservoir_id="reservoir:stable",
                domain_id="domain:stable",
                adapter_id=adapter_id,
                root_locator=str(path),
                enumeration_kind="structured_records",
                capacity_lower=2,
                evidence_mode="direct_year",
                state=ReservoirState.READY,
            )

            first_adapter = ProductionAdapterFactory.open(reservoir)
            records, first_result = first_adapter.execute(_lease(reservoir))
            first = next(iter(first_adapter.extract_hosts(next(records))))
            first_adapter.close()

            second_adapter = ProductionAdapterFactory.open(reservoir)
            records, _second_result = second_adapter.execute(
                _lease(reservoir, cursor=first_result.next_cursor)
            )
            second = next(iter(second_adapter.extract_hosts(next(records))))
            second_adapter.close()

        self.assertEqual(contract_from_adapter_id(adapter_id), contract)
        self.assertEqual(first.evidence_contract_id, contract.contract_id)
        self.assertEqual(second.evidence_contract_id, contract.contract_id)
        self.assertEqual(first.direct_year_mask, 1 << (1998 - 1996))
        self.assertEqual(second.direct_year_mask, 1 << (1999 - 1996))


if __name__ == "__main__":
    unittest.main()
