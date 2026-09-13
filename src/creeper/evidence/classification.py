"""Stable evidence acquisition classification and formal semantic validation."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from urllib.parse import urlsplit

from creeper.authority.normalizer import normalize_official
from creeper.evidence.contracts import CDX_DIRECT_CONTRACT, CDXJ_DIRECT_CONTRACT
from creeper.evidence.policies import EvidenceCapsule


class AcquisitionLane(StrEnum):
    DIRECT_ANNUAL = "DIRECT_ANNUAL"
    VERIFIED_CANDIDATE = "VERIFIED_CANDIDATE"
    REGISTRATION_YEAR = "REGISTRATION_YEAR"
    DNS_REFERENCE = "DNS_REFERENCE"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class EvidenceSemanticValidation:
    lane: AcquisitionLane
    accepted_for_annual: bool
    errors: tuple[str, ...]


_DIRECT_CONTRACTS = {
    (contract.contract_id, contract.policy_version): contract
    for contract in (CDX_DIRECT_CONTRACT, CDXJ_DIRECT_CONTRACT)
}
_CONTRACT_RE = re.compile(r"(?:^|;)contract=([^@;]+)@([^;]+)")
_CAPTURE_TYPES = frozenset({"exact_host_cdx_capture", "archive_capture"})
_CAPTURE_SEMANTICS = frozenset(
    {"capture_timestamp_year", "archive_capture_timestamp"}
)


def _bound_direct_contract(capsule: EvidenceCapsule):
    match = _CONTRACT_RE.search(capsule.extraction_method or "")
    if match is None:
        return None
    contract = _DIRECT_CONTRACTS.get((match.group(1), match.group(2)))
    if contract is None:
        return None
    if capsule.evidence_type != contract.evidence_type:
        return None
    if capsule.temporal_semantics != contract.temporal_semantics:
        return None
    return contract


def classify_acquisition_lane(capsule: EvidenceCapsule) -> AcquisitionLane:
    """Classify how proof was acquired without deciding whether it is valid."""

    if capsule.provider.startswith("direct:"):
        return (
            AcquisitionLane.DIRECT_ANNUAL
            if _bound_direct_contract(capsule) is not None
            else AcquisitionLane.UNKNOWN
        )
    if (
        capsule.provider == "rdap"
        or capsule.evidence_type == "rdap_registration_event"
        or capsule.temporal_semantics == "registration_event_year"
    ):
        return AcquisitionLane.REGISTRATION_YEAR
    dns_text = " ".join(
        (
            capsule.provider,
            capsule.evidence_type,
            capsule.temporal_semantics,
            capsule.extraction_method,
        )
    ).lower()
    if "dns" in dns_text:
        return AcquisitionLane.DNS_REFERENCE
    if (
        capsule.evidence_type in _CAPTURE_TYPES
        and capsule.temporal_semantics in _CAPTURE_SEMANTICS
    ):
        return AcquisitionLane.VERIFIED_CANDIDATE
    return AcquisitionLane.UNKNOWN


def _timestamp_year(timestamp: str) -> int | None:
    text = str(timestamp).strip()
    if len(text) < 4 or not text[:4].isdigit():
        return None
    return int(text[:4])


def _exact_original_hostname(capsule: EvidenceCapsule) -> bool:
    try:
        parsed = urlsplit(capsule.original_url)
    except ValueError:
        return False
    return normalize_official(parsed.hostname or "") == capsule.hostname


def validate_evidence_semantics(
    capsule: EvidenceCapsule,
) -> EvidenceSemanticValidation:
    """Fail-closed formal semantic validation for one annual proof capsule."""

    errors: list[str] = []
    normalized = normalize_official(capsule.hostname)
    if normalized is None or normalized != capsule.hostname:
        errors.append("hostname is not canonical official normalization")
    if capsule.year not in range(1996, 2002):
        errors.append("year is outside 1996..2001")

    for name in (
        "provider",
        "temporal_semantics",
        "evidence_timestamp",
        "source_locator",
        "payload_hash",
        "policy_version",
        "evidence_type",
        "source_id",
        "original_url",
        "record_locator",
        "extraction_method",
    ):
        if not str(getattr(capsule, name, "")).strip():
            errors.append(f"missing evidence provenance: {name}")

    lane = classify_acquisition_lane(capsule)
    timestamp_year = _timestamp_year(capsule.evidence_timestamp)

    if lane in {
        AcquisitionLane.DIRECT_ANNUAL,
        AcquisitionLane.VERIFIED_CANDIDATE,
        AcquisitionLane.REGISTRATION_YEAR,
    }:
        if timestamp_year is None:
            errors.append("evidence timestamp has no valid four-digit year")
        elif timestamp_year != capsule.year:
            errors.append(
                "evidence timestamp year does not match capsule target year"
            )

    if lane in {
        AcquisitionLane.DIRECT_ANNUAL,
        AcquisitionLane.VERIFIED_CANDIDATE,
    }:
        if not _exact_original_hostname(capsule):
            errors.append(
                "exact capture original URL hostname does not match capsule hostname"
            )

    if lane is AcquisitionLane.DIRECT_ANNUAL:
        expected_source_id = capsule.provider.removeprefix("direct:")
        if not expected_source_id or capsule.source_id != expected_source_id:
            errors.append("direct provider/source_id provenance mismatch")
        if _bound_direct_contract(capsule) is None:
            errors.append("direct evidence is not bound to a supported reviewed contract")
    elif lane is AcquisitionLane.VERIFIED_CANDIDATE:
        if capsule.provider.startswith("direct:"):
            errors.append("externally verified capture cannot use direct provider provenance")
    elif lane is AcquisitionLane.REGISTRATION_YEAR:
        if capsule.provider != "rdap":
            errors.append("registration-year evidence must use the RDAP provider")
        if capsule.evidence_type != "rdap_registration_event":
            errors.append("registration-year evidence must be a registration event")
        if capsule.temporal_semantics != "registration_event_year":
            errors.append("registration-year evidence has unsupported temporal semantics")
    elif lane is AcquisitionLane.DNS_REFERENCE:
        errors.append("DNS reference cannot prove annual web presence")
    else:
        errors.append("unsupported evidence type/semantic combination")

    accepted = (
        not errors
        and lane
        in {
            AcquisitionLane.DIRECT_ANNUAL,
            AcquisitionLane.VERIFIED_CANDIDATE,
            AcquisitionLane.REGISTRATION_YEAR,
        }
    )
    return EvidenceSemanticValidation(lane, accepted, tuple(errors))


def contribution_bucket(lane: AcquisitionLane) -> str:
    if lane is AcquisitionLane.DIRECT_ANNUAL:
        return "direct_annual"
    if lane is AcquisitionLane.VERIFIED_CANDIDATE:
        return "verified_candidate"
    return "other_restricted"
