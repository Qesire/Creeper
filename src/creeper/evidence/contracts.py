"""Deterministic evidence-authority contracts for activated sources.

Filename suffixes and discovery-agent priors may identify parser capabilities,
but they are not annual evidence authority.  A source becomes direct annual web
evidence only through one immutable SourceEvidenceContract.

Contracts are frozen into durable adapter IDs at activation time.  That makes
authority stable across leases and process restarts without a per-record schema
lookup or mutable hot-path configuration read.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Mapping
from urllib.parse import urlsplit


_CONTRACT_MARKER = ":evc1:"
_CONTRACT_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
_PARSER_KINDS = frozenset(
    {
        "cdx",
        "cdxj",
        "jsonl",
        "delimited",
        "lines",
        "warc_arc",
    }
)


class EvidenceAuthority(StrEnum):
    """Semantic authority granted by one reviewed source contract."""

    DIRECT_WEB_YEAR = "DIRECT_WEB_YEAR"
    REGISTRATION_YEAR = "REGISTRATION_YEAR"
    DNS_OBSERVATION = "DNS_OBSERVATION"
    DISCOVERY_ONLY = "DISCOVERY_ONLY"


@dataclass(frozen=True)
class SourceEvidenceContract:
    """Immutable, auditable contract for one source parser/semantic family."""

    contract_id: str
    authority: EvidenceAuthority
    parser_kind: str
    temporal_semantics: str
    evidence_type: str
    hostname_field: str | None = None
    timestamp_field: str | None = None
    policy_version: str = "source-evidence-v1"

    def __post_init__(self) -> None:
        object.__setattr__(self, "authority", EvidenceAuthority(self.authority))
        contract_id = self.contract_id.strip().lower()
        if not _CONTRACT_ID_RE.fullmatch(contract_id):
            raise ValueError("invalid evidence contract_id")
        object.__setattr__(self, "contract_id", contract_id)

        parser_kind = self.parser_kind.strip().lower()
        if parser_kind not in _PARSER_KINDS:
            raise ValueError(f"unsupported evidence contract parser_kind: {parser_kind}")
        object.__setattr__(self, "parser_kind", parser_kind)

        for name in ("temporal_semantics", "evidence_type", "policy_version"):
            value = str(getattr(self, name)).strip()
            if not value:
                raise ValueError(f"{name} is required")
            object.__setattr__(self, name, value)

        for name in ("hostname_field", "timestamp_field"):
            value = getattr(self, name)
            if value is not None:
                normalized = str(value).strip()
                if not normalized:
                    raise ValueError(f"{name} must be non-empty when supplied")
                object.__setattr__(self, name, normalized)

        if (
            self.authority is EvidenceAuthority.DIRECT_WEB_YEAR
            and self.parser_kind == "warc_arc"
        ):
            raise ValueError(
                "warc_arc DIRECT_WEB_YEAR contracts are not supported by the "
                "current production adapter"
            )

        if (
            self.authority is EvidenceAuthority.DIRECT_WEB_YEAR
            and self.parser_kind not in {"cdx", "cdxj"}
            and self.hostname_field is None
        ):
            raise ValueError(
                "structured DIRECT_WEB_YEAR contracts require hostname_field"
            )

    @property
    def grants_direct_web_year(self) -> bool:
        return self.authority is EvidenceAuthority.DIRECT_WEB_YEAR

    @property
    def evidence_mode(self) -> str:
        return "direct_year" if self.grants_direct_web_year else "discovery_only"

    @property
    def binding_token(self) -> str:
        payload = json.dumps(
            {
                **asdict(self),
                "authority": self.authority.value,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
        return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")

    @property
    def binding_digest(self) -> str:
        return hashlib.sha256(self.binding_token.encode("ascii")).hexdigest()

    @classmethod
    def from_binding_token(cls, token: str) -> "SourceEvidenceContract":
        if not token or not re.fullmatch(r"[A-Za-z0-9_-]+", token):
            raise ValueError("invalid evidence contract binding token")
        padded = token + "=" * (-len(token) % 4)
        try:
            decoded = base64.urlsafe_b64decode(padded.encode("ascii"))
            payload = json.loads(decoded.decode("utf-8"))
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid evidence contract binding token") from exc
        if not isinstance(payload, dict):
            raise ValueError("invalid evidence contract binding payload")
        allowed = {
            "contract_id",
            "authority",
            "parser_kind",
            "temporal_semantics",
            "evidence_type",
            "hostname_field",
            "timestamp_field",
            "policy_version",
        }
        if set(payload) != allowed:
            raise ValueError("invalid evidence contract binding fields")
        return cls(**payload)


CDX_DIRECT_CONTRACT = SourceEvidenceContract(
    contract_id="archive-cdx-capture-v1",
    authority=EvidenceAuthority.DIRECT_WEB_YEAR,
    parser_kind="cdx",
    temporal_semantics="archive_capture_timestamp",
    evidence_type="dated_archive_index",
    policy_version="archive-capture-contract-v1",
)

CDXJ_DIRECT_CONTRACT = SourceEvidenceContract(
    contract_id="archive-cdxj-capture-v1",
    authority=EvidenceAuthority.DIRECT_WEB_YEAR,
    parser_kind="cdxj",
    temporal_semantics="archive_capture_timestamp",
    evidence_type="dated_archive_index",
    policy_version="archive-capture-contract-v1",
)


def parser_kind_from_locator(locator: str) -> str:
    path = urlsplit(locator).path.lower()
    if path.endswith((".warc.gz", ".arc.gz", ".warc", ".arc")):
        return "warc_arc"
    if path.endswith((".cdxj", ".cdxj.gz")):
        return "cdxj"
    if path.endswith((".cdx", ".cdx.gz")):
        return "cdx"
    if path.endswith((".jsonl", ".jsonl.gz")):
        return "jsonl"
    if path.endswith((".csv", ".csv.gz", ".tsv", ".tsv.gz")):
        return "delimited"
    return "lines"


def discovery_only_contract(parser_kind: str) -> SourceEvidenceContract:
    parser_kind = parser_kind.strip().lower()
    if parser_kind not in _PARSER_KINDS:
        raise ValueError(f"unsupported parser_kind: {parser_kind}")
    return SourceEvidenceContract(
        contract_id=f"discovery-{parser_kind}-v1",
        authority=EvidenceAuthority.DISCOVERY_ONLY,
        parser_kind=parser_kind,
        temporal_semantics="untrusted_or_non_web_temporal_claim",
        evidence_type="discovery_candidate",
        policy_version="source-evidence-v1",
    )


def freeze_contract_bindings(
    bindings: Mapping[str, SourceEvidenceContract] | None,
) -> Mapping[str, SourceEvidenceContract]:
    """Copy an explicit exact-locator allowlist into immutable worker state."""

    if not bindings:
        return MappingProxyType({})
    frozen: dict[str, SourceEvidenceContract] = {}
    for locator, contract in bindings.items():
        key = str(locator).strip()
        if not key:
            raise ValueError("evidence contract binding locator is required")
        if not isinstance(contract, SourceEvidenceContract):
            raise TypeError("evidence contract binding must use SourceEvidenceContract")
        frozen[key] = contract
    return MappingProxyType(frozen)


def resolve_source_evidence_contract(
    locator: str,
    *,
    explicit_contracts: Mapping[str, SourceEvidenceContract] | None = None,
    parser_kind: str | None = None,
) -> SourceEvidenceContract:
    """Resolve authority from immutable code/allowlist, never agent priors.

    CDX/CDXJ are code-allowlisted because their dedicated parsers enforce exact
    capture timestamp + original URL semantics.  Other structured sources are
    discovery-only unless their exact locator is explicitly bound to a reviewed
    contract.
    """

    actual_parser = (
        parser_kind_from_locator(locator)
        if parser_kind is None
        else parser_kind.strip().lower()
    )
    if actual_parser not in _PARSER_KINDS:
        raise ValueError(f"unsupported parser_kind: {actual_parser}")

    if explicit_contracts:
        explicit = explicit_contracts.get(locator)
        if explicit is not None:
            if explicit.parser_kind != actual_parser:
                raise ValueError(
                    "evidence contract parser_kind does not match source parser"
                )
            return explicit

    if actual_parser == "cdx":
        return CDX_DIRECT_CONTRACT
    if actual_parser == "cdxj":
        return CDXJ_DIRECT_CONTRACT
    return discovery_only_contract(actual_parser)


def bind_contract_to_adapter_id(
    adapter_id: str,
    contract: SourceEvidenceContract,
) -> str:
    """Freeze one contract into durable adapter identity."""

    if _CONTRACT_MARKER in adapter_id:
        existing = contract_from_adapter_id(adapter_id)
        if existing != contract:
            raise ValueError("adapter_id is already bound to another evidence contract")
        return adapter_id
    return f"{adapter_id}{_CONTRACT_MARKER}{contract.binding_token}"


def contract_from_adapter_id(adapter_id: str) -> SourceEvidenceContract | None:
    if _CONTRACT_MARKER not in adapter_id:
        return None
    _prefix, token = adapter_id.rsplit(_CONTRACT_MARKER, 1)
    return SourceEvidenceContract.from_binding_token(token)
