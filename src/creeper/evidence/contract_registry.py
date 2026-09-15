"""Versioned reviewed direct-evidence contract registry.

Generic structured sources never gain annual evidence authority from filename
suffixes or agent priors alone. Self-describing record formats such as CDX/CDXJ
derive authority from validated record semantics. A reviewed registry is an
optional stronger binding for other schemas: it binds one exact canonical
locator to both a SourceEvidenceContract and an auditable artifact identity. The
artifact identity is frozen into the durable adapter id at activation so
authority survives restart without mutable configuration reads in the record
hot path.
"""

from __future__ import annotations

import base64
from collections.abc import Callable, Iterator, Mapping
from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
import re
from types import MappingProxyType
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from creeper.evidence.contracts import (
    EvidenceAuthority,
    SourceEvidenceContract,
)
from creeper.source_discovery.index_identity import normalize_strong_etag
from creeper.source_discovery.models import canonicalize_source_entrypoint


REGISTRY_VERSION = "reviewed-source-contract-registry-v1"
_REVIEWED_IDENTITY_MARKER = ":rsi1:"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class ReviewedContractRegistryError(ValueError):
    """Invalid reviewed-contract registry configuration."""


class ReviewedArtifactIdentityError(ValueError):
    """Observed artifact identity conflicts with reviewed authority."""


class ReviewedArtifactIdentityUnverifiable(ReviewedArtifactIdentityError):
    """Artifact identity cannot currently be established."""


@dataclass(frozen=True)
class ReviewedArtifactIdentity:
    kind: str
    value: str
    content_length: int | None = None

    def __post_init__(self) -> None:
        kind = str(self.kind).strip().lower()
        if kind not in {"sha256", "etag+length", "immutable_locator"}:
            raise ReviewedContractRegistryError(
                f"unsupported reviewed source identity kind: {kind}"
            )
        object.__setattr__(self, "kind", kind)

        raw_value = str(self.value).strip()
        if not raw_value:
            raise ReviewedContractRegistryError(
                "reviewed source identity value is required"
            )

        if kind == "sha256":
            normalized = raw_value.lower().removeprefix("sha256:")
            if not _SHA256_RE.fullmatch(normalized):
                raise ReviewedContractRegistryError(
                    "sha256 reviewed identity must be a 64-character hex digest"
                )
            object.__setattr__(self, "value", normalized)
        elif kind == "etag+length":
            etag = normalize_strong_etag(raw_value)
            if etag is None:
                raise ReviewedContractRegistryError(
                    "etag+length reviewed identity requires a strong ETag"
                )
            if (
                isinstance(self.content_length, bool)
                or not isinstance(self.content_length, int)
                or self.content_length < 0
            ):
                raise ReviewedContractRegistryError(
                    "etag+length reviewed identity requires content_length"
                )
            object.__setattr__(self, "value", etag)
        else:
            try:
                canonical = canonicalize_source_entrypoint(raw_value)
            except ValueError as exc:
                raise ReviewedContractRegistryError(
                    "immutable_locator reviewed identity must be an HTTP(S) locator"
                ) from exc
            object.__setattr__(self, "value", canonical)

        if self.content_length is not None and (
            isinstance(self.content_length, bool)
            or not isinstance(self.content_length, int)
            or self.content_length < 0
        ):
            raise ReviewedContractRegistryError(
                "reviewed identity content_length must be non-negative"
            )


@dataclass(frozen=True)
class ReviewedArtifactBinding:
    locator: str
    source_identity: ReviewedArtifactIdentity
    custodian: str
    edition: str

    def __post_init__(self) -> None:
        try:
            locator = canonicalize_source_entrypoint(self.locator)
        except ValueError as exc:
            raise ReviewedContractRegistryError(
                "reviewed contract locator must be canonicalizable HTTP(S)"
            ) from exc
        object.__setattr__(self, "locator", locator)
        for name in ("custodian", "edition"):
            value = str(getattr(self, name)).strip()
            if not value:
                raise ReviewedContractRegistryError(f"{name} is required")
            object.__setattr__(self, name, value)
        if (
            self.source_identity.kind == "immutable_locator"
            and self.source_identity.value != self.locator
        ):
            raise ReviewedContractRegistryError(
                "immutable_locator identity does not match reviewed locator"
            )

    @property
    def binding_token(self) -> str:
        payload = {
            "locator": self.locator,
            "source_identity": asdict(self.source_identity),
            "custodian": self.custodian,
            "edition": self.edition,
        }
        raw = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    @property
    def binding_digest(self) -> str:
        return hashlib.sha256(self.binding_token.encode("ascii")).hexdigest()

    @classmethod
    def from_binding_token(cls, token: str) -> "ReviewedArtifactBinding":
        if not token or not re.fullmatch(r"[A-Za-z0-9_-]+", token):
            raise ValueError("invalid reviewed artifact binding token")
        padded = token + "=" * (-len(token) % 4)
        try:
            payload = json.loads(
                base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
            )
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid reviewed artifact binding token") from exc
        if not isinstance(payload, dict) or set(payload) != {
            "locator",
            "source_identity",
            "custodian",
            "edition",
        }:
            raise ValueError("invalid reviewed artifact binding payload")
        identity = payload["source_identity"]
        if not isinstance(identity, dict):
            raise ValueError("invalid reviewed source identity payload")
        return cls(
            locator=payload["locator"],
            source_identity=ReviewedArtifactIdentity(**identity),
            custodian=payload["custodian"],
            edition=payload["edition"],
        )


@dataclass(frozen=True)
class ReviewedSourceContractBinding:
    artifact: ReviewedArtifactBinding
    contract: SourceEvidenceContract
    review_note: str

    def __post_init__(self) -> None:
        if self.contract.authority is not EvidenceAuthority.DIRECT_WEB_YEAR:
            raise ReviewedContractRegistryError(
                "external reviewed registry entries must grant DIRECT_WEB_YEAR"
            )
        # Parser semantics are reviewed explicitly. Transport locators may be
        # opaque repository/API endpoints with no meaningful filename suffix;
        # activation validates the reviewed parser against the scout-frozen
        # format observation before production.
        if self.contract.hostname_field is None:
            raise ReviewedContractRegistryError(
                "reviewed direct structured contract requires hostname_field"
            )
        if self.contract.timestamp_field is None:
            raise ReviewedContractRegistryError(
                "reviewed direct structured contract requires timestamp_field"
            )
        note = str(self.review_note).strip()
        if not note:
            raise ReviewedContractRegistryError("review_note is required")
        object.__setattr__(self, "review_note", note)

    @property
    def locator(self) -> str:
        return self.artifact.locator

    @property
    def binding_digest(self) -> str:
        payload = (
            self.artifact.binding_digest
            + "\x00"
            + self.contract.binding_digest
            + "\x00"
            + self.review_note
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class ReviewedContractRegistry(Mapping[str, ReviewedSourceContractBinding]):
    """Immutable exact-locator mapping loaded from one reviewed registry file."""

    def __init__(
        self,
        bindings: Mapping[str, ReviewedSourceContractBinding] | None = None,
    ) -> None:
        copied = dict(bindings or {})
        for locator, binding in copied.items():
            if locator != binding.locator:
                raise ReviewedContractRegistryError(
                    "registry key must equal the binding locator"
                )
        self._bindings = MappingProxyType(copied)

    def __getitem__(self, key: str) -> ReviewedSourceContractBinding:
        return self._bindings[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._bindings)

    def __len__(self) -> int:
        return len(self._bindings)

    def get_exact(self, locator: str) -> ReviewedSourceContractBinding | None:
        try:
            canonical = canonicalize_source_entrypoint(locator)
        except ValueError:
            return None
        return self._bindings.get(canonical)

    def to_manifest_payload(self) -> dict[str, object]:
        """Return the deterministic, self-authenticating archive representation."""

        entries = [
            _manifest_entry(self._bindings[locator])
            for locator in sorted(self._bindings)
        ]
        unsigned = {
            "registry_version": REGISTRY_VERSION,
            "entries": entries,
        }
        return {
            **unsigned,
            "registry_digest": _manifest_digest(unsigned),
        }

    @classmethod
    def from_manifest_payload(cls, payload: object) -> "ReviewedContractRegistry":
        """Rebuild a registry from a strict packaged authority declaration."""

        if not isinstance(payload, dict):
            raise ReviewedContractRegistryError(
                "reviewed contract manifest root must be an object"
            )
        if set(payload) != {"registry_version", "entries", "registry_digest"}:
            raise ReviewedContractRegistryError(
                "reviewed contract manifest has unsupported fields"
            )
        if payload.get("registry_version") != REGISTRY_VERSION:
            raise ReviewedContractRegistryError(
                f"registry_version must be {REGISTRY_VERSION!r}"
            )
        entries = payload.get("entries")
        if not isinstance(entries, list):
            raise ReviewedContractRegistryError("registry entries must be a list")
        digest = payload.get("registry_digest")
        if not isinstance(digest, str) or not _SHA256_RE.fullmatch(digest):
            raise ReviewedContractRegistryError(
                "registry_digest must be a SHA-256 value"
            )
        unsigned = {
            "registry_version": payload["registry_version"],
            "entries": entries,
        }
        if digest != _manifest_digest(unsigned):
            raise ReviewedContractRegistryError("reviewed contract registry digest mismatch")

        bindings: dict[str, ReviewedSourceContractBinding] = {}
        for raw in entries:
            binding = _binding_from_manifest_entry(raw)
            if binding.locator in bindings:
                raise ReviewedContractRegistryError(
                    f"duplicate reviewed contract locator: {binding.locator}"
                )
            bindings[binding.locator] = binding
        if entries != [
            _manifest_entry(bindings[locator]) for locator in sorted(bindings)
        ]:
            raise ReviewedContractRegistryError(
                "reviewed contract entries must be canonical and locator-sorted"
            )
        return cls(bindings)


def _required_text(entry: Mapping[str, object], name: str) -> str:
    value = entry.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ReviewedContractRegistryError(f"{name} is required")
    return value.strip()


def _contract_payload(contract: SourceEvidenceContract) -> dict[str, object]:
    return {
        "contract_id": contract.contract_id,
        "authority": contract.authority.value,
        "parser_kind": contract.parser_kind,
        "temporal_semantics": contract.temporal_semantics,
        "evidence_type": contract.evidence_type,
        "hostname_field": contract.hostname_field,
        "timestamp_field": contract.timestamp_field,
        "policy_version": contract.policy_version,
    }


def _manifest_entry(binding: ReviewedSourceContractBinding) -> dict[str, object]:
    entry = {
        "locator": binding.artifact.locator,
        "source_identity": {
            "kind": binding.artifact.source_identity.kind,
            "value": binding.artifact.source_identity.value,
            "content_length": binding.artifact.source_identity.content_length,
        },
        "custodian": binding.artifact.custodian,
        "edition": binding.artifact.edition,
        "contract": _contract_payload(binding.contract),
        "review_note": binding.review_note,
    }
    entry["binding_digest"] = binding.binding_digest
    return entry


def _manifest_digest(payload: Mapping[str, object]) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _binding_from_manifest_entry(raw: object) -> ReviewedSourceContractBinding:
    if not isinstance(raw, dict):
        raise ReviewedContractRegistryError("registry entry must be an object")
    required = {
        "locator",
        "source_identity",
        "custodian",
        "edition",
        "contract",
        "review_note",
        "binding_digest",
    }
    if set(raw) != required:
        raise ReviewedContractRegistryError("reviewed contract entry has unsupported fields")
    identity_raw = raw["source_identity"]
    if not isinstance(identity_raw, dict) or set(identity_raw) != {
        "kind",
        "value",
        "content_length",
    }:
        raise ReviewedContractRegistryError("reviewed source identity fields are invalid")
    contract_raw = raw["contract"]
    contract_fields = {
        "contract_id",
        "authority",
        "parser_kind",
        "temporal_semantics",
        "evidence_type",
        "hostname_field",
        "timestamp_field",
        "policy_version",
    }
    if not isinstance(contract_raw, dict) or set(contract_raw) != contract_fields:
        raise ReviewedContractRegistryError("reviewed contract fields are invalid")
    binding = ReviewedSourceContractBinding(
        artifact=ReviewedArtifactBinding(
            locator=_required_text(raw, "locator"),
            source_identity=_parse_identity(identity_raw),
            custodian=_required_text(raw, "custodian"),
            edition=_required_text(raw, "edition"),
        ),
        contract=SourceEvidenceContract(**contract_raw),
        review_note=_required_text(raw, "review_note"),
    )
    if raw["binding_digest"] != binding.binding_digest:
        raise ReviewedContractRegistryError(
            f"reviewed binding digest mismatch for {binding.locator}"
        )
    return binding


def _parse_identity(raw: object) -> ReviewedArtifactIdentity:
    if not isinstance(raw, dict):
        raise ReviewedContractRegistryError("source_identity must be an object")
    allowed = {"kind", "value", "content_length"}
    extra = set(raw) - allowed
    if extra:
        raise ReviewedContractRegistryError(
            "unsupported source_identity fields: " + ", ".join(sorted(extra))
        )
    return ReviewedArtifactIdentity(
        kind=_required_text(raw, "kind"),
        value=_required_text(raw, "value"),
        content_length=raw.get("content_length"),
    )


def load_reviewed_contract_registry(path: Path) -> ReviewedContractRegistry:
    """Load one strict, versioned registry into immutable process memory."""

    path = Path(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReviewedContractRegistryError(
            f"cannot load reviewed contract registry: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise ReviewedContractRegistryError(
            "reviewed contract registry root must be an object"
        )
    if "registry_digest" in payload:
        return ReviewedContractRegistry.from_manifest_payload(payload)
    if payload.get("registry_version") != REGISTRY_VERSION:
        raise ReviewedContractRegistryError(
            f"registry_version must be {REGISTRY_VERSION!r}"
        )
    entries = payload.get("entries")
    if not isinstance(entries, list):
        raise ReviewedContractRegistryError("registry entries must be a list")

    bindings: dict[str, ReviewedSourceContractBinding] = {}
    for raw in entries:
        if not isinstance(raw, dict):
            raise ReviewedContractRegistryError("registry entry must be an object")
        locator = canonicalize_source_entrypoint(_required_text(raw, "locator"))
        contract = SourceEvidenceContract(
            contract_id=_required_text(raw, "contract_id"),
            authority=_required_text(raw, "authority"),
            parser_kind=_required_text(raw, "parser_kind"),
            hostname_field=_required_text(raw, "hostname_field"),
            timestamp_field=_required_text(raw, "timestamp_field"),
            temporal_semantics=_required_text(raw, "temporal_semantics"),
            evidence_type=_required_text(raw, "evidence_type"),
            policy_version=_required_text(raw, "policy_version"),
        )
        artifact = ReviewedArtifactBinding(
            locator=locator,
            source_identity=_parse_identity(raw.get("source_identity")),
            custodian=_required_text(raw, "custodian"),
            edition=_required_text(raw, "edition"),
        )
        binding = ReviewedSourceContractBinding(
            artifact=artifact,
            contract=contract,
            review_note=_required_text(raw, "review_note"),
        )
        if locator in bindings:
            raise ReviewedContractRegistryError(
                f"duplicate reviewed contract locator: {locator}"
            )
        bindings[locator] = binding
    return ReviewedContractRegistry(bindings)


def bind_reviewed_artifact_to_adapter_id(
    adapter_id: str,
    artifact: ReviewedArtifactBinding,
) -> str:
    """Freeze reviewed artifact identity into durable adapter identity."""

    if _REVIEWED_IDENTITY_MARKER in adapter_id:
        existing = reviewed_artifact_from_adapter_id(adapter_id)
        if existing != artifact:
            raise ValueError(
                "adapter_id is already bound to another reviewed artifact"
            )
        return adapter_id

    before_evidence, evidence_sep, evidence_tail = adapter_id.partition(":evc1:")
    before_schema, schema_sep, schema_tail = before_evidence.partition(":sch1:")
    before_format, format_sep, format_tail = before_schema.partition(":fmt1:")
    bound = (
        f"{before_format}{_REVIEWED_IDENTITY_MARKER}"
        f"{artifact.binding_token}"
    )
    if format_sep:
        bound = f"{bound}:fmt1:{format_tail}"
    if schema_sep:
        bound = f"{bound}:sch1:{schema_tail}"
    if evidence_sep:
        bound = f"{bound}:evc1:{evidence_tail}"
    return bound


def reviewed_artifact_from_adapter_id(
    adapter_id: str,
) -> ReviewedArtifactBinding | None:
    head = (
        adapter_id.split(":evc1:", 1)[0]
        .split(":sch1:", 1)[0]
        .split(":fmt1:", 1)[0]
    )
    if _REVIEWED_IDENTITY_MARKER not in head:
        return None
    _prefix, token = head.rsplit(_REVIEWED_IDENTITY_MARKER, 1)
    return ReviewedArtifactBinding.from_binding_token(token)


IdentityObserver = Callable[
    [ReviewedArtifactBinding],
    ReviewedArtifactIdentity | None,
]


def observe_remote_reviewed_artifact(
    artifact: ReviewedArtifactBinding,
    *,
    timeout: float = 20.0,
) -> ReviewedArtifactIdentity | None:
    """Observe remote ETag+length once at an authority boundary.

    Full remote SHA-256 is intentionally not attempted here. Large remote
    artifacts should use strong ETag+length or an audited immutable locator.
    """

    identity = artifact.source_identity
    if identity.kind == "immutable_locator":
        return ReviewedArtifactIdentity(
            kind="immutable_locator",
            value=artifact.locator,
        )
    if identity.kind == "sha256":
        return None

    request = Request(
        artifact.locator,
        method="HEAD",
        headers={
            "Accept-Encoding": "identity",
            "User-Agent": "creeper-reviewed-contract/1",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            etag = normalize_strong_etag(response.headers.get("ETag"))
            length_raw = response.headers.get("Content-Length")
    except (OSError, HTTPError, URLError):
        return None
    if etag is None or length_raw is None:
        return None
    try:
        length = int(length_raw)
    except ValueError:
        return None
    if length < 0:
        return None
    return ReviewedArtifactIdentity(
        kind="etag+length",
        value=etag,
        content_length=length,
    )


def verify_reviewed_artifact_identity(
    artifact: ReviewedArtifactBinding,
    *,
    observer: IdentityObserver | None = None,
) -> ReviewedArtifactIdentity:
    """Verify current object identity against the reviewed frozen identity."""

    expected = artifact.source_identity
    if expected.kind == "immutable_locator":
        observed = ReviewedArtifactIdentity(
            kind="immutable_locator",
            value=artifact.locator,
        )
    else:
        observed = (observer or observe_remote_reviewed_artifact)(artifact)
        if observed is None:
            raise ReviewedArtifactIdentityUnverifiable(
                "reviewed artifact identity cannot be verified"
            )

    if observed.kind != expected.kind:
        raise ReviewedArtifactIdentityError(
            "reviewed artifact identity kind changed"
        )
    if observed.value != expected.value:
        raise ReviewedArtifactIdentityError(
            "reviewed artifact identity value changed"
        )
    if (
        expected.content_length is not None
        and observed.content_length != expected.content_length
    ):
        raise ReviewedArtifactIdentityError(
            "reviewed artifact content length changed"
        )
    return observed
