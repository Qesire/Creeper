"""Deterministic, progressive intake facts for artifact candidates.

This module joins completed HTTP triage and object-identity checks into a
fail-closed routing decision. Sample statistics remain scheduling signals; they
never become formal evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from urllib.parse import urlsplit

from creeper.source_discovery.coordinator import TriageDisposition, TriageResult
from creeper.source_discovery.index_identity import HistoricalIndexObjectIdentity


class ArtifactAdmission(StrEnum):
    WARM = "WARM"
    HOLD = "HOLD"
    REJECT = "REJECT"


@dataclass(frozen=True)
class ArtifactIntakePolicy:
    reject_html: bool = True
    require_identity_for_warm: bool = True
    allow_formats: frozenset[str] = frozenset(
        {"CDX", "CDXJ", "WARC", "ARC", "HOST_LIST", "JSONL", "CSV"}
    )

    def __post_init__(self) -> None:
        if not isinstance(self.reject_html, bool):
            raise ValueError("reject_html must be boolean")
        if not isinstance(self.require_identity_for_warm, bool):
            raise ValueError("require_identity_for_warm must be boolean")
        if not self.allow_formats:
            raise ValueError("allow_formats must not be empty")


@dataclass(frozen=True)
class ArtifactIntakeResult:
    access_ok: bool
    identity_strength: str
    content_length: int | None
    content_type: str | None
    compression: str | None
    transport_range_supported: bool
    random_access_supported: bool
    format_kind: str
    contract_match: str | None
    sample_stats: object | None
    failure_reason: str | None
    admission: ArtifactAdmission
    sample_is_formal_evidence: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "admission", ArtifactAdmission(self.admission))
        if self.content_length is not None and self.content_length < 0:
            raise ValueError("content_length must be non-negative")


def _path_kind(url: str) -> tuple[str, str | None]:
    path = urlsplit(url).path.lower().rstrip("/")
    compression = None
    for suffix, codec in ((".gz", "gzip"), (".bz2", "bzip2"), (".zst", "zstd")):
        if path.endswith(suffix):
            compression = codec
            path = path[: -len(suffix)]
            break

    suffix_map = (
        (".cdxj", "CDXJ"),
        (".cdx", "CDX"),
        (".warc", "WARC"),
        (".arc", "ARC"),
        (".jsonl", "JSONL"),
        (".ndjson", "JSONL"),
        (".csv", "CSV"),
        (".txt", "HOST_LIST"),
        (".list", "HOST_LIST"),
        (".hosts", "HOST_LIST"),
        (".urls", "HOST_LIST"),
    )
    for suffix, kind in suffix_map:
        if path.endswith(suffix):
            return kind, compression
    return "UNKNOWN", compression


def classify_artifact(url: str, content_type: str | None = None) -> tuple[str, str | None]:
    """Classify conservatively from URL and media type."""
    kind, compression = _path_kind(url)
    media_type = (content_type or "").split(";", 1)[0].strip().lower()
    if compression is None and media_type in {
        "application/gzip", "application/x-gzip", "application/z-gzip"
    }:
        compression = "gzip"
    if kind != "UNKNOWN":
        return kind, compression
    media_map = {
        "application/warc": "WARC",
        "application/x-warc": "WARC",
        "application/arc": "ARC",
        "application/x-arc": "ARC",
        "application/json": "JSONL",
        "application/x-ndjson": "JSONL",
        "text/csv": "CSV",
        "text/plain": "HOST_LIST",
    }
    return media_map.get(media_type, "UNKNOWN"), compression


def _looks_like_html_login(sample_preview: bytes) -> bool:
    text = sample_preview[:65536].decode("utf-8", errors="ignore").lower()
    if "<html" not in text and "<!doctype html" not in text:
        return False
    return any(marker in text for marker in (
        "login", "sign in", "signin", "password", "unauthorized"
    ))


def _identity_strength(identity: HistoricalIndexObjectIdentity | None) -> str:
    if identity is None:
        return "NONE"
    return "STRONG" if identity.is_verifiable else "WEAK"


def _is_compressed_random_access_safe(
    *, format_kind: str, compression: str | None, transport_range_supported: bool
) -> bool:
    if not transport_range_supported or compression is not None:
        return False
    return format_kind in {"CDX", "CDXJ", "HOST_LIST", "JSONL", "CSV"}


def assess_artifact_intake(
    triage: TriageResult,
    *,
    url: str,
    identity: HistoricalIndexObjectIdentity | None,
    contract_match: str | None,
    sample_stats: object | None = None,
    sample_preview: bytes = b"",
    policy: ArtifactIntakePolicy | None = None,
) -> ArtifactIntakeResult:
    """Convert bounded triage and identity facts into a disposition."""
    policy = policy or ArtifactIntakePolicy()
    format_kind, compression = classify_artifact(url, triage.content_type)
    content_type = triage.content_type
    access_ok = (
        triage.disposition is TriageDisposition.SCOUT
        and triage.status_code is not None
        and 200 <= triage.status_code < 400
    )
    reason: str | None = None

    if not access_ok:
        reason = "artifact is not accessible for bounded scout"
    elif (
        policy.reject_html
        and content_type
        and content_type.split(";", 1)[0].strip().lower()
        in {"text/html", "application/xhtml+xml"}
    ):
        access_ok = False
        reason = "HTML/login or error object is not an artifact"
    elif policy.reject_html and _looks_like_html_login(sample_preview):
        access_ok = False
        reason = "HTML/login or error object is not an artifact"
    elif format_kind not in policy.allow_formats:
        reason = "UNSUPPORTED_FORMAT"
    elif contract_match is None:
        reason = "UNKNOWN_CONTRACT_FAMILY"

    strength = _identity_strength(identity)
    random_access = _is_compressed_random_access_safe(
        format_kind=format_kind,
        compression=compression,
        transport_range_supported=bool(triage.range_supported),
    )
    if access_ok and reason is None:
        if policy.require_identity_for_warm and strength != "STRONG":
            reason = "IDENTITY_UNVERIFIABLE"
        else:
            return ArtifactIntakeResult(
                access_ok=True,
                identity_strength=strength,
                content_length=triage.content_length,
                content_type=content_type,
                compression=compression,
                transport_range_supported=bool(triage.range_supported),
                random_access_supported=random_access,
                format_kind=format_kind,
                contract_match=contract_match,
                sample_stats=sample_stats,
                failure_reason=None,
                admission=ArtifactAdmission.WARM,
            )

    admission = (
        ArtifactAdmission.REJECT
        if not access_ok or reason == "UNSUPPORTED_FORMAT"
        else ArtifactAdmission.HOLD
    )
    return ArtifactIntakeResult(
        access_ok=access_ok,
        identity_strength=strength,
        content_length=triage.content_length,
        content_type=content_type,
        compression=compression,
        transport_range_supported=bool(triage.range_supported),
        random_access_supported=random_access,
        format_kind=format_kind,
        contract_match=contract_match,
        sample_stats=sample_stats,
        failure_reason=reason,
        admission=admission,
    )
