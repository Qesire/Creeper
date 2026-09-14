"""Deterministic, progressive intake facts for artifact candidates.

This module joins completed HTTP triage and object-identity checks into a
fail-closed routing decision. Sample statistics remain scheduling signals; they
never become formal evidence.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

from creeper.source_discovery.coordinator import TriageDisposition, TriageResult
from creeper.source_discovery.index_identity import HistoricalIndexObjectIdentity


class ArtifactAdmission(StrEnum):
    WARM = "WARM"
    HOLD = "HOLD"
    REJECT = "REJECT"


class MetadataArtifactAdmission(StrEnum):
    ACCEPT = "ACCEPT"
    HOLD = "HOLD"
    REJECT = "REJECT"


@dataclass(frozen=True)
class ArtifactMetadataAssessment:
    admission: MetadataArtifactAdmission
    format_kind: str
    reason: str
    semantic_hits: tuple[str, ...] = ()


_STRONG_ARTIFACT_FORMATS = frozenset({"CDX", "CDXJ", "WARC", "ARC"})
_SEMANTIC_ARTIFACT_FORMATS = frozenset({"HOST_LIST", "JSONL", "CSV"})
_POSITIVE_METADATA_PATTERNS = (
    ("web_archive", re.compile(r"\bweb\s+archiv(?:e|es|ing|ed)\b", re.I)),
    ("web_crawl", re.compile(r"\bweb\s+crawl(?:s|ed|ing)?\b", re.I)),
    ("historical_web", re.compile(r"\bhistorical\s+web\b", re.I)),
    ("url_list", re.compile(r"\burls?\b|\burl\s+(?:list|dataset|corpus|dump)\b", re.I)),
    ("host_list", re.compile(r"\bhostnames?\b|\bhost\s+list\b", re.I)),
    ("domain_list", re.compile(r"\bdomains?\b|\bdomain\s+(?:list|dataset|corpus|dump)\b", re.I)),
    ("link_graph", re.compile(r"\b(?:web|link)\s*graph\b|\bhyperlink\s+graph\b", re.I)),
    ("capture_index", re.compile(r"\bcapture\s+index\b|\barchive\s+index\b", re.I)),
    ("warc", re.compile(r"\bwarc\b", re.I)),
    ("arc", re.compile(r"\barc\s+(?:file|archive|crawl)\b", re.I)),
    ("cdx", re.compile(r"\bcdxj?\b", re.I)),
    ("webbase", re.compile(r"\bwebbase\b", re.I)),
)
_REJECT_MEDIA_PREFIXES = ("image/", "audio/", "video/", "font/")
_REJECT_MEDIA_TYPES = frozenset({
    "application/pdf",
    "application/msword",
    "application/vnd.ms-powerpoint",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "text/html",
    "application/xhtml+xml",
})
_REJECT_SUFFIXES = (
    ".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx",
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg",
    ".mp3", ".wav", ".flac", ".mp4", ".mov", ".avi", ".mkv",
    ".py", ".pyi", ".js", ".ts", ".java", ".c", ".cc", ".cpp", ".h",
    ".ipynb",
)
_HOLD_CONTAINER_SUFFIXES = (
    ".zip", ".tar", ".tgz", ".tar.gz", ".tar.bz2", ".tar.zst",
    ".7z", ".rar", ".parquet", ".arrow", ".feather",
)


def _metadata_text(
    *,
    filename: str,
    title: str,
    description: str,
    metadata: Mapping[str, Any] | None,
) -> str:
    parts = [filename, title, description]
    md = metadata or {}
    for key in (
        "name", "filename", "title", "description", "subject", "subjects",
        "keywords", "tags", "resource_type", "resourceType", "format",
    ):
        value = md.get(key)
        if isinstance(value, str):
            parts.append(value)
        elif isinstance(value, (list, tuple, set)):
            parts.extend(str(item) for item in value if isinstance(item, (str, int, float)))
    return " ".join(part for part in parts if part).casefold()


def assess_artifact_metadata(
    *,
    locator: str,
    content_type: str | None = None,
    filename: str = "",
    title: str = "",
    description: str = "",
    metadata: Mapping[str, Any] | None = None,
    expected_artifact_family: str = "",
) -> ArtifactMetadataAssessment:
    """Admission before any artifact HTTP request.

    Search/repository metadata is scheduling state only.  The function decides
    whether a lead is worth L2 triage; it never grants evidence authority.
    """
    media_type = (content_type or "").split(";", 1)[0].strip().casefold()
    path = urlsplit(locator).path.casefold()
    filename_path = str(filename or "").strip().casefold()
    classification_target = filename_path or locator
    format_kind, _compression = classify_artifact(
        classification_target,
        content_type,
    )
    if format_kind == "UNKNOWN" and classification_target != locator:
        format_kind, _compression = classify_artifact(locator, content_type)

    if (
        media_type in _REJECT_MEDIA_TYPES
        or any(media_type.startswith(prefix) for prefix in _REJECT_MEDIA_PREFIXES)
        or any(path.endswith(suffix) or filename_path.endswith(suffix) for suffix in _REJECT_SUFFIXES)
        or filename_path.startswith(("readme.", "license.", "citation."))
    ):
        return ArtifactMetadataAssessment(
            MetadataArtifactAdmission.REJECT,
            format_kind,
            "metadata identifies non-ingestible document/media/software",
        )

    expected = str(expected_artifact_family or "").strip().upper()
    if expected in _STRONG_ARTIFACT_FORMATS | _SEMANTIC_ARTIFACT_FORMATS:
        if format_kind != "UNKNOWN" and format_kind != expected:
            return ArtifactMetadataAssessment(
                MetadataArtifactAdmission.REJECT,
                format_kind,
                f"artifact format {format_kind} conflicts with expected {expected}",
            )

    if format_kind in _STRONG_ARTIFACT_FORMATS:
        return ArtifactMetadataAssessment(
            MetadataArtifactAdmission.ACCEPT,
            format_kind,
            "strong historical-web artifact format",
            (format_kind.casefold(),),
        )

    text = _metadata_text(
        filename=filename,
        title=title,
        description=description,
        metadata=metadata,
    )
    hits = tuple(
        name for name, pattern in _POSITIVE_METADATA_PATTERNS
        if pattern.search(text)
    )

    if format_kind in _SEMANTIC_ARTIFACT_FORMATS:
        if hits or expected == format_kind:
            return ArtifactMetadataAssessment(
                MetadataArtifactAdmission.ACCEPT,
                format_kind,
                "supported generic artifact plus historical-web metadata signal",
                hits,
            )
        return ArtifactMetadataAssessment(
            MetadataArtifactAdmission.HOLD,
            format_kind,
            "generic table/list lacks historical-web semantic signal",
        )

    if any(path.endswith(suffix) or filename_path.endswith(suffix) for suffix in _HOLD_CONTAINER_SUFFIXES):
        return ArtifactMetadataAssessment(
            MetadataArtifactAdmission.HOLD,
            format_kind,
            "container/columnar artifact is not directly ingestible",
            hits,
        )

    if hits:
        return ArtifactMetadataAssessment(
            MetadataArtifactAdmission.HOLD,
            format_kind,
            "metadata is relevant but artifact contract/format is unresolved",
            hits,
        )

    return ArtifactMetadataAssessment(
        MetadataArtifactAdmission.REJECT,
        format_kind,
        "metadata has no historical-web artifact signal",
    )


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
    sample_is_formal_evidence: bool = field(default=False, init=False)

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
    if contract_match is not None:
        contract_match = contract_match.strip() or None
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
    elif contract_match is None:
        # Unknown contract is work state, not negative evidence. This check
        # intentionally precedes unsupported-format rejection because the
        # missing contract may be exactly what resolves an opaque suffix.
        reason = "UNKNOWN_CONTRACT_FAMILY"
    elif format_kind not in policy.allow_formats:
        reason = "UNSUPPORTED_FORMAT"

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

    semantic_reject = (
        triage.disposition is TriageDisposition.REJECT
        or reason == "UNSUPPORTED_FORMAT"
        or reason == "HTML/login or error object is not an artifact"
    )
    admission = (
        ArtifactAdmission.REJECT
        if semantic_reject
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
