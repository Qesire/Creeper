"""Immutable identity for historical-index objects.

Region byte bounds are authority only for the exact object that tomography
observed.  Size alone is insufficient because a mutable archive can replace an
object with different bytes at the same length.  This module keeps object
identity explicit and transport-independent so probe and harvest paths can
share the same fail-closed comparison rules.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


IDENTITY_CHANGED_MESSAGE = (
    "historical index object identity changed after tomography"
)
IDENTITY_UNVERIFIABLE_MESSAGE = (
    "historical index object identity cannot be verified"
)


class HistoricalIndexIdentityError(ValueError):
    """An index object is unverifiable or no longer matches its bound identity."""


def normalize_strong_etag(value: str | None) -> str | None:
    """Return a normalized strong ETag, discarding weak validators."""

    if value is None:
        return None
    normalized = value.strip()
    if not normalized or normalized[:2].lower() == "w/":
        return None
    return normalized


@dataclass(frozen=True)
class HistoricalIndexObjectIdentity:
    """Low-cost immutable identity captured for one historical index object."""

    kind: str
    content_length: int | None = None
    etag: str | None = None
    last_modified: str | None = None
    local_device: int | None = None
    local_inode: int | None = None
    local_mtime_ns: int | None = None
    sampled_fingerprint: str | None = None

    def __post_init__(self) -> None:
        normalized_kind = self.kind.strip().lower()
        if normalized_kind not in {"local", "remote"}:
            raise ValueError("historical index identity kind must be local or remote")
        object.__setattr__(self, "kind", normalized_kind)
        if self.content_length is not None and self.content_length < 0:
            raise ValueError("identity content_length must be non-negative")
        if self.local_mtime_ns is not None and self.local_mtime_ns < 0:
            raise ValueError("identity local_mtime_ns must be non-negative")
        if self.etag is not None:
            normalized_etag = normalize_strong_etag(self.etag)
            if normalized_etag is None:
                raise ValueError("identity etag must be a strong ETag")
            object.__setattr__(self, "etag", normalized_etag)
        if self.last_modified is not None:
            normalized_last_modified = self.last_modified.strip()
            object.__setattr__(
                self,
                "last_modified",
                normalized_last_modified or None,
            )
        if self.sampled_fingerprint is not None:
            normalized_fingerprint = self.sampled_fingerprint.strip()
            object.__setattr__(
                self,
                "sampled_fingerprint",
                normalized_fingerprint or None,
            )

    @property
    def is_verifiable(self) -> bool:
        if self.kind == "local":
            return (
                self.content_length is not None
                and self.local_device is not None
                and self.local_inode is not None
                and self.local_mtime_ns is not None
                and self.sampled_fingerprint is not None
            )
        return (
            self.content_length is not None
            and (
                self.etag is not None
                or self.last_modified is not None
                or self.sampled_fingerprint is not None
            )
        )

    @property
    def if_range_validator(self) -> str | None:
        if self.kind != "remote":
            return None
        if self.etag is not None:
            return self.etag
        return self.last_modified


def _identity_mismatch(
    expected: HistoricalIndexObjectIdentity,
    observed: HistoricalIndexObjectIdentity,
) -> bool:
    if expected.kind != observed.kind:
        return True
    if (
        expected.content_length is None
        or observed.content_length is None
        or expected.content_length != observed.content_length
    ):
        return True

    if expected.kind == "local":
        return (
            expected.local_device != observed.local_device
            or expected.local_inode != observed.local_inode
            or expected.local_mtime_ns != observed.local_mtime_ns
            or expected.sampled_fingerprint != observed.sampled_fingerprint
        )

    if expected.etag is not None:
        return observed.etag != expected.etag
    if expected.last_modified is not None:
        return observed.last_modified != expected.last_modified
    if expected.sampled_fingerprint is not None:
        return observed.sampled_fingerprint != expected.sampled_fingerprint
    return True


def ensure_same_historical_index_object(
    expected: HistoricalIndexObjectIdentity,
    observed: HistoricalIndexObjectIdentity,
) -> None:
    """Fail closed unless an observation verifies the already-bound object."""

    if not expected.is_verifiable or not observed.is_verifiable:
        raise HistoricalIndexIdentityError(IDENTITY_UNVERIFIABLE_MESSAGE)
    if _identity_mismatch(expected, observed):
        raise HistoricalIndexIdentityError(IDENTITY_CHANGED_MESSAGE)


def remote_identity_from_headers(
    headers: Mapping[str, str],
    *,
    content_length: int | None,
    sampled_fingerprint: str | None = None,
) -> HistoricalIndexObjectIdentity:
    """Build remote identity from response validators and object total length.

    For a Range response, callers must pass Content-Range's total object size,
    not the response body's Content-Length.
    """

    return HistoricalIndexObjectIdentity(
        kind="remote",
        content_length=content_length,
        etag=normalize_strong_etag(headers.get("etag")),
        last_modified=headers.get("last-modified"),
        sampled_fingerprint=sampled_fingerprint,
    )


def capture_local_identity(
    path: Path,
    *,
    sample_bytes: int = 4096,
) -> HistoricalIndexObjectIdentity:
    """Capture stat identity plus a deterministic bounded content fingerprint."""

    if sample_bytes < 1:
        raise ValueError("sample_bytes must be positive")
    try:
        before = path.stat()
        size = int(before.st_size)
        width = min(sample_bytes, size)
        offsets = (
            ()
            if width == 0
            else tuple(
                sorted(
                    {
                        0,
                        max(0, (size - width) // 2),
                        max(0, size - width),
                    }
                )
            )
        )
        digest = hashlib.sha256()
        digest.update(b"creeper-historical-index-sample-v1\0")
        digest.update(str(size).encode("ascii"))
        with path.open("rb") as source:
            for offset in offsets:
                source.seek(offset)
                payload = source.read(width)
                digest.update(b"\0")
                digest.update(str(offset).encode("ascii"))
                digest.update(b":")
                digest.update(str(len(payload)).encode("ascii"))
                digest.update(b":")
                digest.update(payload)
        after = path.stat()
    except OSError as exc:
        raise HistoricalIndexIdentityError(
            f"unable to capture local historical index identity: {exc}"
        ) from exc

    before_tuple = (
        int(before.st_dev),
        int(before.st_ino),
        int(before.st_size),
        int(before.st_mtime_ns),
    )
    after_tuple = (
        int(after.st_dev),
        int(after.st_ino),
        int(after.st_size),
        int(after.st_mtime_ns),
    )
    if before_tuple != after_tuple:
        raise HistoricalIndexIdentityError(IDENTITY_CHANGED_MESSAGE)

    return HistoricalIndexObjectIdentity(
        kind="local",
        content_length=size,
        local_device=int(after.st_dev),
        local_inode=int(after.st_ino),
        local_mtime_ns=int(after.st_mtime_ns),
        sampled_fingerprint="sha256:" + digest.hexdigest(),
    )
