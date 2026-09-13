"""Stable identity helpers for baseline and EED model authorities."""

from __future__ import annotations

import hashlib
from pathlib import Path


def baseline_authority_signature(path: Path) -> str:
    """Return the readiness-compatible identity of an immutable baseline file."""
    resolved = Path(path).resolve()
    stat = resolved.stat()
    payload = (
        f"{resolved}\0{stat.st_size}\0{stat.st_mtime_ns}"
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def eed_model_authority_signature(path: Path) -> str:
    """Hash the small EED weighting model exactly."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()
