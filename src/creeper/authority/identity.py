"""Stable identity helpers for baseline and EED model authorities."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class AuthoritySnapshot:
    """The complete immutable identity used by every V4-aware component."""

    baseline_id: str
    annual_file_hashes: dict[str, str]
    candidate_file_hash: str
    model_hash: str
    baseline_eed: str
    authority_digest: str

    @classmethod
    def from_manifest(cls, manifest: Mapping[str, object]) -> "AuthoritySnapshot":
        baseline_id = str(manifest.get("baseline_id", "")).strip()
        annual = manifest.get("annual_file_hashes")
        candidate = str(manifest.get("candidate_file_hash", "")).strip()
        model = str(manifest.get("model_hash", "")).strip()
        baseline_eed = str(manifest.get("baseline_eed", "")).strip()
        if not baseline_id or not isinstance(annual, Mapping) or not annual:
            raise ValueError("authority manifest is missing baseline identity")
        expected_years = {f"{year}.txt" for year in range(1996, 2002)}
        if set(str(key) for key in annual) != expected_years:
            raise ValueError("authority manifest must contain all six annual hashes")
        if not candidate or not model or not baseline_eed:
            raise ValueError(
                "authority manifest must include candidate_file_hash, model_hash, and baseline_eed"
            )
        annual_hashes = {str(key): str(value) for key, value in annual.items()}
        if any(
            len(value) != 64
            or any(char not in "0123456789abcdef" for char in value.lower())
            for value in (*annual_hashes.values(), candidate, model)
        ):
            raise ValueError("authority manifest hashes must be SHA-256 values")
        try:
            value = Decimal(baseline_eed)
        except Exception as exc:
            raise ValueError("authority manifest baseline_eed is invalid") from exc
        if not value.is_finite() or value < 0:
            raise ValueError("authority manifest baseline_eed must be non-negative")
        digest = authority_digest(
            baseline_id=baseline_id,
            annual_file_hashes=annual_hashes,
            candidate_file_hash=candidate,
            model_hash=model,
            baseline_eed=format(value, "f"),
        )
        supplied_digest = str(manifest.get("authority_digest", "")).strip()
        if supplied_digest and supplied_digest != digest:
            raise ValueError("authority manifest authority_digest is invalid")
        return cls(
            baseline_id=baseline_id,
            annual_file_hashes=annual_hashes,
            candidate_file_hash=candidate,
            model_hash=model,
            baseline_eed=format(value, "f"),
            authority_digest=digest,
        )

    @classmethod
    def from_manifest_path(cls, path: Path) -> "AuthoritySnapshot":
        try:
            manifest = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"cannot read authority manifest: {path}") from exc
        if not isinstance(manifest, Mapping):
            raise ValueError("authority manifest must contain a JSON object")
        return cls.from_manifest(manifest)

    def as_dict(self) -> dict[str, object]:
        return {
            "baseline_id": self.baseline_id,
            "annual_file_hashes": dict(self.annual_file_hashes),
            "candidate_file_hash": self.candidate_file_hash,
            "model_hash": self.model_hash,
            "baseline_eed": self.baseline_eed,
            "authority_digest": self.authority_digest,
        }


def authority_digest(
    *,
    baseline_id: str,
    annual_file_hashes: Mapping[str, str],
    candidate_file_hash: str,
    model_hash: str,
    baseline_eed: str,
) -> str:
    """Hash the authority inputs, independent of filesystem locations."""
    payload = {
        "baseline_id": baseline_id,
        "annual_file_hashes": dict(sorted(annual_file_hashes.items())),
        "candidate_file_hash": candidate_file_hash,
        "model_hash": model_hash,
        "baseline_eed": baseline_eed,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def baseline_authority_signature(path: Path) -> str:
    """Return the readiness-compatible identity of an immutable baseline file."""
    resolved = Path(path).resolve()
    # A bound index carries the content identity of its source authority. This
    # avoids hashing a multi-gigabyte SQLite file on every readiness cycle.
    try:
        with sqlite3.connect(resolved) as connection:
            row = connection.execute(
                "SELECT value FROM authority_metadata WHERE key = 'authority_digest'"
            ).fetchone()
        if row and row[0]:
            return str(row[0])
    except sqlite3.Error:
        pass
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
