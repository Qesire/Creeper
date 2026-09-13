"""Resolve, hash, and fail-close external submission artifacts."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable


@dataclass(frozen=True)
class ArtifactSpec:
    logical_role: str
    source_path: Path
    archive_path: str
    required: bool = True
    allow_external: bool = False
    license_or_access_note: str = ""


@dataclass(frozen=True)
class ResolvedArtifact:
    logical_role: str
    source_path: Path
    archive_path: str
    sha256: str
    size: int
    required: bool
    license_or_access_note: str


def _sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while True:
            chunk = source.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_archive_path(value: str) -> str:
    path = PurePosixPath(value)
    if not value or path.is_absolute() or ".." in path.parts:
        raise ValueError(f"invalid artifact archive path: {value!r}")
    normalized = path.as_posix()
    if normalized == "MANIFEST.json":
        raise ValueError("external artifact cannot replace MANIFEST.json")
    return normalized


def _within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def resolve_artifacts(
    specs: Iterable[ArtifactSpec],
    *,
    allowed_roots: Iterable[Path] = (),
) -> tuple[ResolvedArtifact, ...]:
    """Freeze artifact identities before archive construction.

    Required missing files fail immediately. Files outside the configured
    policy roots require an explicit per-artifact allow_external=True.
    """
    roots = tuple(Path(root).resolve() for root in allowed_roots)
    resolved: list[ResolvedArtifact] = []
    seen_archive_paths: set[str] = set()
    for spec in specs:
        if not spec.logical_role.strip():
            raise ValueError("artifact logical_role is required")
        archive_path = _normalize_archive_path(spec.archive_path)
        if archive_path in seen_archive_paths:
            raise ValueError(f"duplicate artifact archive path: {archive_path}")
        seen_archive_paths.add(archive_path)

        source_path = Path(spec.source_path).expanduser().resolve()
        if not source_path.exists():
            if spec.required:
                raise FileNotFoundError(
                    f"required artifact does not exist: {source_path}"
                )
            continue
        if not source_path.is_file():
            raise FileNotFoundError(f"artifact is not a file: {source_path}")
        if roots and not any(_within(source_path, root) for root in roots):
            if not spec.allow_external:
                raise PermissionError(
                    "artifact is outside allowed roots and was not explicitly "
                    f"allowed: {source_path}"
                )
        elif not roots and not spec.allow_external:
            raise PermissionError(
                "artifact policy roots are empty; external artifact requires "
                f"allow_external=True: {source_path}"
            )

        stat = source_path.stat()
        resolved.append(
            ResolvedArtifact(
                logical_role=spec.logical_role,
                source_path=source_path,
                archive_path=archive_path,
                sha256=_sha256_file(source_path),
                size=int(stat.st_size),
                required=bool(spec.required),
                license_or_access_note=spec.license_or_access_note,
            )
        )
    return tuple(sorted(resolved, key=lambda item: item.archive_path))


def verify_resolved_artifact(
    artifact: ResolvedArtifact,
    *,
    chunk_size: int = 1024 * 1024,
) -> None:
    """Fail closed if an artifact changed after its manifest identity froze."""
    try:
        size = artifact.source_path.stat().st_size
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"artifact disappeared after manifesting: {artifact.source_path}"
        ) from exc
    if int(size) != artifact.size:
        raise RuntimeError(
            f"artifact changed after manifesting: {artifact.source_path}"
        )
    if _sha256_file(artifact.source_path, chunk_size=chunk_size) != artifact.sha256:
        raise RuntimeError(
            f"artifact changed after manifesting: {artifact.source_path}"
        )


def artifact_manifest_rows(
    artifacts: Iterable[ResolvedArtifact],
) -> list[dict[str, object]]:
    """Return the small control-plane manifest, not artifact payload bytes."""
    return [
        {
            "logical_role": artifact.logical_role,
            "source_path": str(artifact.source_path),
            "archive_path": artifact.archive_path,
            "sha256": artifact.sha256,
            "size": artifact.size,
            "required": artifact.required,
            "license_or_access_note": artifact.license_or_access_note,
        }
        for artifact in artifacts
    ]
