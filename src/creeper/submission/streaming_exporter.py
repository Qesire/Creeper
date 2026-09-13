"""Bounded-memory formal submission archive construction."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import zipfile
from collections.abc import Iterable, Iterator
from contextlib import ExitStack
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from creeper.authority.normalizer import normalize_official
from creeper.evidence.classification import (
    classify_acquisition_lane,
    contribution_bucket,
)
from creeper.evidence.policies import EvidenceCapsule
from creeper.storage.candidate_store import CandidateStore
from creeper.submission.artifact_manifest import (
    ArtifactSpec,
    ResolvedArtifact,
    artifact_manifest_rows,
    resolve_artifacts,
)
from creeper.submission.precheck import precheck_submission
from creeper.submission.snapshot import SubmissionSnapshot


_CHUNK_SIZE = 1024 * 1024
_EXCLUDED_SOURCE_PARTS = {
    ".git",
    ".venv",
    "build",
    "dist",
    "__pycache__",
    ".pytest_cache",
}


def _zip_info(name: str, created_at: str) -> zipfile.ZipInfo:
    """Build deterministic ZIP metadata from the frozen snapshot timestamp."""
    try:
        dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        year = max(1980, min(2107, dt.year))
        date_time = (year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
    except ValueError:
        date_time = (1980, 1, 1, 0, 0, 0)
    info = zipfile.ZipInfo(name, date_time=date_time)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o100644 << 16
    return info


def _remember_hash(
    hashes: dict[str, str],
    name: str,
    digest: str,
) -> None:
    if name in hashes:
        raise ValueError(f"duplicate submission entry: {name}")
    hashes[name] = digest


def _write_bytes(
    bundle: zipfile.ZipFile,
    name: str,
    payload: bytes,
    *,
    created_at: str,
    hashes: dict[str, str],
) -> None:
    digest = hashlib.sha256(payload).hexdigest()
    with bundle.open(_zip_info(name, created_at), "w") as target:
        target.write(payload)
    _remember_hash(hashes, name, digest)


def _write_file(
    bundle: zipfile.ZipFile,
    name: str,
    source_path: Path,
    *,
    created_at: str,
    hashes: dict[str, str],
    expected: ResolvedArtifact | None = None,
    chunk_size: int = _CHUNK_SIZE,
) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with source_path.open("rb") as source:
        with bundle.open(_zip_info(name, created_at), "w") as target:
            while True:
                chunk = source.read(chunk_size)
                if not chunk:
                    break
                digest.update(chunk)
                size += len(chunk)
                target.write(chunk)
    value = digest.hexdigest()
    if expected is not None and (
        value != expected.sha256 or size != expected.size
    ):
        raise RuntimeError(
            f"artifact changed after manifesting: {expected.source_path}"
        )
    _remember_hash(hashes, name, value)
    return value, size


def _candidate_json(entry) -> str:
    payload = asdict(entry)
    for key, value in tuple(payload.items()):
        if hasattr(value, "value"):
            payload[key] = value.value
    return json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"


def _stage_candidates(
    directory: Path,
    snapshot: SubmissionSnapshot,
    candidate_store: CandidateStore | None,
) -> dict[str, object]:
    active_txt = directory / "active_candidates.txt"
    active_jsonl = directory / "candidate_active.jsonl"
    isc_jsonl = directory / "candidate_isc.jsonl"
    isc_manifest = directory / "isc_manifest.json"
    unparsed_txt = directory / "candidate_unparsed.txt"
    unparsed_jsonl = directory / "candidate_unparsed.jsonl"

    active_count = 0
    isc_count = 0
    unparsed_count = 0
    active_scopes: set[str] = set()

    with (
        active_txt.open("w", encoding="utf-8", newline="\n") as active_out,
        active_jsonl.open("w", encoding="utf-8", newline="\n") as active_audit,
        isc_jsonl.open("w", encoding="utf-8", newline="\n") as isc_audit,
        isc_manifest.open("w", encoding="utf-8", newline="\n") as isc_out,
        unparsed_txt.open("w", encoding="utf-8", newline="\n") as unparsed_out,
        unparsed_jsonl.open("w", encoding="utf-8", newline="\n") as unparsed_audit,
    ):
        if candidate_store is not None:
            last_active = None
            for entry in candidate_store.iter_active_candidates():
                active_audit.write(_candidate_json(entry))
                active_scopes.add(entry.scope.value)
                if entry.hostname != last_active:
                    active_out.write(entry.hostname + "\n")
                    active_count += 1
                    last_active = entry.hostname

            isc_out.write('{"records":[')
            first = True
            last_isc = None
            for entry in candidate_store.iter_isc_reference():
                isc_audit.write(_candidate_json(entry))
                if entry.hostname == last_isc:
                    continue
                if not first:
                    isc_out.write(",")
                isc_out.write(json.dumps(entry.hostname))
                first = False
                isc_count += 1
                last_isc = entry.hostname
            isc_out.write("]}\n")

            for entry in candidate_store.iter_unparsed():
                unparsed_out.write(entry.raw_value + "\n")
                unparsed_audit.write(_candidate_json(entry))
                unparsed_count += 1
        else:
            pairs = sorted(set(zip(
                snapshot.active_candidates,
                snapshot.active_candidate_scopes,
                strict=True,
            )))
            for hostname, scope in pairs:
                active_out.write(hostname + "\n")
                active_audit.write(json.dumps(
                    {"hostname": hostname, "scope": scope},
                    sort_keys=True,
                    separators=(",", ":"),
                ) + "\n")
                active_scopes.add(scope)
                active_count += 1

            isc_out.write('{"records":[')
            for index, hostname in enumerate(sorted(set(snapshot.isc_reference))):
                if index:
                    isc_out.write(",")
                isc_out.write(json.dumps(hostname))
                isc_audit.write(json.dumps(
                    {"hostname": hostname, "status": "ISC_REFERENCE"},
                    sort_keys=True,
                    separators=(",", ":"),
                ) + "\n")
                isc_count += 1
            isc_out.write("]}\n")

            for raw_value in sorted(snapshot.unparsed):
                unparsed_out.write(raw_value + "\n")
                unparsed_audit.write(json.dumps(
                    {
                        "raw_value": raw_value,
                        "reason": "legacy_snapshot_unparsed",
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ) + "\n")
                unparsed_count += 1

    return {
        "paths": {
            "active_candidates.txt": active_txt,
            "candidate_ledger/active.jsonl": active_jsonl,
            "candidate_pool_unparsed_format.txt": unparsed_txt,
            "candidate_ledger/unparsed.jsonl": unparsed_jsonl,
            "isc_reference/manifest.json": isc_manifest,
            "candidate_ledger/isc_reference.jsonl": isc_jsonl,
        },
        "active_count": active_count,
        "isc_count": isc_count,
        "unparsed_count": unparsed_count,
        "active_scopes": sorted(active_scopes),
    }


def _stage_evidence(
    directory: Path,
    records: Iterable[EvidenceCapsule],
) -> dict[str, object]:
    evidence_path = directory / "evidence.jsonl"
    annual_paths = {
        year: directory / f"{year}.txt"
        for year in range(1996, 2002)
    }
    annual_files = {
        year: path.open("w", encoding="utf-8", newline="\n")
        for year, path in annual_paths.items()
    }
    count = 0
    lane_counts = {
        "direct_annual": 0,
        "verified_candidate": 0,
        "other_restricted": 0,
    }
    duplicate_count = 0
    last_key: tuple[str, int] | None = None
    try:
        with evidence_path.open("w", encoding="utf-8", newline="\n") as evidence:
            for capsule in records:
                hostname = normalize_official(capsule.hostname)
                year = int(capsule.year)
                if hostname is None:
                    raise ValueError(
                        f"streaming evidence contains invalid hostname: {capsule.hostname}"
                    )
                if year not in annual_paths:
                    raise ValueError(
                        f"streaming evidence year out of range: {year}"
                    )
                key = (hostname, year)
                if last_key is not None and key < last_key:
                    raise ValueError(
                        "streaming evidence must be ordered by (hostname, year)"
                    )
                if key == last_key:
                    duplicate_count += 1
                    continue
                last_key = key
                row = capsule.__dict__.copy()
                row["hostname"] = hostname
                row["year"] = year
                evidence.write(
                    json.dumps(
                        row,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                annual_files[year].write(hostname + "\n")
                count += 1
                bucket = contribution_bucket(classify_acquisition_lane(capsule))
                lane_counts[bucket] += 1
    finally:
        for output in annual_files.values():
            output.close()
    return {
        "evidence_path": evidence_path,
        "annual_paths": annual_paths,
        "count": count,
        "lane_counts": lane_counts,
        "duplicate_count": duplicate_count,
    }


def _iter_source_files(
    source_root: Path,
    *,
    excluded_root: Path | None,
) -> Iterator[tuple[Path, str]]:
    paths = []
    for path in source_root.rglob("*"):
        if not path.is_file():
            continue
        if any(part in _EXCLUDED_SOURCE_PARTS for part in path.parts):
            continue
        if any(part.endswith(".egg-info") for part in path.parts):
            continue
        if path.name.endswith(".egg-info"):
            continue
        if excluded_root is not None:
            try:
                path.resolve().relative_to(excluded_root.resolve())
                continue
            except ValueError:
                pass
        paths.append(path)
    for path in sorted(paths):
        yield path, path.relative_to(source_root).as_posix()


def _source_contribution(
    snapshot: SubmissionSnapshot,
    *,
    lane_records: dict[str, int],
) -> dict[str, object]:
    if snapshot.source_contribution is not None:
        return snapshot.source_contribution
    # The streaming fallback can report exact host-year counts without retaining
    # an in-memory evidence map. EED attribution remains zero unless supplied by
    # the authoritative runtime readiness/EED report; never assign total EED to
    # one lane heuristically.
    return {
        "by_source": {},
        "direct_annual": {
            "novel_host_years": int(lane_records["direct_annual"]),
            "novel_eed": "0",
        },
        "verified_candidate": {
            "novel_host_years": int(lane_records["verified_candidate"]),
            "novel_eed": "0",
        },
        "other_restricted": {
            "novel_host_years": int(lane_records["other_restricted"]),
            "novel_eed": "0",
        },
        "note": (
            "Per-lane EED attribution was not supplied by the authoritative "
            "readiness report; streaming export preserves exact classifier "
            "host-year counts and does not invent EED allocation."
        ),
    }


def build_streaming_submission_zip(
    snapshot: SubmissionSnapshot,
    name: str,
    output_dir: Path,
    *,
    source_root: Path,
    documentation_path: Path,
    evidence_records: Iterable[EvidenceCapsule],
    candidate_store: CandidateStore | None = None,
    artifact_specs: Iterable[ArtifactSpec] = (),
    artifact_allowed_roots: Iterable[Path] = (),
    require_production_config: bool = True,
) -> Path:
    """Build a deterministic formal archive without O(total-evidence) memory.

    evidence_records must be canonical and monotonically ordered by
    (hostname, year). EvidenceStore.iter_canonical_host_year_capsules() is the
    intended production source.
    """
    report = precheck_submission(snapshot)
    if not report.ready:
        raise ValueError(
            "submission precheck failed: " + "; ".join(report.reasons)
        )
    if not name or any(ch in name for ch in "/\\\0"):
        raise ValueError("invalid submission name")
    source_root = Path(source_root)
    documentation_path = Path(documentation_path)
    output_dir = Path(output_dir)
    if not source_root.is_dir():
        raise FileNotFoundError(f"source root does not exist: {source_root}")
    if (
        not documentation_path.is_file()
        or documentation_path.suffix.lower() != ".docx"
    ):
        raise FileNotFoundError(
            f"Word documentation is required: {documentation_path}"
        )

    allowed_roots = tuple(artifact_allowed_roots) or (source_root,)
    resolved_artifacts = resolve_artifacts(
        artifact_specs,
        allowed_roots=allowed_roots,
    )
    if require_production_config and not any(
        artifact.logical_role == "production_config"
        for artifact in resolved_artifacts
    ):
        raise ValueError(
            "formal streaming export requires the production_config artifact"
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    safe_time = (
        snapshot.created_at.replace(":", "").replace("-", "").replace("+00:00", "Z")
    )
    archive = output_dir / f"DomainDataCollectionTask_{safe_time}_{name}.zip"
    archive_tmp = output_dir / f".{archive.name}.partial"
    archive_tmp.unlink(missing_ok=True)

    with ExitStack() as cleanup:
        cleanup.callback(archive_tmp.unlink, missing_ok=True)
        with tempfile.TemporaryDirectory(
            prefix=".creeper-export-",
            dir=output_dir,
        ) as tmp:
            staging = Path(tmp)
            evidence_stage = _stage_evidence(staging, evidence_records)
            candidate_stage = _stage_candidates(staging, snapshot, candidate_store)

            hashes: dict[str, str] = {}
            source_files: list[str] = []
            with zipfile.ZipFile(
                archive_tmp,
                "w",
                compression=zipfile.ZIP_DEFLATED,
                allowZip64=True,
            ) as bundle:
                for year in range(1996, 2002):
                    _write_file(
                        bundle,
                        f"{year}.txt",
                        evidence_stage["annual_paths"][year],
                        created_at=snapshot.created_at,
                        hashes=hashes,
                    )
                _write_file(
                    bundle,
                    "evidence.jsonl",
                    evidence_stage["evidence_path"],
                    created_at=snapshot.created_at,
                    hashes=hashes,
                )
                for archive_path, path in candidate_stage["paths"].items():
                    _write_file(
                        bundle,
                        archive_path,
                        path,
                        created_at=snapshot.created_at,
                        hashes=hashes,
                    )

                _write_bytes(
                    bundle,
                    "reports/eed.json",
                    json.dumps(
                        snapshot.eed_report,
                        indent=2,
                        sort_keys=True,
                    ).encode(),
                    created_at=snapshot.created_at,
                    hashes=hashes,
                )
                streamed_count = int(evidence_stage["count"])
                reconciliation = {
                    "baseline_id": snapshot.baseline_id,
                    "baseline_eed": snapshot.baseline_eed,
                    "input_records": (
                        streamed_count
                        + snapshot.invalid_count
                        + snapshot.overlap_count
                        + snapshot.within_year_duplicates
                        + int(evidence_stage["duplicate_count"])
                    ),
                    "within_year_duplicates": (
                        snapshot.within_year_duplicates
                        + int(evidence_stage["duplicate_count"])
                    ),
                    "invalid_records": snapshot.invalid_count,
                    "baseline_overlap": snapshot.overlap_count,
                    "novel_host_years": streamed_count,
                    "novel_eed": snapshot.novel_eed,
                    "growth_rate": snapshot.growth_rate,
                }
                _write_bytes(
                    bundle,
                    "reports/baseline_reconciliation.json",
                    json.dumps(
                        reconciliation,
                        indent=2,
                        sort_keys=True,
                    ).encode(),
                    created_at=snapshot.created_at,
                    hashes=hashes,
                )
                _write_bytes(
                    bundle,
                    "reports/source_contribution.json",
                    json.dumps(
                        _source_contribution(
                            snapshot,
                            lane_records=dict(evidence_stage["lane_counts"]),
                        ),
                        indent=2,
                        sort_keys=True,
                    ).encode(),
                    created_at=snapshot.created_at,
                    hashes=hashes,
                )
                _write_bytes(
                    bundle,
                    "cdx_audit.json",
                    json.dumps(
                        list(snapshot.cdx_audit_set),
                        indent=2,
                    ).encode(),
                    created_at=snapshot.created_at,
                    hashes=hashes,
                )
                _write_bytes(
                    bundle,
                    "source_reports.json",
                    json.dumps(
                        list(snapshot.source_report_set),
                        indent=2,
                    ).encode(),
                    created_at=snapshot.created_at,
                    hashes=hashes,
                )
                _write_bytes(
                    bundle,
                    "method_failure_summary.json",
                    json.dumps(
                        {
                            "incomplete_query_count": snapshot.incomplete_query_count,
                        },
                        indent=2,
                    ).encode(),
                    created_at=snapshot.created_at,
                    hashes=hashes,
                )

                documentation_archive = f"documentation/{documentation_path.name}"
                _write_file(
                    bundle,
                    documentation_archive,
                    documentation_path,
                    created_at=snapshot.created_at,
                    hashes=hashes,
                )

                for path, relative in _iter_source_files(
                    source_root,
                    excluded_root=output_dir,
                ):
                    _write_file(
                        bundle,
                        f"code/{relative}",
                        path,
                        created_at=snapshot.created_at,
                        hashes=hashes,
                    )
                    source_files.append(relative)

                artifact_rows = artifact_manifest_rows(resolved_artifacts)
                _write_bytes(
                    bundle,
                    "artifacts/manifest.json",
                    json.dumps(
                        artifact_rows,
                        indent=2,
                        sort_keys=True,
                    ).encode(),
                    created_at=snapshot.created_at,
                    hashes=hashes,
                )
                for artifact in resolved_artifacts:
                    _write_file(
                        bundle,
                        artifact.archive_path,
                        artifact.source_path,
                        created_at=snapshot.created_at,
                        hashes=hashes,
                        expected=artifact,
                    )

                manifest = {
                    "format_version": "submission-v2",
                    "exporter": "streaming-v1",
                    "submission_snapshot_id": snapshot.submission_snapshot_id,
                    "created_at": snapshot.created_at,
                    "baseline_id": snapshot.baseline_id,
                    "baseline_hashes": snapshot.baseline_hashes,
                    "candidate_file_hash": snapshot.candidate_file_hash,
                    "model_hash": snapshot.model_hash,
                    "baseline_eed": snapshot.baseline_eed,
                    "authority_digest": snapshot.authority_digest,
                    "policy_versions": {
                        "normalizer": snapshot.normalizer_version,
                        "evidence": snapshot.evidence_policy_version,
                        "eed": snapshot.eed_policy_version,
                    },
                    "code_revision": snapshot.code_revision,
                    "novel_records": streamed_count,
                    "active_candidates": candidate_stage["active_count"],
                    "active_candidate_scopes": candidate_stage["active_scopes"],
                    "isc_reference_records": candidate_stage["isc_count"],
                    "unparsed_records": candidate_stage["unparsed_count"],
                    "source_files": source_files,
                    "documentation_file": documentation_archive,
                    "novel_eed": snapshot.novel_eed,
                    "growth_rate": snapshot.growth_rate,
                    "evidence_coverage": snapshot.evidence_coverage,
                    "artifacts": artifact_rows,
                    "entry_sha256": {
                        path: hashes[path]
                        for path in sorted(hashes)
                    },
                }
                manifest_payload = json.dumps(
                    manifest,
                    indent=2,
                    sort_keys=True,
                ).encode()
                # MANIFEST is deliberately last and is not self-hashed.
                with bundle.open(
                    _zip_info("MANIFEST.json", snapshot.created_at),
                    "w",
                ) as target:
                    target.write(manifest_payload)
            # Only publish a final archive after every entry (including required
            # external artifacts) has been copied and re-verified successfully.
            os.replace(archive_tmp, archive)
        cleanup.pop_all()
    return archive
