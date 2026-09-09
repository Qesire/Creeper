"""Formal V3 submission archive exporter."""

from __future__ import annotations

import hashlib
import json
import zipfile
from collections import defaultdict
from pathlib import Path

from creeper.submission.precheck import precheck_submission
from creeper.submission.snapshot import SubmissionSnapshot


def build_submission_zip(
    snapshot: SubmissionSnapshot,
    name: str,
    output_dir: Path,
    *,
    source_root: Path,
    documentation_path: Path,
) -> Path:
    report = precheck_submission(snapshot)
    if not report.ready:
        raise ValueError("submission precheck failed: " + "; ".join(report.reasons))
    if not name or any(ch in name for ch in "/\\\0"):
        raise ValueError("invalid submission name")
    if not source_root.is_dir():
        raise FileNotFoundError(f"source root does not exist: {source_root}")
    if not documentation_path.is_file() or documentation_path.suffix.lower() != ".docx":
        raise FileNotFoundError(f"Word documentation is required: {documentation_path}")
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_time = snapshot.created_at.replace(":", "").replace("-", "").replace("+00:00", "Z")
    archive = output_dir / f"DomainDataCollectionTask_{safe_time}_{name}.zip"
    annual: dict[int, set[str]] = defaultdict(set)
    evidence_lines = []
    for record in snapshot.novel_records:
        annual[record.year].add(record.hostname)
        evidence_lines.append(json.dumps(record.__dict__, sort_keys=True) + "\n")
    entries: dict[str, bytes] = {}
    for year in range(1996, 2002):
        entries[f"{year}.txt"] = "".join(
            host + "\n" for host in sorted(annual[year])
        ).encode()
    entries["active_candidates.txt"] = "".join(
        host + "\n" for host in sorted(set(snapshot.active_candidates))
    ).encode()
    entries["candidate_pool_unparsed_format.txt"] = "".join(
        raw + "\n" for raw in snapshot.unparsed
    ).encode()
    entries["isc_reference/manifest.json"] = json.dumps(
        {"records": sorted(set(snapshot.isc_reference))}, indent=2
    ).encode()
    entries["evidence.jsonl"] = "".join(evidence_lines).encode()
    entries["reports/eed.json"] = json.dumps(snapshot.eed_report, indent=2, sort_keys=True).encode()
    entries["cdx_audit.json"] = json.dumps(list(snapshot.cdx_audit_set), indent=2).encode()
    entries["source_reports.json"] = json.dumps(list(snapshot.source_report_set), indent=2).encode()
    entries["method_failure_summary.json"] = json.dumps(
        {"incomplete_query_count": snapshot.incomplete_query_count}, indent=2
    ).encode()
    entries[f"documentation/{documentation_path.name}"] = documentation_path.read_bytes()
    excluded_parts = {
        ".git", ".venv", "build", "dist", "__pycache__", ".pytest_cache",
    }
    source_files = []
    for path in sorted(source_root.rglob("*")):
        if (
            not path.is_file()
            or any(part in excluded_parts for part in path.parts)
            or any(part.endswith(".egg-info") for part in path.parts)
            or path.name.endswith(".egg-info")
        ):
            continue
        relative = path.relative_to(source_root)
        entries[f"code/{relative.as_posix()}"] = path.read_bytes()
        source_files.append(relative.as_posix())
    manifest = {
        "format_version": "submission-v1",
        "submission_snapshot_id": snapshot.submission_snapshot_id,
        "created_at": snapshot.created_at,
        "baseline_id": snapshot.baseline_id,
        "baseline_hashes": snapshot.baseline_hashes,
        "policy_versions": {
            "normalizer": snapshot.normalizer_version,
            "evidence": snapshot.evidence_policy_version,
            "eed": snapshot.eed_policy_version,
        },
        "code_revision": snapshot.code_revision,
        "novel_records": len(snapshot.novel_records),
        "active_candidates": len(snapshot.active_candidates),
        "active_candidate_scopes": list(snapshot.active_candidate_scopes),
        "source_files": source_files,
        "documentation_file": f"documentation/{documentation_path.name}",
        "novel_eed": snapshot.novel_eed,
        "growth_rate": snapshot.growth_rate,
        "evidence_coverage": snapshot.evidence_coverage,
        "entry_sha256": {
            path: hashlib.sha256(payload).hexdigest()
            for path, payload in sorted(entries.items())
        },
    }
    entries["MANIFEST.json"] = json.dumps(manifest, indent=2, sort_keys=True).encode()
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for path, payload in sorted(entries.items()):
            bundle.writestr(path, payload)
    return archive
