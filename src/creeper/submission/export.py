"""Create a reproducible submission archive from accepted evidence capsules."""

from __future__ import annotations

import json
import zipfile
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from creeper.authority.baseline_index import BaselineIndex, YEAR_BITS
from creeper.authority.normalizer import normalize_official
from creeper.evidence.policies import EvidenceCapsule


def _accepted(capsules: Iterable[EvidenceCapsule], index: BaselineIndex):
    seen: set[tuple[str, int]] = set()
    for capsule in capsules:
        hostname = normalize_official(capsule.hostname)
        if hostname is None or capsule.year not in YEAR_BITS:
            continue
        key = (hostname, capsule.year)
        if key in seen or index.year_mask(hostname) & YEAR_BITS[capsule.year]:
            continue
        seen.add(key)
        yield EvidenceCapsule(
            hostname=hostname,
            year=capsule.year,
            provider=capsule.provider,
            temporal_semantics=capsule.temporal_semantics,
            evidence_timestamp=capsule.evidence_timestamp,
            source_locator=capsule.source_locator,
            payload_hash=capsule.payload_hash,
            policy_version=capsule.policy_version,
            evidence_type=capsule.evidence_type,
            source_id=capsule.source_id,
            original_url=capsule.original_url,
            record_locator=capsule.record_locator,
            extraction_method=capsule.extraction_method,
        )


def export_submission(
    capsules: Iterable[EvidenceCapsule],
    index: BaselineIndex,
    output_dir: Path,
    *,
    contributor: str = "local",
    submission_time: datetime | None = None,
) -> Path:
    """Write annual text files, evidence provenance, and a manifest in a zip."""
    accepted = list(_accepted(capsules, index))
    when = submission_time or datetime.now(timezone.utc)
    stamp = when.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_name = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in contributor)
    archive = output_dir / f"DomainDataCollectionTask_{stamp}_{safe_name}.zip"
    by_year: dict[int, set[str]] = defaultdict(set)
    for capsule in accepted:
        by_year[capsule.year].add(capsule.hostname)
    manifest = {
        "format_version": "submission-v1",
        "contributor": contributor,
        "submission_time": when.astimezone(timezone.utc).isoformat(),
        "records": len(accepted),
        "years": {str(year): len(by_year[year]) for year in range(1996, 2002)},
        "annual_files": [f"annual/{year}.txt" for year in range(1996, 2002)],
        "evidence_policy": sorted({c.policy_version for c in accepted}),
        "baseline_index": str(index.path),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr("manifest.json", json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        bundle.writestr(
            "evidence.jsonl",
            "".join(json.dumps(asdict(c), sort_keys=True) + "\n" for c in accepted),
        )
        for year in range(1996, 2002):
            bundle.writestr(
                f"annual/{year}.txt",
                "".join(host + "\n" for host in sorted(by_year[year])),
            )
    return archive
