#!/usr/bin/env python3
"""Write an evidence-traceable efficiency report for a completed local run."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import resource
import time
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex


def code_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted((root / "src").rglob("*.py")):
        digest.update(str(path.relative_to(root)).encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, default=Path(__file__).parents[1])
    parser.add_argument("--index", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--build-seconds", type=float, required=True)
    parser.add_argument("--build-maxrss-kb", type=int, required=True)
    parser.add_argument("--pilot", type=Path)
    parser.add_argument("--batch-pilot", type=Path)
    parser.add_argument("--batch-resume-pilot", type=Path)
    parser.add_argument("--lookup", type=Path)
    parser.add_argument("--candidate-pilot", type=Path)
    parser.add_argument("--candidate-pilot-cache-hit", type=Path)
    parser.add_argument("--auxiliary-pilot", type=Path)
    parser.add_argument("--auxiliary-evidence-pilot", type=Path)
    parser.add_argument("--auxiliary-full-source", type=Path)
    parser.add_argument("--auxiliary-full-evidence-pilot", type=Path)
    parser.add_argument("--auxiliary-2001-source", type=Path)
    parser.add_argument("--isc-reference-sample", type=Path)
    parser.add_argument("--isc-evidence-pilot", type=Path)
    parser.add_argument("--arquivo-cdx-probe", type=Path)
    parser.add_argument("--arquivo-cdxj-probe", type=Path)
    parser.add_argument("--arquivo-cdxj-catalog-probe", type=Path)
    parser.add_argument("--arquivo-cdxj-evidence-pilot", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    started = time.perf_counter()
    index = BaselineIndex(args.index)
    counts = index.counts()
    overlap = index.connection.execute(
        "SELECT COUNT(*) FROM candidate_hostnames c "
        "JOIN annual_hostnames a ON c.hostname = a.hostname"
    ).fetchone()[0]
    index.close()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    report = {
        "report_version": "efficiency-v1",
        "created_unix": time.time(),
        "code_sha256": code_digest(args.source_root),
        "machine": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "processor": platform.processor(),
        },
        "baseline_id": manifest["baseline_id"],
        "baseline_manifest_sha256": hashlib.sha256(args.manifest.read_bytes()).hexdigest(),
        "full_baseline_build": {
            "elapsed_seconds": args.build_seconds,
            "maxrss_kb": args.build_maxrss_kb,
            "annual_hostnames": counts["annual_hostnames"],
            "candidate_hostnames": counts["candidate_hostnames"],
            "candidate_annual_overlap": overlap,
            "active_candidate_estimate": counts["candidate_hostnames"] - overlap,
        },
        "report_generation_seconds": round(time.perf_counter() - started, 6),
    }
    if args.pilot is not None:
        report["evidence_pilot"] = json.loads(args.pilot.read_text(encoding="utf-8"))
    if args.batch_pilot is not None:
        report["batch_evidence_pilot"] = json.loads(
            args.batch_pilot.read_text(encoding="utf-8")
        )
    if args.batch_resume_pilot is not None:
        report["batch_evidence_resume_pilot"] = json.loads(
            args.batch_resume_pilot.read_text(encoding="utf-8")
        )
    if args.lookup is not None:
        report["lookup_benchmark"] = json.loads(args.lookup.read_text(encoding="utf-8"))
    if args.candidate_pilot is not None:
        report["candidate_pilot"] = json.loads(args.candidate_pilot.read_text(encoding="utf-8"))
    if args.candidate_pilot_cache_hit is not None:
        report["candidate_pilot_cache_hit"] = json.loads(
            args.candidate_pilot_cache_hit.read_text(encoding="utf-8")
        )
    if args.auxiliary_pilot is not None:
        report["auxiliary_pilot"] = json.loads(
            args.auxiliary_pilot.read_text(encoding="utf-8")
        )
    if args.auxiliary_evidence_pilot is not None:
        report["auxiliary_evidence_pilot"] = json.loads(
            args.auxiliary_evidence_pilot.read_text(encoding="utf-8")
        )
    if args.auxiliary_full_source is not None:
        report["auxiliary_full_source_sample"] = json.loads(
            args.auxiliary_full_source.read_text(encoding="utf-8")
        )
    if args.auxiliary_full_evidence_pilot is not None:
        report["auxiliary_full_source_evidence_pilot"] = json.loads(
            args.auxiliary_full_evidence_pilot.read_text(encoding="utf-8")
        )
    if args.auxiliary_2001_source is not None:
        report["auxiliary_2001_source_sample"] = json.loads(
            args.auxiliary_2001_source.read_text(encoding="utf-8")
        )
    if args.isc_reference_sample is not None:
        report["isc_reference_sample"] = json.loads(
            args.isc_reference_sample.read_text(encoding="utf-8")
        )
    if args.isc_evidence_pilot is not None:
        report["isc_evidence_pilot"] = json.loads(
            args.isc_evidence_pilot.read_text(encoding="utf-8")
        )
    if args.arquivo_cdx_probe is not None:
        report["arquivo_cdx_probe"] = json.loads(
            args.arquivo_cdx_probe.read_text(encoding="utf-8")
        )
    if args.arquivo_cdxj_probe is not None:
        report["arquivo_cdxj_probe"] = json.loads(
            args.arquivo_cdxj_probe.read_text(encoding="utf-8")
        )
    if args.arquivo_cdxj_catalog_probe is not None:
        report["arquivo_cdxj_catalog_probe"] = json.loads(
            args.arquivo_cdxj_catalog_probe.read_text(encoding="utf-8")
        )
    if args.arquivo_cdxj_evidence_pilot is not None:
        report["arquivo_cdxj_evidence_pilot"] = json.loads(
            args.arquivo_cdxj_evidence_pilot.read_text(encoding="utf-8")
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
