#!/usr/bin/env python3
"""Exercise candidate -> evidence -> submission using an offline fixture transport.

This is a wiring test only. Its generated evidence is synthetic and must never
be presented as organizer-verified competition evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.providers.cdx import query_year
from creeper.records.candidates import CandidateRecord, CandidateSourceScope, reconcile_active_candidates
from creeper.submission.builder import build_snapshot
from creeper.submission.exporter import build_submission_zip


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("task_root", type=Path)
    parser.add_argument("index", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--documentation", type=Path, required=True)
    args = parser.parse_args()
    source_path = args.task_root / "merged260909-3" / "candidate_pool.txt"
    records = []
    with source_path.open("r", encoding="utf-8", errors="replace") as source:
        for line in source:
            records.append(CandidateRecord(line, "official_pool", CandidateSourceScope.OFFICIAL_POOL))
            if len(records) >= args.limit:
                break
    index = BaselineIndex(args.index)
    candidates = reconcile_active_candidates(records, index)
    target_year = 2001
    capsules = []
    for candidate in candidates.active:
        result = query_year(
            candidate.hostname,
            target_year,
            lambda hostname, year: [(
                [{
                    "timestamp": f"{year}0101000000",
                    "original": f"http://{hostname}/fixture",
                    "status": "200",
                }],
                True,
            )],
        )
        if result.capsule:
            capsules.append(result.capsule)
    manifest_path = args.index.parents[1] / "authority" / "baseline_manifest.json"
    baseline_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    code_digest = hashlib.sha256(Path(__file__).parents[1].joinpath("src/creeper/cli.py").read_bytes()).hexdigest()
    snapshot = build_snapshot(
        "offline-dry-run",
        capsules,
        index,
        baseline_manifest,
        code_revision=code_digest,
        source_report_set=("offline-source-report.json",),
        cdx_audit_set=("offline-cdx-audit.json",),
        eed_report={"equivalent_english_domains": "0.0000"},
        novel_eed="0.0000",
        growth_rate="0.000000",
    )
    archive = build_submission_zip(
        snapshot,
        "offline-dry-run",
        args.output_dir,
        source_root=Path(__file__).parents[1],
        documentation_path=args.documentation,
    )
    report = {
        "synthetic": True,
        "candidate_sample": len(records),
        "active_candidates": len(candidates.active),
        "pass_evidence": len(capsules),
        "precheck_ready": snapshot.ready,
        "target_year": target_year,
        "archive": str(archive),
        "warning": "Synthetic evidence; not suitable for official submission.",
    }
    report_path = args.output_dir / "offline_dry_run.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    index.close()
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
