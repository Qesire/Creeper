#!/usr/bin/env python3
"""Verify a submission archive against explicit baseline/model authority."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from creeper.submission.verify import verify_submission_archive


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    parser.add_argument("--baseline-manifest", type=Path, required=True)
    parser.add_argument("--baseline-index", type=Path, required=True)
    parser.add_argument("--eed-model", type=Path, required=True)
    args = parser.parse_args()
    report = verify_submission_archive(
        args.archive,
        baseline_manifest_path=args.baseline_manifest,
        baseline_index_path=args.baseline_index,
        eed_model_path=args.eed_model,
    )
    print(json.dumps({
        "ready": report.ready,
        "errors": list(report.errors),
        "annual_records": report.annual_records,
        "evidence_records": report.evidence_records,
        "active_candidates": report.active_candidates,
        "recomputed_novel_eed": report.recomputed_novel_eed,
        "recomputed_growth_rate": report.recomputed_growth_rate,
    }, ensure_ascii=False, indent=2))
    return 0 if report.ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
