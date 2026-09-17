#!/usr/bin/env python3
"""Emit a read-only calibration report for deterministic residual search."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from creeper.source_discovery.residual_report import load_residual_search_report


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Read an initialized Creeper control SQLite database without "
            "mutation and report residual-search coverage/economics."
        )
    )
    parser.add_argument("control_db", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    report = load_residual_search_report(args.control_db)
    text = json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    if args.output is None:
        print(text, end="")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
        print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
