#!/usr/bin/env python3
"""Build a hash and line-count manifest for the V3 authority snapshot."""

from __future__ import annotations

import argparse
from pathlib import Path

from creeper.authority.manifest import build_manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("task_root", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--source-archive", type=Path)
    args = parser.parse_args()
    manifest = build_manifest(
        args.task_root, args.output, source_archive_path=args.source_archive
    )
    print(f"baseline_id={manifest['baseline_id']}")
    print(f"annual_records={sum(manifest['annual_line_counts'].values()):,}")
    print(f"candidate_records={manifest['candidate_line_count']:,}")
    print(f"unparsed_records={manifest['unparsed_line_count']:,}")
    print(f"manifest={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
