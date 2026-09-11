"""CLI for bounded and soak runtime validation windows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from creeper.metrics.validation import (
    capture_runtime_snapshot,
    finish_validation_run,
    start_validation_run,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="creeper-validation")
    sub = parser.add_subparsers(dest="command", required=True)

    start = sub.add_parser("start")
    start.add_argument("runtime_data_root", type=Path)
    start.add_argument("run_dir", type=Path)
    start.add_argument("--label", required=True)
    start.add_argument("--target-source-records", type=int)
    start.add_argument("--code-revision")

    finish = sub.add_parser("finish")
    finish.add_argument("runtime_data_root", type=Path)
    finish.add_argument("run_dir", type=Path)
    finish.add_argument("--code-revision")

    snapshot = sub.add_parser("snapshot")
    snapshot.add_argument("runtime_data_root", type=Path)

    args = parser.parse_args(argv)
    try:
        if args.command == "start":
            payload = start_validation_run(
                runtime_data_root=args.runtime_data_root,
                run_dir=args.run_dir,
                label=args.label,
                target_source_records=args.target_source_records,
                code_revision=args.code_revision,
            )
        elif args.command == "finish":
            payload = finish_validation_run(
                runtime_data_root=args.runtime_data_root,
                run_dir=args.run_dir,
                code_revision=args.code_revision,
            )
        else:
            payload = capture_runtime_snapshot(args.runtime_data_root)
    except (OSError, ValueError, KeyError) as exc:
        parser.error(str(exc))
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
