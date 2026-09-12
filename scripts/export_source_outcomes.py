#!/usr/bin/env python3
"""Export Creeper source outcomes for offline replay/policy fitting."""

from __future__ import annotations

import argparse
from pathlib import Path

from creeper.source_discovery.outcomes import encode_jsonl, iter_source_outcomes
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.storage.control_store import ControlStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("runtime_data_root", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args(argv)

    control = ControlStore(args.runtime_data_root / "control.sqlite3")
    try:
        registry = SourceDiscoveryRegistry(control)
        payload = encode_jsonl(iter_source_outcomes(registry))
    finally:
        control.close()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(payload, encoding="utf-8")
    temporary.replace(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
