#!/usr/bin/env python3
"""Report the learned provider evidence-action value model."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from creeper.evidence.actions import (
    ACTION_PRIOR_STRENGTH,
    EvidenceActionKind,
    action_prior_yield,
)
from creeper.storage.control_store import ControlStore


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-db", type=Path, required=True)
    args = parser.parse_args()

    store = ControlStore(args.control_db)
    try:
        stats = store.evidence_action_value_summary()
        payload = {
            "prior_strength_requests": ACTION_PRIOR_STRENGTH,
            "actions": {
                kind.value: {
                    "bootstrap_host_years_per_request": action_prior_yield(kind),
                    "attempts": stats[kind.value].attempts,
                    "provider_requests": stats[kind.value].provider_requests,
                    "provider_elapsed_milliseconds": (
                        stats[kind.value].provider_elapsed_milliseconds
                    ),
                    "final_novel_host_years": (
                        stats[kind.value].final_novel_host_years
                    ),
                    "final_novel_eed": stats[kind.value].final_novel_eed,
                    "posterior_host_years_per_request": (
                        stats[kind.value].posterior_host_years_per_request
                    ),
                    "final_eed_per_request": (
                        stats[kind.value].final_eed_per_request
                    ),
                }
                for kind in EvidenceActionKind
            },
        }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 0
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
