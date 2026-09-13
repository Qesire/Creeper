from __future__ import annotations

import json
import unittest

from creeper.source_discovery.research_models import (
    ExplorationRegion,
    RegionState,
    legal_region_transition,
)


def make_region(region_id: str = "r1", **overrides) -> ExplorationRegion:
    values = {
        "region_id": region_id,
        "surface_kind": "STATIC_LIST",
        "root": "https://example.test/catalog",
        "purpose": "historical index enumeration",
        "query_family_json": json.dumps(
            {
                "template": "https://example.test/{year}/{part}.cdxj",
                "dimensions": {"year": [1996, 1997], "part": [1, 2]},
            }
        ),
        "enumerator_spec_json": json.dumps(
            {"kind": "STATIC_LIST", "config": {"urls": []}}
        ),
        "artifact_predicate_json": json.dumps({"suffix": [".cdxj"]}),
        "hard_bounds_json": json.dumps(
            {
                "max_queries": 100,
                "max_pages": 100,
                "max_artifacts": 1000,
                "max_requests": 100,
                "max_bytes": 10_000_000,
                "max_wall_seconds": 60,
            }
        ),
        "stop_conditions_json": json.dumps(["QUERY_FAMILY_EXHAUSTED"]),
        "expected_source_family": "historical-index",
        "expected_contract_family": "CDXJ",
        "context_hash": "ctx-v1",
    }
    values.update(overrides)
    return ExplorationRegion(**values)


class RegionModelTests(unittest.TestCase):
    def test_region_key_is_semantic_not_proposal_identity(self) -> None:
        first = make_region("proposal-a")
        second = make_region("proposal-b")
        self.assertEqual(first.region_key, second.region_key)
        self.assertTrue(first.region_key.startswith("region:"))

    def test_compiler_version_changes_semantic_identity(self) -> None:
        first = make_region(compiler_version="compiler-a")
        second = make_region(compiler_version="compiler-b")
        self.assertNotEqual(first.region_key, second.region_key)

    def test_region_transition_matrix(self) -> None:
        self.assertTrue(
            legal_region_transition(RegionState.READY, RegionState.RUNNING)
        )
        self.assertTrue(
            legal_region_transition(
                RegionState.RUNNING, RegionState.FAILED_RETRYABLE
            )
        )
        self.assertFalse(
            legal_region_transition(RegionState.EXHAUSTED, RegionState.READY)
        )

    def test_invalid_json_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "valid JSON"):
            make_region(query_family_json="{not-json")


if __name__ == "__main__":
    unittest.main()
