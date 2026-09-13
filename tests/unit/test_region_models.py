from __future__ import annotations

import json
import unittest

from creeper.source_discovery.research_models import ExplorationRegion, RegionState


def make_region(**overrides):
    values = dict(
        region_id="r1",
        surface_kind="STATIC_LIST",
        root="https://example.test/catalog",
        purpose="historical catalog",
        query_family_json=json.dumps({"template": "x", "dimensions": {"year": [1996, 1997]}}),
        enumerator_spec_json=json.dumps({"kind": "STATIC_LIST"}),
        artifact_predicate_json=json.dumps({"suffix": [".cdx"]}),
        hard_bounds_json=json.dumps({"max_requests": 10}),
        stop_conditions_json=json.dumps({"on": "EOF"}),
        expected_source_family="catalog",
        expected_contract_family="cdx",
        context_hash="ctx",
    )
    values.update(overrides)
    return ExplorationRegion(**values)


class RegionModelTests(unittest.TestCase):
    def test_semantic_identity_dedupes_and_policy_identity_changes_key(self):
        first = make_region()
        same = make_region(region_id="r2")
        changed = make_region(compiler_version="v7.1")
        self.assertEqual(first.region_key, same.region_key)
        self.assertNotEqual(first.region_key, changed.region_key)

    def test_state_transition_contract(self):
        self.assertTrue(RegionState.READY.value == "READY")
