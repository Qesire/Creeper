from __future__ import annotations

import unittest

from creeper.source_discovery.query_family import QueryFamily, QueryFamilyExpansionError


class QueryFamilyTests(unittest.TestCase):
    def test_cartesian_product_expands_in_stable_dimension_order(self) -> None:
        family = QueryFamily(
            template="{concept} {year} {structure}",
            dimensions={
                "concept": ("web graph", "link graph"),
                "year": (1996, 1997, 1998, 1999, 2000, 2001),
                "structure": ("dataset", "archive"),
            },
        )
        self.assertEqual(family.cardinality, 24)
        self.assertEqual(
            family.expand()[0],
            "web graph 1996 dataset",
        )
        self.assertEqual(
            family.expand()[-1],
            "link graph 2001 archive",
        )

    def test_cardinality_over_bound_is_rejected_before_expansion(self) -> None:
        family = QueryFamily(
            template="{x}",
            dimensions={"x": tuple(range(73))},
        )
        with self.assertRaises(QueryFamilyExpansionError):
            family.expand(max_queries=72)

    def test_empty_dimension_is_invalid_and_infinite_dimension_is_impossible(self) -> None:
        with self.assertRaises(ValueError):
            QueryFamily(template="{x}", dimensions={"x": ()})
        with self.assertRaises(ValueError):
            QueryFamily(template="{x}", dimensions={"x": iter(int, 1)})


if __name__ == "__main__":
    unittest.main()
