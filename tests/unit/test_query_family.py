from __future__ import annotations

import unittest

from creeper.source_discovery.query_family import (
    QueryFamily,
    QueryFamilyExpansionError,
)
from creeper.source_discovery.region_compilation import (
    CompiledScoutPlan,
    EnumeratorSpec,
    ExecutionBounds,
)


class QueryFamilyTests(unittest.TestCase):
    def test_72_query_cartesian_family_expands_stably(self) -> None:
        family = QueryFamily(
            template="https://data.example/{year}/part-{part}.cdxj",
            dimensions={
                "year": tuple(range(1996, 2002)),
                "part": tuple(range(1, 13)),
            },
        )
        expanded = family.expand(max_queries=72)
        self.assertEqual(family.cardinality, 72)
        self.assertEqual(len(expanded), 72)
        self.assertEqual(
            expanded[0], "https://data.example/1996/part-1.cdxj"
        )
        self.assertEqual(
            expanded[-1], "https://data.example/2001/part-12.cdxj"
        )

    def test_overbound_family_rejects_before_execution(self) -> None:
        family = QueryFamily(
            template="https://data.example/{part}.cdxj",
            dimensions={"part": tuple(range(73))},
        )
        with self.assertRaises(QueryFamilyExpansionError):
            family.expand(max_queries=72)

    def test_unsized_dimension_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            QueryFamily(
                template="{x}",
                dimensions={"x": iter(range(10))},
            )

    def test_static_cardinality_is_validated_before_io(self) -> None:
        with self.assertRaisesRegex(ValueError, "max_artifacts"):
            CompiledScoutPlan(
                region_id="r",
                region_key="region:key",
                source_family="synthetic",
                surface_kind="STATIC_LIST",
                root="https://data.example/",
                query_family=None,
                enumerator=EnumeratorSpec(
                    "STATIC_LIST",
                    {
                        "urls": tuple(
                            f"https://data.example/{index}.cdxj"
                            for index in range(3)
                        )
                    },
                ),
                artifact_predicate=None,
                hard_bounds=ExecutionBounds(
                    max_queries=10,
                    max_pages=10,
                    max_artifacts=2,
                    max_requests=10,
                    max_bytes=1_000,
                    max_wall_seconds=10,
                ),
                stop_conditions=("EXHAUSTED",),
            )

    def test_html_catalog_requires_explicit_origin_policy(self) -> None:
        with self.assertRaisesRegex(ValueError, "origin_policy"):
            EnumeratorSpec(
                "HTML_CATALOG",
                {"root": "https://example.test/catalog"},
            )


if __name__ == "__main__":
    unittest.main()
