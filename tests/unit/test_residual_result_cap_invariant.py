from __future__ import annotations

import unittest

from creeper.source_discovery.deterministic_search import (
    DeterministicSearchExecutor,
    DeterministicSearchPolicy,
)


class ResidualResultCapInvariantTests(unittest.TestCase):
    class Provider:
        def __init__(self, name: str) -> None:
            self.name = name

        async def search(self, plan, *, limit):
            return ()

    def test_global_cap_cannot_starve_a_configured_provider(self) -> None:
        providers = tuple(self.Provider(f"p{index}") for index in range(4))
        with self.assertRaisesRegex(
            ValueError,
            "max_total_results must be at least the number of configured",
        ):
            DeterministicSearchExecutor(
                providers,
                policy=DeterministicSearchPolicy(
                    results_per_provider=100,
                    max_total_results=3,
                ),
            )

    def test_global_cap_equal_to_provider_count_is_valid(self) -> None:
        providers = tuple(self.Provider(f"p{index}") for index in range(4))
        executor = DeterministicSearchExecutor(
            providers,
            policy=DeterministicSearchPolicy(
                results_per_provider=100,
                max_total_results=4,
            ),
        )
        self.assertEqual(len(executor.providers), 4)
        self.assertEqual(executor.policy.max_total_results, 4)


if __name__ == "__main__":
    unittest.main()
