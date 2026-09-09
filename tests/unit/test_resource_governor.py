import unittest

from creeper.runtime.resource_governor import (
    GovernorState,
    ResourceGovernor,
    ResourceSample,
)


class ResourceGovernorTests(unittest.TestCase):
    def test_normal_and_throttled_states(self):
        governor = ResourceGovernor(
            rss_throttle_bytes=100,
            rss_stop_bytes=200,
            disk_throttle_bytes=100,
            disk_stop_bytes=50,
        )
        self.assertEqual(
            governor.evaluate(ResourceSample(rss_bytes=10, disk_free_bytes=500)),
            GovernorState.NORMAL,
        )
        self.assertEqual(
            governor.evaluate(ResourceSample(rss_bytes=150, disk_free_bytes=500)),
            GovernorState.THROTTLED,
        )

    def test_emergency_stop_wins_over_throttle(self):
        governor = ResourceGovernor(
            rss_throttle_bytes=100,
            rss_stop_bytes=200,
            disk_throttle_bytes=100,
            disk_stop_bytes=50,
        )
        self.assertEqual(
            governor.evaluate(ResourceSample(rss_bytes=10, disk_free_bytes=20)),
            GovernorState.EMERGENCY_STOP,
        )


if __name__ == "__main__":
    unittest.main()
