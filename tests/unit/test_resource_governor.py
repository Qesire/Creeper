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

    def test_drain_only_credits_stop_source_fetch_but_keep_draining_stages(self):
        governor = ResourceGovernor(
            rss_throttle_bytes=100,
            rss_stop_bytes=200,
            disk_throttle_bytes=100,
            disk_stop_bytes=50,
        )

        credits = governor.credits(
            ResourceSample(rss_bytes=10, disk_free_bytes=500, provider_pressure=1.0),
            {
                "source_fetch": 4,
                "parse": 2,
                "commit": 1,
                "wayback": 5,
            },
        )

        self.assertEqual(credits.source_fetch, 0)
        self.assertGreater(credits.parse, 0)
        self.assertGreater(credits.commit, 0)
        self.assertEqual(credits.evidence["wayback"], 0)

    def test_normal_credits_reflect_configured_capacities(self):
        governor = ResourceGovernor(
            rss_throttle_bytes=100,
            rss_stop_bytes=200,
            disk_throttle_bytes=100,
            disk_stop_bytes=50,
        )

        credits = governor.credits(
            ResourceSample(rss_bytes=10, disk_free_bytes=500),
            {
                "source_fetch": 4,
                "parse": 2,
                "commit": 1,
                "wayback": 5,
            },
        )

        self.assertEqual(credits.source_fetch, 4)
        self.assertEqual(credits.parse, 2)
        self.assertEqual(credits.commit, 1)
        self.assertEqual(credits.evidence, {"wayback": 5})


if __name__ == "__main__":
    unittest.main()
