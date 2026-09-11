import tempfile
import unittest
from pathlib import Path

from creeper.runtime.resource_governor import (
    GovernorState,
    LocalResourceSampler,
    ResourceGovernor,
    ResourceSample,
    StabilizedResourceGovernor,
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

    def test_disk_throttle_enters_drain_only_before_hard_stop(self):
        governor = ResourceGovernor(
            rss_throttle_bytes=100,
            rss_stop_bytes=200,
            disk_throttle_bytes=100,
            disk_stop_bytes=50,
        )

        self.assertEqual(
            governor.evaluate(ResourceSample(rss_bytes=10, disk_free_bytes=90)),
            GovernorState.DRAIN_ONLY,
        )
        self.assertEqual(
            governor.evaluate(ResourceSample(rss_bytes=10, disk_free_bytes=40)),
            GovernorState.EMERGENCY_STOP,
        )

    def test_stabilized_governor_escalates_immediately_and_recovers_after_streak(self):
        governor = StabilizedResourceGovernor(
            ResourceGovernor(
                rss_throttle_bytes=100,
                rss_stop_bytes=200,
                disk_throttle_bytes=100,
                disk_stop_bytes=50,
            ),
            recovery_samples=3,
        )
        high = ResourceSample(rss_bytes=150, disk_free_bytes=500)
        healthy = ResourceSample(rss_bytes=10, disk_free_bytes=500)

        self.assertEqual(governor.update(high), GovernorState.THROTTLED)
        self.assertEqual(governor.update(healthy), GovernorState.THROTTLED)
        self.assertEqual(governor.update(healthy), GovernorState.THROTTLED)
        self.assertEqual(governor.update(healthy), GovernorState.NORMAL)

    def test_local_sampler_sums_root_and_descendant_rss(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proc = root / "proc"
            proc.mkdir()
            disk = root / "runtime"
            disk.mkdir()

            fixtures = {
                100: (1, 10),
                101: (100, 20),
                102: (101, 30),
                200: (1, 40),
            }
            for pid, (ppid, rss_kib) in fixtures.items():
                directory = proc / str(pid)
                directory.mkdir()
                (directory / "status").write_text(
                    f"Name:\tfixture\nPPid:\t{ppid}\nVmRSS:\t{rss_kib} kB\n",
                    encoding="utf-8",
                )

            sampler = LocalResourceSampler(disk, proc_root=proc)

            self.assertEqual(
                sampler.process_tree_rss([100]),
                (10 + 20 + 30) * 1024,
            )
            self.assertEqual(
                sampler.process_tree_rss([100, 200]),
                (10 + 20 + 30 + 40) * 1024,
            )
            sample = sampler.sample([100])
            self.assertEqual(sample.rss_bytes, (10 + 20 + 30) * 1024)
            self.assertGreater(sample.disk_free_bytes, 0)

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
