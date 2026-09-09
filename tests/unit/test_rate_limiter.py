import unittest

from creeper.evidence.limits import RequestRateLimiter


class RateLimiterTests(unittest.TestCase):
    def test_enforces_minimum_interval_with_injected_clock(self):
        now = [0.0]
        sleeps: list[float] = []

        def clock() -> float:
            return now[0]

        def sleep(seconds: float) -> None:
            sleeps.append(seconds)
            now[0] += seconds

        limiter = RequestRateLimiter(2.0, clock=clock, sleep=sleep)
        limiter.acquire()
        limiter.acquire()
        limiter.acquire()

        self.assertEqual(sleeps, [0.5, 0.5])

    def test_zero_rate_does_not_sleep(self):
        sleeps: list[float] = []
        limiter = RequestRateLimiter(0.0, clock=lambda: 10.0, sleep=sleeps.append)
        limiter.acquire()
        self.assertEqual(sleeps, [])


if __name__ == "__main__":
    unittest.main()
