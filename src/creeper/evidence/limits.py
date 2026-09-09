"""Small dependency-free request limiting primitives."""

from __future__ import annotations

import time
from collections.abc import Callable


class RequestRateLimiter:
    """Enforce a minimum interval between requests in one worker."""

    def __init__(
        self,
        requests_per_second: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if requests_per_second < 0:
            raise ValueError("requests_per_second cannot be negative")
        self.interval = 1.0 / requests_per_second if requests_per_second else 0.0
        self.clock = clock
        self.sleep = sleep
        self._next_allowed: float | None = None

    def acquire(self) -> None:
        if self.interval == 0.0:
            return
        now = self.clock()
        if self._next_allowed is None:
            self._next_allowed = now + self.interval
            return
        scheduled = max(self._next_allowed, now)
        delay = scheduled - now
        if delay > 0:
            self.sleep(delay)
        self._next_allowed = scheduled + self.interval
