"""Cheap operational triage for newly discovered source entrypoints.

Triage is not evidence validation and does not estimate novel EED. It only asks
whether the current source entrypoint is cheaply reachable enough to justify a
bounded scout. Historical value is therefore never rejected from agent priors.
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx

from creeper.source_discovery.coordinator import TriageDisposition, TriageResult
from creeper.source_discovery.models import SourceCandidate


class TriageTransientError(RuntimeError):
    """Operational failure that should be retried after coordinator backoff."""


@dataclass(frozen=True)
class HttpTriagePolicy:
    timeout_seconds: float = 10.0
    fallback_get_statuses: frozenset[int] = frozenset({400, 403, 405, 501})
    transient_statuses: frozenset[int] = frozenset({408, 425, 429})

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        for status in self.fallback_get_statuses | self.transient_statuses:
            if not 100 <= status <= 599:
                raise ValueError("HTTP triage status codes must be valid")


class HttpSourceTriageExecutor:
    """Probe an entrypoint with HEAD and a zero-body streaming GET fallback.

    Some archives and object stores reject HEAD while serving GET normally. For
    those statuses a Range GET is opened only far enough to observe response
    headers; the body is not consumed. 429/5xx/network failures raise so the
    coordinator's TTL suppression provides retry/backoff instead of parking a
    source permanently.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        policy: HttpTriagePolicy | None = None,
    ) -> None:
        self.client = client
        self.policy = policy or HttpTriagePolicy()

    @staticmethod
    def _result_for_status(status: int, *, method: str) -> TriageResult:
        if 200 <= status < 400:
            return TriageResult(
                TriageDisposition.SCOUT,
                reason=f"{method} entrypoint probe returned HTTP {status}",
            )
        return TriageResult(
            TriageDisposition.HOLD,
            reason=f"{method} entrypoint probe returned HTTP {status}",
        )

    def _raise_if_transient(self, status: int, *, method: str) -> None:
        if status >= 500 or status in self.policy.transient_statuses:
            raise TriageTransientError(
                f"{method} entrypoint probe returned transient HTTP {status}"
            )

    async def _range_get_status(self, url: str) -> int:
        async with self.client.stream(
            "GET",
            url,
            headers={"Range": "bytes=0-0"},
            follow_redirects=True,
            timeout=self.policy.timeout_seconds,
        ) as response:
            # Deliberately do not consume response content. Closing the stream
            # bounds local memory even when the origin ignores the Range header.
            return int(response.status_code)

    async def __call__(self, candidate: SourceCandidate) -> TriageResult:
        url = candidate.canonical_entrypoint
        try:
            response = await self.client.head(
                url,
                follow_redirects=True,
                timeout=self.policy.timeout_seconds,
            )
            status = int(response.status_code)
            if status in self.policy.fallback_get_statuses:
                status = await self._range_get_status(url)
                self._raise_if_transient(status, method="GET")
                return self._result_for_status(status, method="GET")
            self._raise_if_transient(status, method="HEAD")
            return self._result_for_status(status, method="HEAD")
        except TriageTransientError:
            raise
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
            raise TriageTransientError(
                f"entrypoint probe failed transiently: {type(exc).__name__}: {exc}"
            ) from exc
