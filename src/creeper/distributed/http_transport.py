"""HTTPX transport fencing every distributed provider request by Authority."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import httpx

from creeper.runtime.http import configured_http_proxy


PermitAcquire = Callable[[], Awaitable[object]]
PermitReport = Callable[
    [object, int | None, httpx.Headers | None, int],
    Awaitable[None],
]


class _PermitStream(httpx.AsyncByteStream):
    """Hold global inflight ownership until a response body is consumed/closed."""

    def __init__(
        self,
        inner: httpx.AsyncByteStream,
        release: Callable[[int], Awaitable[None]],
    ) -> None:
        self.inner = inner
        self.release = release
        self.response_bytes = 0
        self._released = False
        self._release_lock = asyncio.Lock()

    async def _release_once(self) -> None:
        async with self._release_lock:
            if self._released:
                return
            self._released = True
            await self.release(self.response_bytes)

    async def __aiter__(self):
        try:
            async for chunk in self.inner:
                self.response_bytes += len(chunk)
                yield chunk
        except BaseException:
            await self._release_once()
            raise
        else:
            await self._release_once()

    async def aclose(self) -> None:
        try:
            await self.inner.aclose()
        finally:
            await self._release_once()


class AuthorityPermitTransport(httpx.AsyncBaseTransport):
    """Wrap one transport so every actual HTTP attempt consumes one permit.

    This lives entirely inside the distributed derivative. Core CDX clients
    continue using ordinary HTTPX semantics and do not know about Authority,
    leases, workers, or global provider budgets.
    """

    def __init__(
        self,
        inner: httpx.AsyncBaseTransport,
        *,
        acquire: PermitAcquire,
        report: PermitReport,
    ) -> None:
        self.inner = inner
        self.acquire = acquire
        self.report = report

    async def handle_async_request(
        self,
        request: httpx.Request,
    ) -> httpx.Response:
        permit = await self.acquire()
        response: httpx.Response | None = None

        async def release(response_bytes: int) -> None:
            await self.report(
                permit,
                None if response is None else int(response.status_code),
                None if response is None else response.headers,
                int(response_bytes),
            )

        try:
            response = await self.inner.handle_async_request(request)
        except BaseException:
            await release(0)
            raise

        return httpx.Response(
            status_code=response.status_code,
            headers=response.headers,
            stream=_PermitStream(response.stream, release),
            extensions=response.extensions,
            request=request,
        )

    async def aclose(self) -> None:
        await self.inner.aclose()


def build_authority_transport(
    *,
    acquire: PermitAcquire,
    report: PermitReport,
    max_connections: int,
    max_keepalive_connections: int,
    keepalive_expiry_seconds: float,
    inner: httpx.AsyncBaseTransport | None = None,
) -> AuthorityPermitTransport:
    """Build a derivative transport while preserving core HTTP pooling."""

    if inner is None:
        inner = httpx.AsyncHTTPTransport(
            proxy=configured_http_proxy(),
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_keepalive_connections,
                keepalive_expiry=keepalive_expiry_seconds,
            ),
        )
    return AuthorityPermitTransport(
        inner,
        acquire=acquire,
        report=report,
    )
