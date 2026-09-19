"""HMAC authentication for Fabric v2 worker requests."""

from __future__ import annotations

import hashlib
import hmac
import time
from collections.abc import Mapping
from typing import Protocol


class AuthenticationError(RuntimeError):
    pass


class NonceStore(Protocol):
    def consume_request_nonce(
        self,
        worker_id: str,
        nonce: str,
        *,
        retention_seconds: float = 900.0,
    ) -> bool: ...


def body_digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def canonical_request(
    *,
    method: str,
    path: str,
    body: bytes,
    timestamp: str,
    nonce: str,
) -> bytes:
    return "\n".join(
        (
            method.upper(),
            path,
            body_digest(body),
            timestamp,
            nonce,
        )
    ).encode("utf-8")


def sign_request(
    secret: str | bytes,
    *,
    method: str,
    path: str,
    body: bytes,
    timestamp: str,
    nonce: str,
) -> str:
    key = secret.encode("utf-8") if isinstance(secret, str) else secret
    if not key:
        raise ValueError("worker HMAC secret must be non-empty")
    return hmac.new(
        key,
        canonical_request(
            method=method,
            path=path,
            body=body,
            timestamp=timestamp,
            nonce=nonce,
        ),
        hashlib.sha256,
    ).hexdigest()


class HMACRequestAuthenticator:
    """Verify worker identity and durably reject request replay."""

    def __init__(
        self,
        store: NonceStore,
        credentials: Mapping[str, str | bytes],
        *,
        max_clock_skew_seconds: float = 300.0,
        nonce_retention_seconds: float = 900.0,
        clock=time.time,
    ) -> None:
        if max_clock_skew_seconds <= 0 or nonce_retention_seconds <= 0:
            raise ValueError("authentication time windows must be positive")
        self.store = store
        self.credentials = dict(credentials)
        self.max_clock_skew_seconds = float(max_clock_skew_seconds)
        self.nonce_retention_seconds = max(
            float(nonce_retention_seconds),
            2.0 * float(max_clock_skew_seconds),
        )
        self.clock = clock

    def verify(
        self,
        *,
        worker_id: str,
        method: str,
        path: str,
        body: bytes,
        timestamp: str,
        nonce: str,
        signature: str,
    ) -> str:
        secret = self.credentials.get(worker_id)
        if secret is None:
            raise AuthenticationError("unknown worker credential")
        if not worker_id.strip() or not nonce.strip():
            raise AuthenticationError("missing worker identity or nonce")
        if not timestamp.strip() or not signature.strip():
            raise AuthenticationError("missing signed request metadata")
        try:
            request_time = float(timestamp)
        except ValueError as exc:
            raise AuthenticationError("invalid request timestamp") from exc
        now = float(self.clock())
        if abs(now - request_time) > self.max_clock_skew_seconds:
            raise AuthenticationError("expired request timestamp")
        expected = sign_request(
            secret,
            method=method,
            path=path,
            body=body,
            timestamp=timestamp,
            nonce=nonce,
        )
        if not hmac.compare_digest(expected, signature.lower()):
            raise AuthenticationError("invalid request signature")
        if not self.store.consume_request_nonce(
            worker_id,
            nonce,
            retention_seconds=self.nonce_retention_seconds,
        ):
            raise AuthenticationError("request nonce replay")
        return worker_id
