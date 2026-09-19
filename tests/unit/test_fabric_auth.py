from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.distributed.auth import (
    AuthenticationError,
    HMACRequestAuthenticator,
    sign_request,
)
from creeper.distributed.authority_store import DistributedAuthorityStore


class FabricAuthenticationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = DistributedAuthorityStore(
            Path(self.tmp.name) / "fabric.sqlite3",
            clock=lambda: 1_000.0,
        )
        self.auth = HMACRequestAuthenticator(
            self.store,
            {"worker-a": "secret"},
            max_clock_skew_seconds=30.0,
            nonce_retention_seconds=120.0,
            clock=lambda: 1_000.0,
        )

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    @staticmethod
    def _signature(
        *,
        body: bytes = b'{"x":1}',
        timestamp: str = "1000",
        nonce: str = "nonce-a",
        instance: str = "instance-1",
        path: str = "/v2/tasks/claim",
    ) -> str:
        return sign_request(
            "secret",
            method="POST",
            path=path,
            body=body,
            worker_instance_id=instance,
            timestamp=timestamp,
            nonce=nonce,
        )

    def test_valid_signature_consumes_nonce_once(self) -> None:
        signature = self._signature()

        identity = self.auth.verify(
            worker_id="worker-a",
            worker_instance_id="instance-1",
            method="POST",
            path="/v2/tasks/claim",
            body=b'{"x":1}',
            timestamp="1000",
            nonce="nonce-a",
            signature=signature,
        )

        self.assertEqual(identity, ("worker-a", "instance-1"))
        with self.assertRaisesRegex(AuthenticationError, "nonce replay"):
            self.auth.verify(
                worker_id="worker-a",
                worker_instance_id="instance-1",
                method="POST",
                path="/v2/tasks/claim",
                body=b'{"x":1}',
                timestamp="1000",
                nonce="nonce-a",
                signature=signature,
            )

    def test_signature_binds_body_path_and_instance(self) -> None:
        signature = self._signature(nonce="nonce-body")
        for body, path, instance in (
            (b'{"x":2}', "/v2/tasks/claim", "instance-1"),
            (b'{"x":1}', "/v2/tasks/renew", "instance-1"),
            (b'{"x":1}', "/v2/tasks/claim", "instance-2"),
        ):
            with self.subTest(body=body, path=path, instance=instance):
                with self.assertRaisesRegex(AuthenticationError, "signature"):
                    self.auth.verify(
                        worker_id="worker-a",
                        worker_instance_id=instance,
                        method="POST",
                        path=path,
                        body=body,
                        timestamp="1000",
                        nonce="nonce-body",
                        signature=signature,
                    )

    def test_expired_timestamp_is_rejected_before_nonce_consumption(self) -> None:
        signature = self._signature(
            timestamp="900",
            nonce="nonce-old",
        )
        with self.assertRaisesRegex(AuthenticationError, "expired"):
            self.auth.verify(
                worker_id="worker-a",
                worker_instance_id="instance-1",
                method="POST",
                path="/v2/tasks/claim",
                body=b'{"x":1}',
                timestamp="900",
                nonce="nonce-old",
                signature=signature,
            )
        # The same nonce may be used with a fresh timestamp because the
        # expired request never entered the replay ledger.
        fresh = self._signature(
            timestamp="1000",
            nonce="nonce-old",
        )
        self.assertEqual(
            self.auth.verify(
                worker_id="worker-a",
                worker_instance_id="instance-1",
                method="POST",
                path="/v2/tasks/claim",
                body=b'{"x":1}',
                timestamp="1000",
                nonce="nonce-old",
                signature=fresh,
            ),
            ("worker-a", "instance-1"),
        )


if __name__ == "__main__":
    unittest.main()
