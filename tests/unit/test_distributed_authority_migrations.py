from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from creeper.distributed.authority_store import DistributedAuthorityStore


class DistributedAuthorityMigrationTests(unittest.TestCase):
    def test_old_provider_permit_table_gains_request_identity_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "authority.sqlite3"
            connection = sqlite3.connect(path)
            connection.executescript(
                """
                CREATE TABLE distributed_provider_permits (
                    permit_id TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    worker_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    allowed_requests INTEGER NOT NULL,
                    max_inflight INTEGER NOT NULL,
                    expires_at REAL NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1,
                    issued_at REAL NOT NULL,
                    status_code INTEGER
                ) WITHOUT ROWID;
                CREATE INDEX idx_distributed_provider_permit_active
                    ON distributed_provider_permits(
                        provider, active, expires_at
                    );
                """
            )
            connection.commit()
            connection.close()

            store = DistributedAuthorityStore(path)
            try:
                columns = {
                    str(row["name"])
                    for row in store.connection.execute(
                        "PRAGMA table_info(distributed_provider_permits)"
                    )
                }
                self.assertIn("request_id", columns)
                indexes = {
                    str(row["name"])
                    for row in store.connection.execute(
                        "PRAGMA index_list(distributed_provider_permits)"
                    )
                }
                self.assertIn(
                    "idx_distributed_provider_permit_request",
                    indexes,
                )
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
