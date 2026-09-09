"""SQLite evidence-capsule store with idempotent writes."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from creeper.authority.normalizer import normalize_official
from creeper.evidence.policies import EvidenceCapsule


class EvidenceStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS evidence_capsules (
                hostname TEXT NOT NULL,
                year INTEGER NOT NULL,
                provider TEXT NOT NULL,
                temporal_semantics TEXT NOT NULL,
                evidence_timestamp TEXT NOT NULL,
                source_locator TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                PRIMARY KEY(hostname, year, provider, payload_hash)
            ) WITHOUT ROWID
            """
        )
        self.connection.commit()

    def put(self, capsule: EvidenceCapsule) -> None:
        hostname = normalize_official(capsule.hostname)
        if hostname is None:
            raise ValueError("evidence capsule contains an invalid hostname")
        self.connection.execute(
            """
            INSERT OR IGNORE INTO evidence_capsules(
                hostname, year, provider, temporal_semantics, evidence_timestamp,
                source_locator, payload_hash, policy_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                hostname, capsule.year, capsule.provider, capsule.temporal_semantics,
                capsule.evidence_timestamp, capsule.source_locator,
                capsule.payload_hash, capsule.policy_version,
            ),
        )
        self.connection.commit()

    def for_hostname(self, hostname: str) -> list[EvidenceCapsule]:
        value = normalize_official(hostname)
        if value is None:
            return []
        rows = self.connection.execute(
            "SELECT * FROM evidence_capsules WHERE hostname = ? ORDER BY year, provider",
            (value,),
        ).fetchall()
        return [EvidenceCapsule(**dict(row)) for row in rows]

    def count(self) -> int:
        return self.connection.execute("SELECT COUNT(*) FROM evidence_capsules").fetchone()[0]

    def close(self) -> None:
        self.connection.close()
