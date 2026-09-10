"""SQLite evidence-capsule store with idempotent writes."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from pathlib import Path

from creeper.authority.baseline_index import YEAR_BITS
from creeper.authority.normalizer import normalize_official
from creeper.evidence.policies import EvidenceCapsule


class EvidenceStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        columns = self.connection.execute(
            "PRAGMA table_info(evidence_capsules)"
        ).fetchall()
        if not columns:
            self.connection.execute(
                """
                CREATE TABLE evidence_capsules (
                    hostname TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    provider TEXT NOT NULL,
                    temporal_semantics TEXT NOT NULL,
                    evidence_timestamp TEXT NOT NULL,
                    source_locator TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    policy_version TEXT NOT NULL,
                    PRIMARY KEY(hostname, year, provider, payload_hash, policy_version)
                ) WITHOUT ROWID
                """
            )
        else:
            primary_key = {row[1] for row in columns if row[5]}
            if "policy_version" not in primary_key:
                self.connection.execute(
                    """
                    CREATE TABLE evidence_capsules_v2 (
                        hostname TEXT NOT NULL,
                        year INTEGER NOT NULL,
                        provider TEXT NOT NULL,
                        temporal_semantics TEXT NOT NULL,
                        evidence_timestamp TEXT NOT NULL,
                        source_locator TEXT NOT NULL,
                        payload_hash TEXT NOT NULL,
                        policy_version TEXT NOT NULL,
                        PRIMARY KEY(hostname, year, provider, payload_hash, policy_version)
                    ) WITHOUT ROWID
                    """
                )
                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO evidence_capsules_v2(
                        hostname, year, provider, temporal_semantics, evidence_timestamp,
                        source_locator, payload_hash, policy_version
                    ) SELECT hostname, year, provider, temporal_semantics, evidence_timestamp,
                        source_locator, payload_hash, policy_version
                    FROM evidence_capsules
                    """
                )
                self.connection.execute("DROP TABLE evidence_capsules")
                self.connection.execute(
                    "ALTER TABLE evidence_capsules_v2 RENAME TO evidence_capsules"
                )
        self.connection.commit()

    def put(self, capsule: EvidenceCapsule) -> None:
        self.put_many([capsule])

    def put_many(self, capsules: Iterable[EvidenceCapsule]) -> int:
        rows = []
        for capsule in capsules:
            hostname = normalize_official(capsule.hostname)
            if hostname is None:
                raise ValueError("evidence capsule contains an invalid hostname")
            rows.append(
                (
                    hostname, capsule.year, capsule.provider, capsule.temporal_semantics,
                    capsule.evidence_timestamp, capsule.source_locator,
                    capsule.payload_hash, capsule.policy_version,
                )
            )
        if not rows:
            return 0
        with self.connection:
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO evidence_capsules(
                    hostname, year, provider, temporal_semantics, evidence_timestamp,
                    source_locator, payload_hash, policy_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)

    def for_hostname(self, hostname: str) -> list[EvidenceCapsule]:
        value = normalize_official(hostname)
        if value is None:
            return []
        rows = self.connection.execute(
            "SELECT * FROM evidence_capsules WHERE hostname = ? ORDER BY year, provider",
            (value,),
        ).fetchall()
        return [EvidenceCapsule(**dict(row)) for row in rows]

    def all_capsules(self) -> list[EvidenceCapsule]:
        """Return every persisted capsule in a stable, reproducible order."""
        rows = self.connection.execute(
            "SELECT * FROM evidence_capsules "
            "ORDER BY hostname, year, provider, payload_hash, policy_version"
        ).fetchall()
        return [EvidenceCapsule(**dict(row)) for row in rows]

    def resolve_year_masks(
        self, hostnames: Iterable[str], chunk_size: int = 900
    ) -> dict[str, int]:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")

        normalized = {}
        for raw_hostname in hostnames:
            if not isinstance(raw_hostname, str):
                continue
            hostname = normalize_official(raw_hostname)
            if hostname is not None:
                normalized.setdefault(hostname, 0)

        result = dict(normalized)
        limit = min(chunk_size, 900)
        values = list(normalized)
        for start in range(0, len(values), limit):
            chunk = values[start:start + limit]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.connection.execute(
                f"SELECT hostname, year FROM evidence_capsules "
                f"WHERE hostname IN ({placeholders})",
                chunk,
            ).fetchall()
            for row in rows:
                result[row["hostname"]] |= YEAR_BITS.get(row["year"], 0)
        return result

    def count(self) -> int:
        return self.connection.execute("SELECT COUNT(*) FROM evidence_capsules").fetchone()[0]

    def close(self) -> None:
        self.connection.close()
