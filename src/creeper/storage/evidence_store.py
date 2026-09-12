"""SQLite evidence-capsule store with idempotent writes."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from creeper.authority.baseline_index import YEAR_BITS
from creeper.authority.normalizer import normalize_official
from creeper.evidence.policies import EvidenceCapsule


@dataclass(frozen=True)
class EvidenceHostYear:
    sequence: int
    hostname: str
    year: int


class EvidenceStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, timeout=30.0)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA busy_timeout=30000")
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
                    evidence_type TEXT NOT NULL DEFAULT '',
                    source_id TEXT NOT NULL DEFAULT '',
                    original_url TEXT NOT NULL DEFAULT '',
                    record_locator TEXT NOT NULL DEFAULT '',
                    extraction_method TEXT NOT NULL DEFAULT '',
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
        current_columns = {
            str(row[1])
            for row in self.connection.execute(
                "PRAGMA table_info(evidence_capsules)"
            ).fetchall()
        }
        for name in (
            "evidence_type",
            "source_id",
            "original_url",
            "record_locator",
            "extraction_method",
        ):
            if name not in current_columns:
                self.connection.execute(
                    f"ALTER TABLE evidence_capsules ADD COLUMN {name} TEXT NOT NULL DEFAULT ''"
                )
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS evidence_host_years (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                hostname TEXT NOT NULL,
                year INTEGER NOT NULL,
                UNIQUE(hostname, year)
            );
            CREATE TABLE IF NOT EXISTS evidence_store_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) WITHOUT ROWID;
            """
        )
        # Serialize the one-time backfill across independently started
        # producer/evidence/readiness processes. Without this transaction, all
        # three can observe a missing marker and scan the full capsule table.
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            indexed = self.connection.execute(
                "SELECT 1 FROM evidence_store_meta WHERE key = ?",
                ("host-year-index-v1",),
            ).fetchone()
            if indexed is None:
                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO evidence_host_years(hostname, year)
                    SELECT hostname, year
                    FROM evidence_capsules
                    GROUP BY hostname, year
                    ORDER BY hostname, year
                    """
                )
                self.connection.execute(
                    """
                    INSERT INTO evidence_store_meta(key, value)
                    VALUES (?, ?)
                    """,
                    ("host-year-index-v1", "complete"),
                )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

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
                    capsule.evidence_type, capsule.source_id, capsule.original_url,
                    capsule.record_locator, capsule.extraction_method,
                )
            )
        if not rows:
            return 0
        before = self.connection.total_changes
        host_years = list(
            dict.fromkeys((str(row[0]), int(row[1])) for row in rows)
        )
        with self.connection:
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO evidence_capsules(
                    hostname, year, provider, temporal_semantics, evidence_timestamp,
                    source_locator, payload_hash, policy_version, evidence_type,
                    source_id, original_url, record_locator, extraction_method
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            inserted_capsules = self.connection.total_changes - before
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO evidence_host_years(hostname, year)
                VALUES (?, ?)
                """,
                host_years,
            )
        return inserted_capsules

    def for_hostname(self, hostname: str) -> list[EvidenceCapsule]:
        value = normalize_official(hostname)
        if value is None:
            return []
        rows = self.connection.execute(
            "SELECT * FROM evidence_capsules WHERE hostname = ? ORDER BY year, provider",
            (value,),
        ).fetchall()
        return [EvidenceCapsule(**dict(row)) for row in rows]

    def canonical_host_year_capsules(self) -> list[EvidenceCapsule]:
        """Return one deterministic capsule for each proven host-year."""
        rows = self.connection.execute(
            """
            WITH ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY hostname, year
                           ORDER BY provider, payload_hash, policy_version
                       ) AS rn
                FROM evidence_capsules
            )
            SELECT hostname, year, provider, temporal_semantics,
                   evidence_timestamp, source_locator, payload_hash,
                   policy_version, evidence_type, source_id, original_url,
                   record_locator, extraction_method
            FROM ranked
            WHERE rn = 1
            ORDER BY hostname, year
            """
        ).fetchall()
        return [EvidenceCapsule(**dict(row)) for row in rows]

    def host_years_after(
        self,
        sequence: int,
        *,
        limit: int = 50_000,
    ) -> list[EvidenceHostYear]:
        if sequence < 0:
            raise ValueError("sequence must be non-negative")
        if limit < 1:
            raise ValueError("limit must be positive")
        rows = self.connection.execute(
            """
            SELECT sequence, hostname, year
            FROM evidence_host_years
            WHERE sequence > ?
            ORDER BY sequence
            LIMIT ?
            """,
            (int(sequence), int(limit)),
        ).fetchall()
        return [
            EvidenceHostYear(
                sequence=int(row["sequence"]),
                hostname=str(row["hostname"]),
                year=int(row["year"]),
            )
            for row in rows
        ]

    def max_host_year_sequence(self) -> int:
        row = self.connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) FROM evidence_host_years"
        ).fetchone()
        return int(row[0] or 0)

    def host_year_count(self) -> int:
        return int(
            self.connection.execute(
                "SELECT COUNT(*) FROM evidence_host_years"
            ).fetchone()[0]
        )

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
            # evidence_host_years is the compact deduplicated authority
            # projection maintained by put_many(). Querying it avoids scanning
            # multiple capsule witnesses for the same proven host-year.
            rows = self.connection.execute(
                f"SELECT hostname, year FROM evidence_host_years "
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
