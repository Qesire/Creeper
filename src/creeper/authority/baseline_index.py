"""Year-aware V3 baseline index.

The V3 authority files are large enough that the candidate pool must not be
implemented as a second pass of updates over the annual table. The index
therefore keeps annual presence and official-candidate membership in separate
tables. Each input file has a durable line offset so a killed build can be
resumed without discarding completed stages.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from itertools import islice
import sqlite3
from pathlib import Path

from .normalizer import normalize_official


YEAR_BITS = {year: 1 << (year - 1996) for year in range(1996, 2002)}
ALL_YEAR_MASK = sum(YEAR_BITS.values())


def novel_year_mask(evidence_mask: int, baseline_mask: int, target_mask: int = ALL_YEAR_MASK) -> int:
    return evidence_mask & ~baseline_mask & target_mask


class BaselineIndex:
    def __init__(self, path: Path):
        self.path = path
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row

    @classmethod
    def build(
        cls,
        task_root: Path,
        output_path: Path,
        *,
        batch_size: int = 50_000,
        resume: bool = True,
    ) -> "BaselineIndex":
        """Build or resume the V3 index."""
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        baseline_dir = task_root / "merged260909-3"
        if not baseline_dir.is_dir():
            raise FileNotFoundError(f"missing V3 baseline directory: {baseline_dir}")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(output_path)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA temp_store=FILE")
        existing_tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        if "hostnames" in existing_tables and "annual_hostnames" not in existing_tables:
            connection.close()
            raise ValueError(
                "output contains the obsolete prototype schema; choose a new path"
            )
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS annual_hostnames (
                hostname TEXT PRIMARY KEY,
                year_mask INTEGER NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS candidate_hostnames (
                hostname TEXT PRIMARY KEY
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS import_state (
                stage TEXT PRIMARY KEY,
                line_offset INTEGER NOT NULL DEFAULT 0,
                completed INTEGER NOT NULL DEFAULT 0
            ) WITHOUT ROWID;
            """
        )
        if not resume:
            connection.execute("DELETE FROM annual_hostnames")
            connection.execute("DELETE FROM candidate_hostnames")
            connection.execute("DELETE FROM import_state")
            connection.commit()
        connection.commit()

        def get_state(stage: str) -> tuple[int, bool]:
            row = connection.execute(
                "SELECT line_offset, completed FROM import_state WHERE stage = ?",
                (stage,),
            ).fetchone()
            return (int(row[0]), bool(row[1])) if row else (0, False)

        def save_state(stage: str, offset: int, completed: bool) -> None:
            connection.execute(
                "INSERT INTO import_state(stage, line_offset, completed) VALUES (?, ?, ?) "
                "ON CONFLICT(stage) DO UPDATE SET line_offset=excluded.line_offset, "
                "completed=excluded.completed",
                (stage, offset, int(completed)),
            )
            connection.commit()

        def import_file(path: Path, stage: str, sql: str, params_factory) -> None:
            offset, completed = get_state(stage)
            if completed:
                return
            with path.open("r", encoding="utf-8", errors="replace") as source:
                for _ in range(offset):
                    next(source, None)
                current = offset
                batch: list[tuple[str, int] | tuple[str]] = []
                for line in source:
                    current += 1
                    value = normalize_official(line)
                    if value:
                        batch.append(params_factory(value))
                    if len(batch) >= batch_size:
                        connection.executemany(sql, batch)
                        save_state(stage, current, False)
                        batch.clear()
                if batch:
                    connection.executemany(sql, batch)
                save_state(stage, current, True)

        annual_sql = (
            "INSERT INTO annual_hostnames(hostname, year_mask) VALUES (?, ?) "
            "ON CONFLICT(hostname) DO UPDATE SET year_mask = "
            "annual_hostnames.year_mask | excluded.year_mask"
        )
        for year, bit in YEAR_BITS.items():
            import_file(
                baseline_dir / f"{year}.txt",
                f"year:{year}",
                annual_sql,
                lambda value, bit=bit: (value, bit),
            )

        candidate_sql = "INSERT OR IGNORE INTO candidate_hostnames(hostname) VALUES (?)"
        import_file(
            baseline_dir / "candidate_pool.txt",
            "candidate_pool",
            candidate_sql,
            lambda value: (value,),
        )

        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.commit()
        connection.close()
        return cls(output_path)

    def year_mask(self, hostname: str) -> int:
        value = normalize_official(hostname)
        if value is None:
            return 0
        row = self.connection.execute(
            "SELECT year_mask FROM annual_hostnames WHERE hostname = ?", (value,)
        ).fetchone()
        return int(row["year_mask"]) if row else 0

    def is_official_candidate(self, hostname: str) -> bool:
        value = normalize_official(hostname)
        if value is None:
            return False
        row = self.connection.execute(
            "SELECT 1 FROM candidate_hostnames WHERE hostname = ?", (value,)
        ).fetchone()
        return row is not None

    def resolve_batch(
        self,
        hostnames: Iterable[str],
        *,
        chunk_size: int = 900,
    ) -> dict[str, tuple[int, bool]]:
        """Resolve annual mask and candidate membership in bounded SQL batches.

        The return key is the official-normalized hostname. Invalid or empty
        inputs are omitted. A single batch call avoids the two round trips per
        hostname used by the scalar lookup methods and is the path used by
        throughput benchmarks and large candidate reconciliation jobs.
        """
        if not 1 <= chunk_size <= 900:
            raise ValueError("chunk_size must be between 1 and 900")
        normalized: list[str] = []
        seen: set[str] = set()
        for raw in hostnames:
            value = normalize_official(raw)
            if value is not None and value not in seen:
                seen.add(value)
                normalized.append(value)
        result: dict[str, tuple[int, bool]] = {}
        for start in range(0, len(normalized), chunk_size):
            batch = normalized[start : start + chunk_size]
            placeholders = ",".join("?" for _ in batch)
            annual = {
                row["hostname"]: int(row["year_mask"])
                for row in self.connection.execute(
                    f"SELECT hostname, year_mask FROM annual_hostnames "
                    f"WHERE hostname IN ({placeholders})",
                    batch,
                )
            }
            candidates = {
                row["hostname"]
                for row in self.connection.execute(
                    f"SELECT hostname FROM candidate_hostnames "
                    f"WHERE hostname IN ({placeholders})",
                    batch,
                )
            }
            for hostname in batch:
                result[hostname] = (annual.get(hostname, 0), hostname in candidates)
        return result

    def iter_resolve_batches(
        self,
        hostnames: Iterable[str],
        input_batch_size: int = 50_000,
        chunk_size: int = 900,
    ) -> Iterator[dict[str, tuple[int, bool]]]:
        """Resolve a source iterator in bounded input batches.

        Each input batch is materialized only long enough to preserve the
        existing ``resolve_batch`` semantics. The next source values are not
        consumed until the current resolved batch has been yielded.
        """
        if input_batch_size < 1:
            raise ValueError("input_batch_size must be positive")
        source = iter(hostnames)
        while True:
            batch = list(islice(source, input_batch_size))
            if not batch:
                return
            resolved = self.resolve_batch(batch, chunk_size=chunk_size)
            if resolved:
                yield resolved

    def counts(self) -> dict[str, int]:
        return {
            "annual_hostnames": self.connection.execute(
                "SELECT COUNT(*) FROM annual_hostnames"
            ).fetchone()[0],
            "candidate_hostnames": self.connection.execute(
                "SELECT COUNT(*) FROM candidate_hostnames"
            ).fetchone()[0],
        }

    def close(self) -> None:
        self.connection.close()
