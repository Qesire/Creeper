"""Year-aware baseline index.

The authority files are large enough that the candidate pool must not be
implemented as a second pass of updates over the annual table. The index
therefore keeps annual presence and official-candidate membership in separate
tables. Each input file has a durable line offset so a killed build can be
resumed without discarding completed stages.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from itertools import islice
import json
import sqlite3
from pathlib import Path

from .identity import AuthoritySnapshot
from .normalizer import normalize_official
from .paths import find_baseline_dir


YEAR_BITS = {year: 1 << (year - 1996) for year in range(1996, 2002)}
ALL_YEAR_MASK = sum(YEAR_BITS.values())
BASELINE_INDEX_SCHEMA_VERSION = "baseline-index-v2"


def _coerce_authority(
    value: Path | dict[str, object] | AuthoritySnapshot | None,
) -> AuthoritySnapshot | None:
    if value is None:
        return None
    if isinstance(value, AuthoritySnapshot):
        return value
    if isinstance(value, Path):
        return AuthoritySnapshot.from_manifest_path(value)
    return AuthoritySnapshot.from_manifest(value)


def _authority_metadata(authority: AuthoritySnapshot) -> dict[str, str]:
    return {
        "index_schema_version": BASELINE_INDEX_SCHEMA_VERSION,
        "baseline_id": authority.baseline_id,
        "authority_digest": authority.authority_digest,
        "annual_file_hashes": json.dumps(
            authority.annual_file_hashes, sort_keys=True, separators=(",", ":")
        ),
        "candidate_file_hash": authority.candidate_file_hash,
        "model_hash": authority.model_hash,
        "baseline_eed": authority.baseline_eed,
    }


def novel_year_mask(evidence_mask: int, baseline_mask: int, target_mask: int = ALL_YEAR_MASK) -> int:
    return evidence_mask & ~baseline_mask & target_mask


class BaselineIndex:
    def __init__(
        self,
        path: Path,
        *,
        authority: AuthoritySnapshot | None = None,
    ):
        self.path = path
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        if authority is not None:
            try:
                self._assert_authority(authority)
            except BaseException:
                self.connection.close()
                raise

    def _metadata(self) -> dict[str, str]:
        try:
            rows = self.connection.execute(
                "SELECT key, value FROM authority_metadata"
            ).fetchall()
        except sqlite3.OperationalError:
            return {}
        return {str(row["key"]): str(row["value"]) for row in rows}

    def _assert_authority(self, authority: AuthoritySnapshot) -> None:
        actual = self._metadata()
        if not actual:
            raise ValueError(
                "baseline index has no embedded authority identity; refusing to resume"
            )
        expected = _authority_metadata(authority)
        if actual != expected:
            mismatches = sorted(
                key
                for key in set(actual) | set(expected)
                if actual.get(key) != expected.get(key)
            )
            raise ValueError(
                "baseline index authority mismatch; refusing to resume: "
                + ", ".join(mismatches)
            )

    def assert_authority(self, authority: AuthoritySnapshot) -> None:
        """Verify this open index is bound to the requested authority."""
        self._assert_authority(authority)

    @classmethod
    def bind_authority(
        cls,
        path: Path,
        authority: AuthoritySnapshot,
    ) -> "BaselineIndex":
        """Bind only an empty index shell; populated unbound indexes must rebuild."""
        connection = sqlite3.connect(path)
        try:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS authority_metadata "
                "(key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID"
            )
            existing = {
                str(row[0]): str(row[1])
                for row in connection.execute(
                    "SELECT key, value FROM authority_metadata"
                )
            }
            expected = _authority_metadata(authority)
            if existing and existing != expected:
                raise ValueError("baseline index authority mismatch")
            populated = False
            for table in ("annual_hostnames", "candidate_hostnames", "import_state"):
                exists = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                ).fetchone()
                if exists and connection.execute(
                    f"SELECT 1 FROM {table} LIMIT 1"
                ).fetchone():
                    populated = True
                    break
            if populated and not existing:
                raise ValueError(
                    "cannot bind authority to a populated unbound index; "
                    "build a new authority-bound index"
                )
            connection.executemany(
                "INSERT INTO authority_metadata(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                sorted(expected.items()),
            )
            connection.commit()
        finally:
            connection.close()
        return cls(path, authority=authority)

    @classmethod
    def build(
        cls,
        task_root: Path | None = None,
        output_path: Path | None = None,
        *,
        baseline_dir: Path | None = None,
        authority_manifest: Path | dict[str, object] | AuthoritySnapshot | None = None,
        batch_size: int = 50_000,
        resume: bool = True,
    ) -> "BaselineIndex":
        """Build or resume the baseline index for a task package."""
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        if output_path is None:
            raise ValueError("output_path is required")
        if baseline_dir is None and task_root is None:
            raise ValueError("task_root or baseline_dir is required")
        baseline_dir = (
            Path(baseline_dir)
            if baseline_dir is not None
            else find_baseline_dir(Path(task_root))
        )
        authority = _coerce_authority(authority_manifest)
        if authority is None:
            raise ValueError(
                "authority_manifest is required for baseline index construction"
            )
        if baseline_dir is None:
            assert task_root is not None
            baseline_dir = Path(task_root) / authority.baseline_id
        else:
            baseline_dir = Path(baseline_dir)
        authority.verify_baseline_dir(baseline_dir)

        output_path = Path(output_path)
        if not resume and output_path.exists() and output_path.stat().st_size > 0:
            raise ValueError(
                "non-resume rebuild refuses to mutate an existing index; "
                "choose a new output path or remove it after readers are quiesced"
            )
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
            CREATE TABLE IF NOT EXISTS authority_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) WITHOUT ROWID;
            """
        )
        existing_metadata = {
            str(row[0]): str(row[1])
            for row in connection.execute(
                "SELECT key, value FROM authority_metadata"
            )
        }
        expected_metadata = _authority_metadata(authority)
        has_work = any(
            connection.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
            is not None
            for table in ("annual_hostnames", "candidate_hostnames", "import_state")
        )
        if existing_metadata:
            if existing_metadata != expected_metadata:
                mismatches = sorted(
                    key
                    for key in set(existing_metadata) | set(expected_metadata)
                    if existing_metadata.get(key) != expected_metadata.get(key)
                )
                connection.close()
                raise ValueError(
                    "baseline index authority mismatch; refusing to resume: "
                    + ", ".join(mismatches)
                )
        elif has_work:
            connection.close()
            raise ValueError(
                "baseline index has unbound data/import_state; refusing to resume"
            )
        connection.executemany(
            "INSERT INTO authority_metadata(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            sorted(expected_metadata.items()),
        )
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
        return cls(output_path, authority=authority)

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
