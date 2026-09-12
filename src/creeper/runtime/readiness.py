"""Incremental annual EED readiness over append-only evidence host-years."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Callable

from creeper.authority.baseline_index import BaselineIndex, YEAR_BITS
from creeper.authority.eed import load_english_weights
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceHostYear, EvidenceStore


FORMAL_GROWTH_RATE = Decimal("0.05")
PREWARM_GATE_FRACTION = Decimal("0.90")


def _metadata_signature(path: Path) -> str:
    """Cheap cache-invalidation signature for an immutable authority file."""
    resolved = Path(path).resolve()
    stat = resolved.stat()
    payload = (
        f"{resolved}\0{stat.st_size}\0{stat.st_mtime_ns}"
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _model_signature(path: Path) -> str:
    """Hash the small EED model exactly; final submission also records its path."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class IncrementalReadinessReport:
    baseline_signature: str
    model_signature: str
    evidence_cursor: int
    latest_evidence_sequence: int
    processed_host_years: int
    novel_host_years: int
    novel_eed: str
    baseline_eed: str
    growth_rate: str
    five_percent_delta: str
    confirmed_fraction_of_five_percent: str
    prewarm_reached: bool
    formal_gate_reached: bool
    annual: dict[str, dict[str, object]]
    source_attribution: dict[str, dict[str, object]]

    def as_dict(self) -> dict[str, object]:
        return {
            "report_version": "incremental-readiness-v1",
            "baseline_signature": self.baseline_signature,
            "model_signature": self.model_signature,
            "evidence_cursor": self.evidence_cursor,
            "latest_evidence_sequence": self.latest_evidence_sequence,
            "processed_host_years": self.processed_host_years,
            "novel_host_years": self.novel_host_years,
            "novel_eed": self.novel_eed,
            "baseline_eed": self.baseline_eed,
            "growth_rate": self.growth_rate,
            "five_percent_delta": self.five_percent_delta,
            "confirmed_fraction_of_five_percent": (
                self.confirmed_fraction_of_five_percent
            ),
            "prewarm_reached": self.prewarm_reached,
            "formal_gate_reached": self.formal_gate_reached,
            "annual": self.annual,
            "source_attribution": self.source_attribution,
        }


class IncrementalReadinessLedger:
    """Crash-safe cumulative novelty/EED counters keyed by EvidenceHostYear sequence."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = Path(path)
        self.connection = sqlite3.connect(path, timeout=30.0)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA busy_timeout=30000")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS readiness_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                baseline_signature TEXT NOT NULL,
                model_signature TEXT NOT NULL,
                evidence_cursor INTEGER NOT NULL,
                processed_host_years INTEGER NOT NULL,
                novel_host_years INTEGER NOT NULL,
                novel_eed TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS readiness_annual (
                year INTEGER PRIMARY KEY,
                novel_host_years INTEGER NOT NULL,
                novel_eed TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS readiness_source (
                source_key TEXT PRIMARY KEY,
                novel_host_years INTEGER NOT NULL,
                novel_eed TEXT NOT NULL
            ) WITHOUT ROWID;
            """
        )
        self.connection.commit()

    def ensure_authority(
        self,
        *,
        baseline_signature: str,
        model_signature: str,
    ) -> bool:
        """Reset cached novelty if baseline/model identity changed."""
        row = self.connection.execute(
            "SELECT baseline_signature, model_signature FROM readiness_state "
            "WHERE singleton = 1"
        ).fetchone()
        if (
            row is not None
            and str(row["baseline_signature"]) == baseline_signature
            and str(row["model_signature"]) == model_signature
        ):
            return False

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self.connection.execute("DELETE FROM readiness_annual")
            self.connection.execute("DELETE FROM readiness_source")
            self.connection.execute("DELETE FROM readiness_state")
            self.connection.execute(
                """
                INSERT INTO readiness_state(
                    singleton, baseline_signature, model_signature,
                    evidence_cursor, processed_host_years,
                    novel_host_years, novel_eed
                ) VALUES (1, ?, ?, 0, 0, 0, '0')
                """,
                (baseline_signature, model_signature),
            )
            for year in YEAR_BITS:
                self.connection.execute(
                    """
                    INSERT INTO readiness_annual(year, novel_host_years, novel_eed)
                    VALUES (?, 0, '0')
                    """,
                    (year,),
                )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        return True

    def cursor(self) -> int:
        row = self.connection.execute(
            "SELECT evidence_cursor FROM readiness_state WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise RuntimeError("readiness authority has not been initialized")
        return int(row["evidence_cursor"])

    def apply_batch(
        self,
        rows: list[EvidenceHostYear],
        *,
        baseline: BaselineIndex,
        weights: dict[str, Decimal],
        source_origins: dict[tuple[str, int], str] | None = None,
    ) -> int:
        if not rows:
            return 0
        expected_cursor = self.cursor()
        if rows[0].sequence <= expected_cursor:
            raise RuntimeError("readiness batch does not advance current cursor")

        resolved = baseline.resolve_batch(row.hostname for row in rows)
        annual_counts: dict[int, int] = {year: 0 for year in YEAR_BITS}
        annual_eed: dict[int, Decimal] = {
            year: Decimal("0") for year in YEAR_BITS
        }
        novel_count = 0
        novel_eed = Decimal("0")
        source_origins = source_origins or {}
        source_counts: dict[str, int] = {}
        source_eed: dict[str, Decimal] = {}

        for row in rows:
            bit = YEAR_BITS.get(row.year)
            if bit is None:
                continue
            baseline_mask = resolved.get(row.hostname, (0, False))[0]
            if baseline_mask & bit:
                continue
            contribution = weights.get(
                row.hostname.rsplit(".", 1)[-1],
                Decimal("0"),
            )
            annual_counts[row.year] += 1
            annual_eed[row.year] += contribution
            novel_count += 1
            novel_eed += contribution
            source_key = source_origins.get((row.hostname, row.year))
            if source_key is not None:
                source_counts[source_key] = source_counts.get(source_key, 0) + 1
                source_eed[source_key] = (
                    source_eed.get(source_key, Decimal("0")) + contribution
                )

        new_cursor = rows[-1].sequence
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            state = self.connection.execute(
                """
                SELECT evidence_cursor, processed_host_years,
                       novel_host_years, novel_eed
                FROM readiness_state
                WHERE singleton = 1
                """
            ).fetchone()
            if state is None:
                raise RuntimeError("readiness authority disappeared")
            if int(state["evidence_cursor"]) != expected_cursor:
                raise RuntimeError("readiness cursor changed concurrently")

            self.connection.execute(
                """
                UPDATE readiness_state
                SET evidence_cursor = ?,
                    processed_host_years = ?,
                    novel_host_years = ?,
                    novel_eed = ?
                WHERE singleton = 1
                """,
                (
                    new_cursor,
                    int(state["processed_host_years"]) + len(rows),
                    int(state["novel_host_years"]) + novel_count,
                    format(
                        Decimal(str(state["novel_eed"])) + novel_eed,
                        "f",
                    ),
                ),
            )
            for source_key in sorted(source_counts):
                current_source = self.connection.execute(
                    """
                    SELECT novel_host_years, novel_eed
                    FROM readiness_source WHERE source_key = ?
                    """,
                    (source_key,),
                ).fetchone()
                if current_source is None:
                    self.connection.execute(
                        """
                        INSERT INTO readiness_source(
                            source_key, novel_host_years, novel_eed
                        ) VALUES (?, ?, ?)
                        """,
                        (
                            source_key,
                            source_counts[source_key],
                            format(source_eed[source_key], "f"),
                        ),
                    )
                else:
                    self.connection.execute(
                        """
                        UPDATE readiness_source
                        SET novel_host_years = ?, novel_eed = ?
                        WHERE source_key = ?
                        """,
                        (
                            int(current_source["novel_host_years"])
                            + source_counts[source_key],
                            format(
                                Decimal(str(current_source["novel_eed"]))
                                + source_eed[source_key],
                                "f",
                            ),
                            source_key,
                        ),
                    )
            for year in YEAR_BITS:
                current = self.connection.execute(
                    """
                    SELECT novel_host_years, novel_eed
                    FROM readiness_annual WHERE year = ?
                    """,
                    (year,),
                ).fetchone()
                assert current is not None
                self.connection.execute(
                    """
                    UPDATE readiness_annual
                    SET novel_host_years = ?, novel_eed = ?
                    WHERE year = ?
                    """,
                    (
                        int(current["novel_host_years"]) + annual_counts[year],
                        format(
                            Decimal(str(current["novel_eed"]))
                            + annual_eed[year],
                            "f",
                        ),
                        year,
                    ),
                )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        return len(rows)

    def report(
        self,
        *,
        latest_evidence_sequence: int,
        baseline_eed: Decimal,
    ) -> IncrementalReadinessReport:
        state = self.connection.execute(
            "SELECT * FROM readiness_state WHERE singleton = 1"
        ).fetchone()
        if state is None:
            raise RuntimeError("readiness authority has not been initialized")

        annual = {
            str(row["year"]): {
                "novel_host_years": int(row["novel_host_years"]),
                "novel_eed": str(row["novel_eed"]),
            }
            for row in self.connection.execute(
                "SELECT * FROM readiness_annual ORDER BY year"
            )
        }
        source_attribution = {
            str(row["source_key"]): {
                "novel_host_years": int(row["novel_host_years"]),
                "novel_eed": str(row["novel_eed"]),
            }
            for row in self.connection.execute(
                "SELECT * FROM readiness_source ORDER BY source_key"
            )
        }
        novel_eed = Decimal(str(state["novel_eed"]))
        five_percent = baseline_eed * FORMAL_GROWTH_RATE
        growth_rate = (
            novel_eed / baseline_eed
            if baseline_eed > 0
            else Decimal("0")
        )
        fraction = (
            novel_eed / five_percent
            if five_percent > 0
            else Decimal("0")
        )
        return IncrementalReadinessReport(
            baseline_signature=str(state["baseline_signature"]),
            model_signature=str(state["model_signature"]),
            evidence_cursor=int(state["evidence_cursor"]),
            latest_evidence_sequence=int(latest_evidence_sequence),
            processed_host_years=int(state["processed_host_years"]),
            novel_host_years=int(state["novel_host_years"]),
            novel_eed=format(novel_eed, "f"),
            baseline_eed=format(baseline_eed, "f"),
            growth_rate=format(growth_rate, "f"),
            five_percent_delta=format(five_percent, "f"),
            confirmed_fraction_of_five_percent=format(fraction, "f"),
            prewarm_reached=fraction >= PREWARM_GATE_FRACTION,
            formal_gate_reached=growth_rate >= FORMAL_GROWTH_RATE,
            annual=annual,
            source_attribution=source_attribution,
        )

    def close(self) -> None:
        self.connection.close()


class IncrementalReadinessRuntime:
    """Incrementally reconcile new evidence host-years against current authority."""

    def __init__(
        self,
        runtime_data_root: Path,
        *,
        baseline_index: Path,
        eed_model: Path,
        baseline_eed: Decimal | str | int,
        batch_size: int = 50_000,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        self.runtime_data_root = Path(runtime_data_root)
        self.baseline_path = Path(baseline_index)
        self.model_path = Path(eed_model)
        self.baseline_eed = Decimal(str(baseline_eed))
        if not self.baseline_eed.is_finite() or self.baseline_eed < 0:
            raise ValueError("baseline_eed must be a finite non-negative decimal")
        self.batch_size = int(batch_size)
        self.evidence = EvidenceStore(
            self.runtime_data_root / "evidence.sqlite3"
        )
        self.control: ControlStore | None = None
        self.ledger = IncrementalReadinessLedger(
            self.runtime_data_root / "readiness.sqlite3"
        )
        self.baseline: BaselineIndex | None = None
        self.weights: dict[str, Decimal] = {}
        self._baseline_signature = ""
        self._model_signature = ""
        self._refresh_authority(force=True)

    def _refresh_authority(self, *, force: bool = False) -> bool:
        baseline_signature = _metadata_signature(self.baseline_path)
        model_signature = _model_signature(self.model_path)
        changed = (
            force
            or baseline_signature != self._baseline_signature
            or model_signature != self._model_signature
        )
        if not changed:
            return False

        if self.baseline is not None:
            self.baseline.close()
        self.baseline = BaselineIndex(self.baseline_path)
        self.weights = load_english_weights(self.model_path)
        self._baseline_signature = baseline_signature
        self._model_signature = model_signature
        self.ledger.ensure_authority(
            baseline_signature=baseline_signature,
            model_signature=model_signature,
        )
        return True

    def _source_origins(
        self,
        rows: list[EvidenceHostYear],
    ) -> dict[tuple[str, int], str]:
        control_path = self.runtime_data_root / "control.sqlite3"
        if self.control is None and control_path.exists():
            self.control = ControlStore(control_path)
        if self.control is None:
            return {}
        return self.control.resolve_primary_source_origins(
            (row.hostname, row.year) for row in rows
        )

    def sync_once(self) -> IncrementalReadinessReport:
        self._refresh_authority()
        cursor = self.ledger.cursor()
        rows = self.evidence.host_years_after(
            cursor,
            limit=self.batch_size,
        )
        if rows:
            assert self.baseline is not None
            self.ledger.apply_batch(
                rows,
                baseline=self.baseline,
                weights=self.weights,
                source_origins=self._source_origins(rows),
            )
        return self.ledger.report(
            latest_evidence_sequence=self.evidence.max_host_year_sequence(),
            baseline_eed=self.baseline_eed,
        )

    def sync_until_current(
        self,
        *,
        max_batches: int | None = None,
    ) -> IncrementalReadinessReport:
        if max_batches is not None and max_batches < 1:
            raise ValueError("max_batches must be positive when provided")
        batches = 0
        while True:
            report = self.sync_once()
            if report.evidence_cursor >= report.latest_evidence_sequence:
                return report
            batches += 1
            if max_batches is not None and batches >= max_batches:
                return report

    @staticmethod
    def write_report_atomic(
        report: IncrementalReadinessReport,
        path: Path,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(report.as_dict(), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)

    def close(self) -> None:
        if self.baseline is not None:
            self.baseline.close()
            self.baseline = None
        if self.control is not None:
            self.control.close()
            self.control = None
        self.ledger.close()
        self.evidence.close()

    def __enter__(self) -> "IncrementalReadinessRuntime":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
