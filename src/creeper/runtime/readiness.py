"""Incremental annual EED readiness over append-only evidence host-years."""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Callable

from creeper.authority.baseline_index import BaselineIndex, YEAR_BITS
from creeper.authority.eed import load_english_weights
from creeper.authority.identity import (
    AuthoritySnapshot,
    baseline_authority_signature,
    eed_model_authority_signature,
)
from creeper.scheduler.leases import LeaseState
from creeper.source_discovery.registry import SourceDiscoveryRegistry, SourceRunOutcome
from creeper.storage.candidate_store import CandidateStore
from creeper.storage.control_store import ControlStore, TERMINAL_STATES
from creeper.storage.evidence_store import EvidenceHostYear, EvidenceStore


FORMAL_GROWTH_RATE = Decimal("0.05")
PREWARM_GATE_FRACTION = Decimal("0.90")
DEFAULT_DISPATCH_THRESHOLD = Decimal("0.0525")


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
    task_kind_attribution: dict[str, dict[str, object]]
    source_run_attribution: dict[str, dict[str, object]] | None = None
    baseline_id: str = ""
    authority_digest: str = ""
    dispatch_threshold: str = format(DEFAULT_DISPATCH_THRESHOLD, "f")
    submission_dispatch_ready: bool = False
    baseline_reconciliation: dict[str, object] | None = None
    source_contribution: dict[str, object] | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "report_version": "incremental-readiness-v2",
            "baseline_id": self.baseline_id,
            "authority_digest": self.authority_digest,
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
            "dispatch_threshold": self.dispatch_threshold,
            "submission_dispatch_ready": self.submission_dispatch_ready,
            "annual": self.annual,
            "source_attribution": self.source_attribution,
            "task_kind_attribution": self.task_kind_attribution,
            "source_run_attribution": self.source_run_attribution or {},
            "baseline_reconciliation": self.baseline_reconciliation or {},
            "source_contribution": self.source_contribution or {},
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
            CREATE TABLE IF NOT EXISTS readiness_task_kind (
                task_kind TEXT PRIMARY KEY,
                novel_host_years INTEGER NOT NULL,
                novel_eed TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS readiness_source_run (
                source_key TEXT NOT NULL,
                reservoir_id TEXT NOT NULL,
                lease_id TEXT NOT NULL,
                novel_host_years INTEGER NOT NULL DEFAULT 0,
                novel_eed TEXT NOT NULL DEFAULT '0',
                max_evidence_sequence INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(source_key, reservoir_id, lease_id)
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
            self.connection.execute("DELETE FROM readiness_task_kind")
            self.connection.execute("DELETE FROM readiness_source_run")
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
        task_kinds: dict[tuple[str, int], str] | None = None,
        run_origins: dict[
            tuple[str, int], tuple[str, str, str]
        ] | None = None,
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
        task_kinds = task_kinds or {}
        run_origins = run_origins or {}
        source_counts: dict[str, int] = {}
        source_eed: dict[str, Decimal] = {}
        task_kind_counts: dict[str, int] = {}
        task_kind_eed: dict[str, Decimal] = {}
        run_counts: dict[tuple[str, str, str], int] = {}
        run_eed: dict[tuple[str, str, str], Decimal] = {}
        run_max_sequence: dict[tuple[str, str, str], int] = {}

        for row in rows:
            pair = (row.hostname, row.year)
            run_origin = run_origins.get(pair)
            if run_origin is not None:
                run_max_sequence[run_origin] = max(
                    run_max_sequence.get(run_origin, 0),
                    int(row.sequence),
                )
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
            source_key = source_origins.get(pair)
            if source_key is not None:
                source_counts[source_key] = source_counts.get(source_key, 0) + 1
                source_eed[source_key] = (
                    source_eed.get(source_key, Decimal("0")) + contribution
                )
            if run_origin is not None:
                run_counts[run_origin] = run_counts.get(run_origin, 0) + 1
                run_eed[run_origin] = (
                    run_eed.get(run_origin, Decimal("0")) + contribution
                )
            task_kind = task_kinds.get(pair)
            if task_kind is not None:
                task_kind_counts[task_kind] = task_kind_counts.get(task_kind, 0) + 1
                task_kind_eed[task_kind] = (
                    task_kind_eed.get(task_kind, Decimal("0")) + contribution
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
            for run_key in sorted(run_max_sequence):
                source_key, reservoir_id, lease_id = run_key
                current_run = self.connection.execute(
                    """
                    SELECT novel_host_years, novel_eed, max_evidence_sequence
                    FROM readiness_source_run
                    WHERE source_key = ? AND reservoir_id = ? AND lease_id = ?
                    """,
                    (source_key, reservoir_id, lease_id),
                ).fetchone()
                novel_host_years = run_counts.get(run_key, 0)
                novel_eed_value = run_eed.get(run_key, Decimal("0"))
                max_sequence = run_max_sequence[run_key]
                if current_run is None:
                    self.connection.execute(
                        """
                        INSERT INTO readiness_source_run(
                            source_key, reservoir_id, lease_id,
                            novel_host_years, novel_eed, max_evidence_sequence
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            source_key,
                            reservoir_id,
                            lease_id,
                            novel_host_years,
                            format(novel_eed_value, "f"),
                            max_sequence,
                        ),
                    )
                else:
                    self.connection.execute(
                        """
                        UPDATE readiness_source_run
                        SET novel_host_years = ?,
                            novel_eed = ?,
                            max_evidence_sequence = ?
                        WHERE source_key = ? AND reservoir_id = ? AND lease_id = ?
                        """,
                        (
                            int(current_run["novel_host_years"])
                            + novel_host_years,
                            format(
                                Decimal(str(current_run["novel_eed"]))
                                + novel_eed_value,
                                "f",
                            ),
                            max(
                                int(current_run["max_evidence_sequence"]),
                                max_sequence,
                            ),
                            source_key,
                            reservoir_id,
                            lease_id,
                        ),
                    )
            for task_kind in sorted(task_kind_counts):
                current_kind = self.connection.execute(
                    """
                    SELECT novel_host_years, novel_eed
                    FROM readiness_task_kind WHERE task_kind = ?
                    """,
                    (task_kind,),
                ).fetchone()
                if current_kind is None:
                    self.connection.execute(
                        """
                        INSERT INTO readiness_task_kind(
                            task_kind, novel_host_years, novel_eed
                        ) VALUES (?, ?, ?)
                        """,
                        (
                            task_kind,
                            task_kind_counts[task_kind],
                            format(task_kind_eed[task_kind], "f"),
                        ),
                    )
                else:
                    self.connection.execute(
                        """
                        UPDATE readiness_task_kind
                        SET novel_host_years = ?, novel_eed = ?
                        WHERE task_kind = ?
                        """,
                        (
                            int(current_kind["novel_host_years"])
                            + task_kind_counts[task_kind],
                            format(
                                Decimal(str(current_kind["novel_eed"]))
                                + task_kind_eed[task_kind],
                                "f",
                            ),
                            task_kind,
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
        baseline_id: str = "",
        authority_digest: str = "",
        dispatch_threshold: Decimal = DEFAULT_DISPATCH_THRESHOLD,
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
        task_kind_attribution = {
            str(row["task_kind"]): {
                "novel_host_years": int(row["novel_host_years"]),
                "novel_eed": str(row["novel_eed"]),
            }
            for row in self.connection.execute(
                "SELECT * FROM readiness_task_kind ORDER BY task_kind"
            )
        }
        source_run_attribution = {
            "|".join(
                (
                    str(row["source_key"]),
                    str(row["reservoir_id"]),
                    str(row["lease_id"]),
                )
            ): {
                "source_key": str(row["source_key"]),
                "reservoir_id": str(row["reservoir_id"]),
                "lease_id": str(row["lease_id"]),
                "novel_host_years": int(row["novel_host_years"]),
                "novel_eed": str(row["novel_eed"]),
                "max_evidence_sequence": int(row["max_evidence_sequence"]),
            }
            for row in self.connection.execute(
                """
                SELECT source_key, reservoir_id, lease_id,
                       novel_host_years, novel_eed, max_evidence_sequence
                FROM readiness_source_run
                ORDER BY source_key, reservoir_id, lease_id
                """
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
        direct = task_kind_attribution.get("direct", {
            "novel_host_years": 0,
            "novel_eed": "0",
        })
        verified_kinds = frozenset({"exact", "range", "domain"})
        verified_candidate_host_years = sum(
            int(payload["novel_host_years"])
            for kind, payload in task_kind_attribution.items()
            if kind in verified_kinds
        )
        verified_candidate_eed = sum(
            (
                Decimal(str(payload["novel_eed"]))
                for kind, payload in task_kind_attribution.items()
                if kind in verified_kinds
            ),
            Decimal("0"),
        )
        # Registration/DNS/reference or future provider task kinds do not
        # silently become annual web-presence contribution. Unknown non-direct
        # kinds stay restricted until an explicit reviewed lane mapping exists.
        restricted_host_years = sum(
            int(payload["novel_host_years"])
            for kind, payload in task_kind_attribution.items()
            if kind != "direct" and kind not in verified_kinds
        )
        restricted_eed = sum(
            (
                Decimal(str(payload["novel_eed"]))
                for kind, payload in task_kind_attribution.items()
                if kind != "direct" and kind not in verified_kinds
            ),
            Decimal("0"),
        )
        reconciliation = {
            "baseline_id": baseline_id,
            "baseline_eed": format(baseline_eed, "f"),
            "input_records": int(state["processed_host_years"]),
            "within_year_duplicates": 0,
            "invalid_records": 0,
            "baseline_overlap": int(state["processed_host_years"]) - int(state["novel_host_years"]),
            "novel_host_years": int(state["novel_host_years"]),
            "novel_eed": format(novel_eed, "f"),
            "growth_rate": format(growth_rate, "f"),
        }
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
            task_kind_attribution=task_kind_attribution,
            source_run_attribution=source_run_attribution,
            baseline_id=baseline_id,
            authority_digest=authority_digest,
            dispatch_threshold=format(dispatch_threshold, "f"),
            submission_dispatch_ready=(
                growth_rate >= dispatch_threshold
                and growth_rate >= FORMAL_GROWTH_RATE
            ),
            baseline_reconciliation=reconciliation,
            source_contribution={
                "by_source": source_attribution,
                "direct_annual": direct,
                "verified_candidate": {
                    "novel_host_years": verified_candidate_host_years,
                    "novel_eed": format(verified_candidate_eed, "f"),
                },
                "other_restricted": {
                    "novel_host_years": restricted_host_years,
                    "novel_eed": format(restricted_eed, "f"),
                },
            },
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
        baseline_eed: Decimal | str | int | None = None,
        authority_manifest: Path | dict[str, object] | AuthoritySnapshot | None = None,
        dispatch_threshold: Decimal | str | int = DEFAULT_DISPATCH_THRESHOLD,
        batch_size: int = 50_000,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        self.runtime_data_root = Path(runtime_data_root)
        self.baseline_path = Path(baseline_index)
        self.model_path = Path(eed_model)
        self.authority = _coerce_authority(authority_manifest)
        if self.authority is not None:
            actual_model_hash = eed_model_authority_signature(self.model_path)
            if actual_model_hash != self.authority.model_hash:
                raise ValueError("EED model does not match supplied authority manifest")
            self.baseline_eed = Decimal(self.authority.baseline_eed)
            if baseline_eed is not None and Decimal(str(baseline_eed)) != self.baseline_eed:
                raise ValueError("baseline_eed conflicts with supplied authority manifest")
        elif baseline_eed is not None:
            self.baseline_eed = Decimal(str(baseline_eed))
        else:
            raise ValueError("authority_manifest is required when baseline_eed is omitted")
        if not self.baseline_eed.is_finite() or self.baseline_eed < 0:
            raise ValueError("baseline_eed must be a finite non-negative decimal")
        self.dispatch_threshold = Decimal(str(dispatch_threshold))
        if not self.dispatch_threshold.is_finite() or not (
            FORMAL_GROWTH_RATE <= self.dispatch_threshold <= Decimal("1")
        ):
            raise ValueError("dispatch_threshold must be between 0.05 and 1")
        self.batch_size = int(batch_size)
        self.evidence = EvidenceStore(
            self.runtime_data_root / "evidence.sqlite3"
        )
        self.candidates = CandidateStore(
            self.runtime_data_root / "candidates.sqlite3"
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
        # Publish official EED weights immediately, even when there is no new
        # evidence batch yet. Pending evidence work can then be value-ranked
        # from service startup rather than waiting for the first readiness row.
        self._control_store()

    def _refresh_authority(self, *, force: bool = False) -> bool:
        if self.authority is not None:
            baseline_signature = self.authority.authority_digest
        else:
            baseline_signature = baseline_authority_signature(self.baseline_path)
        model_signature = eed_model_authority_signature(self.model_path)
        changed = (
            force
            or baseline_signature != self._baseline_signature
            or model_signature != self._model_signature
        )
        if not changed:
            return False

        if self.baseline is not None:
            self.baseline.close()
        self.baseline = BaselineIndex(self.baseline_path, authority=self.authority)
        self.weights = load_english_weights(self.model_path)
        self._baseline_signature = baseline_signature
        self._model_signature = model_signature
        self.ledger.ensure_authority(
            baseline_signature=baseline_signature,
            model_signature=model_signature,
        )
        if self.control is not None:
            self.control.set_eed_tld_weights(self.weights)
            SourceDiscoveryRegistry(self.control).set_scout_authority(
                baseline_signature=baseline_signature,
                model_signature=model_signature,
            )
        return True

    def _control_store(self) -> ControlStore | None:
        control_path = self.runtime_data_root / "control.sqlite3"
        if self.control is None and control_path.exists():
            self.control = ControlStore(control_path)
            # Persist the same official EED weights used by readiness so the
            # durable evidence queue can prioritize higher score value without
            # implementing a second weighting model.
            self.control.set_eed_tld_weights(self.weights)
            SourceDiscoveryRegistry(self.control).set_scout_authority(
                baseline_signature=self._baseline_signature,
                model_signature=self._model_signature,
            )
        return self.control

    def _resolved_attribution(
        self,
        rows: list[EvidenceHostYear],
    ) -> tuple[
        dict[tuple[str, int], str],
        dict[tuple[str, int], str],
        dict[tuple[str, int], tuple[str, str, str]],
    ]:
        """Resolve proof-valid task, source and source-run attribution.

        Run attribution is deliberately stricter than source attribution: a
        lease identity is accepted only when it is corroborated by proof-side
        provenance (provider tasks) or by a direct capsule whose source id
        matches the durable direct-origin row.
        """

        pairs = [(row.hostname, row.year) for row in rows]
        direct_sources = self.evidence.resolve_direct_source_origins(pairs)
        provider_provenance = self.evidence.resolve_provider_task_provenance(
            pairs
        )
        cutover = self.evidence.provider_task_provenance_cutover_sequence()
        control = self._control_store()
        control_kinds = (
            {}
            if control is None
            else control.resolve_host_year_task_kinds(pairs)
        )
        control_origins = (
            {}
            if control is None
            else control.resolve_primary_source_origins(pairs)
        )
        control_runs: dict[
            tuple[str, int], tuple[str, str, str]
        ] = {}
        if control is not None and pairs:
            limit = 350
            for offset in range(0, len(pairs), limit):
                chunk = pairs[offset : offset + limit]
                predicates = " OR ".join(
                    "(hostname = ? AND year = ?)" for _ in chunk
                )
                params: list[object] = []
                for hostname, year in chunk:
                    params.extend((hostname, year))
                for origin in control.connection.execute(
                    f"""
                    SELECT hostname, year, source_key, reservoir_id, lease_id
                    FROM evidence_host_year_origins
                    WHERE {predicates}
                    """,
                    params,
                ):
                    control_runs[
                        (str(origin["hostname"]), int(origin["year"]))
                    ] = (
                        str(origin["source_key"]),
                        str(origin["reservoir_id"]),
                        str(origin["lease_id"]),
                    )

        kinds: dict[tuple[str, int], str] = {}
        origins: dict[tuple[str, int], str] = {}
        run_origins: dict[
            tuple[str, int], tuple[str, str, str]
        ] = {}
        for row in rows:
            pair = (row.hostname, row.year)
            direct = direct_sources.get(pair, ())
            provider = provider_provenance.get(pair)
            cached_kind = control_kinds.get(pair)
            post_cutover = row.sequence > cutover

            selected_kind: str | None = None
            if cached_kind == "direct":
                if direct:
                    selected_kind = "direct"
            elif cached_kind is not None:
                if not post_cutover:
                    selected_kind = cached_kind
                elif provider is not None and provider.task_kind == cached_kind:
                    selected_kind = cached_kind

            if selected_kind is None:
                if direct:
                    selected_kind = "direct"
                elif provider is not None:
                    selected_kind = provider.task_kind

            if selected_kind is None:
                continue
            kinds[pair] = selected_kind

            cached_origin = control_origins.get(pair)
            cached_run = control_runs.get(pair)
            if selected_kind == "direct":
                if direct:
                    selected_source = (
                        cached_origin
                        if cached_origin in direct
                        else direct[0]
                    )
                    origins[pair] = selected_source
                    if (
                        cached_run is not None
                        and cached_run[0] == selected_source
                    ):
                        run_origins[pair] = cached_run
                continue

            if not post_cutover:
                if cached_origin is not None:
                    origins[pair] = cached_origin
                    if (
                        cached_run is not None
                        and cached_run[0] == cached_origin
                    ):
                        run_origins[pair] = cached_run
                elif provider is not None and provider.source_key:
                    origins[pair] = provider.source_key
                    if provider.reservoir_id and provider.lease_id:
                        run_origins[pair] = (
                            provider.source_key,
                            provider.reservoir_id,
                            provider.lease_id,
                        )
                continue

            # New provider evidence is credited only from provenance persisted
            # in the same EvidenceStore transaction as the proof capsule.
            if (
                provider is not None
                and provider.task_kind == selected_kind
                and provider.source_key
            ):
                origins[pair] = provider.source_key
                if provider.reservoir_id and provider.lease_id:
                    run_origins[pair] = (
                        provider.source_key,
                        provider.reservoir_id,
                        provider.lease_id,
                    )

        return kinds, origins, run_origins

    def _publish_evidence_action_rewards(
        self,
        report: IncrementalReadinessReport,
    ) -> None:
        """Close provider action proxy yield with formal readiness reward."""

        control = self._control_store()
        if control is None:
            return
        control.publish_evidence_action_final_rewards(
            report.task_kind_attribution,
            baseline_signature=report.baseline_signature,
            model_signature=report.model_signature,
        )

    def _invalidate_learning_rewards(self) -> None:
        """Fail cold while a changed baseline/model authority is rebuilding."""

        control = self._control_store()
        if control is None:
            return
        control.invalidate_evidence_action_final_rewards()
        SourceDiscoveryRegistry(control).reset_final_rewards()

    def _publish_source_rewards(
        self,
        report: IncrementalReadinessReport,
        *,
        reset: bool,
    ) -> None:
        """Compatibility path for pre-V5 sources with positive FINAL credit.

        Explicit-zero publication is reserved for registered source runs. This
        legacy path therefore never manufactures a zero from absence of
        attribution and never overwrites a V5 run aggregate.
        """
        control = self._control_store()
        if control is None:
            return
        registry = SourceDiscoveryRegistry(control)
        if reset:
            registry.reset_final_rewards()
        for source_key, payload in report.source_attribution.items():
            candidate = registry.get_candidate(source_key)
            if candidate is None:
                continue
            if registry.list_source_run_outcomes(source_key):
                continue
            final_eed = float(payload["novel_eed"])
            if final_eed <= 0:
                continue
            registry.record_final_reward(
                source_key,
                final_accepted_eed=final_eed,
                baseline_signature=report.baseline_signature,
                model_signature=report.model_signature,
            )

    def _max_source_run_evidence_sequence(
        self,
        run: SourceRunOutcome,
    ) -> int:
        """Return the highest durable host-year sequence attributable to a run."""
        control = self._control_store()
        if control is None:
            return 0

        pairs: set[tuple[str, int]] = set()
        for row in control.connection.execute(
            """
            SELECT hostname, year
            FROM evidence_host_year_origins
            WHERE source_key = ? AND reservoir_id = ? AND lease_id = ?
            """,
            (run.source_key, run.reservoir_id, run.lease_id),
        ):
            pairs.add((str(row["hostname"]), int(row["year"])))

        for row in self.evidence.connection.execute(
            """
            SELECT DISTINCT hostname, year
            FROM evidence_capsule_task_provenance
            WHERE source_key = ? AND reservoir_id = ? AND lease_id = ?
            """,
            (run.source_key, run.reservoir_id, run.lease_id),
        ):
            pairs.add((str(row["hostname"]), int(row["year"])))

        maximum = 0
        values = sorted(pairs)
        for offset in range(0, len(values), 350):
            chunk = values[offset : offset + 350]
            predicates = " OR ".join(
                "(hostname = ? AND year = ?)" for _ in chunk
            )
            params: list[object] = []
            for hostname, year in chunk:
                params.extend((hostname, year))
            row = self.evidence.connection.execute(
                f"""
                SELECT COALESCE(MAX(sequence), 0) AS max_sequence
                FROM evidence_host_years
                WHERE {predicates}
                """,
                params,
            ).fetchone()
            maximum = max(maximum, int(row["max_sequence"] or 0))
        return maximum

    def _source_run_operational_metrics(
        self,
        run: SourceRunOutcome,
    ) -> tuple[bool, int, int, int, int, float]:
        """Read lease/evidence closure and provider exposure from ControlStore."""
        control = self._control_store()
        if control is None:
            return False, 0, 0, 0, 0, 0.0

        lease = control.connection.execute(
            "SELECT state FROM work_leases WHERE lease_id = ? AND reservoir_id = ?",
            (run.lease_id, run.reservoir_id),
        ).fetchone()
        lease_terminal = (
            lease is not None
            and str(lease["state"])
            in {
                LeaseState.SUCCEEDED.value,
                LeaseState.ABORTED.value,
                LeaseState.EXPIRED.value,
            }
        )

        task = control.connection.execute(
            f"""
            SELECT
                COUNT(*) AS created,
                SUM(CASE WHEN e.state IN ({",".join("?" for _ in TERMINAL_STATES)})
                         THEN 1 ELSE 0 END) AS terminal
            FROM evidence_task_origins o
            JOIN evidence_tasks e
              ON e.hostname = o.hostname
             AND e.year_from = o.year_from
             AND e.year_to = o.year_to
             AND e.provider = o.provider
             AND e.policy_version = o.policy_version
            WHERE o.source_key = ?
              AND o.reservoir_id = ?
              AND o.lease_id = ?
            """,
            (*sorted(TERMINAL_STATES), run.source_key, run.reservoir_id, run.lease_id),
        ).fetchone()
        queued = int(task["created"] or 0)
        terminal = int(task["terminal"] or 0)

        staged = 0
        has_spillover = control.connection.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = 'evidence_route_backlog_v1'
            """
        ).fetchone()
        if has_spillover is not None:
            staged_row = control.connection.execute(
                """
                SELECT COUNT(*) AS n
                FROM evidence_route_backlog_v1
                WHERE source_key = ? AND reservoir_id = ? AND lease_id = ?
                """,
                (run.source_key, run.reservoir_id, run.lease_id),
            ).fetchone()
            staged = int(staged_row["n"] or 0)

        provider = control.connection.execute(
            """
            SELECT
                COALESCE(SUM(m.provider_requests), 0) AS provider_requests,
                COALESCE(SUM(m.provider_elapsed_milliseconds), 0) AS elapsed_ms
            FROM evidence_task_origins o
            JOIN evidence_task_attempt_metrics m
              ON m.hostname = o.hostname
             AND m.year_from = o.year_from
             AND m.year_to = o.year_to
             AND m.provider = o.provider
             AND m.policy_version = o.policy_version
             AND m.recorded_at >= o.first_observed_at
            WHERE o.source_key = ?
              AND o.reservoir_id = ?
              AND o.lease_id = ?
            """,
            (run.source_key, run.reservoir_id, run.lease_id),
        ).fetchone()

        direct = control.connection.execute(
            """
            SELECT COUNT(*) AS n
            FROM evidence_host_year_origins
            WHERE source_key = ?
              AND reservoir_id = ?
              AND lease_id = ?
              AND task_year_from IS NULL
            """,
            (run.source_key, run.reservoir_id, run.lease_id),
        ).fetchone()

        return (
            lease_terminal,
            queued + staged,
            terminal,
            int(direct["n"] or 0),
            int(provider["provider_requests"] or 0),
            float(provider["elapsed_ms"] or 0) / 1000.0,
        )

    def _reconcile_source_run_rewards(
        self,
        report: IncrementalReadinessReport,
    ) -> int:
        """Close registered V5 source runs only after all four FINAL gates."""
        control = self._control_store()
        if control is None:
            return 0
        registry = SourceDiscoveryRegistry(control)
        runs = registry.list_source_run_outcomes(
            baseline_signature=report.baseline_signature,
            model_signature=report.model_signature,
            closed_only=False,
        )
        closed = 0
        attribution = report.source_run_attribution or {}
        for run in runs:
            if run.closed:
                continue
            key = "|".join((run.source_key, run.reservoir_id, run.lease_id))
            payload = attribution.get(key, {})
            accepted_host_years = int(payload.get("novel_host_years", 0))
            accepted_eed = float(payload.get("novel_eed", 0.0))
            readiness_sequence = int(payload.get("max_evidence_sequence", 0))
            actual_sequence = self._max_source_run_evidence_sequence(run)
            (
                lease_terminal,
                evidence_created,
                evidence_terminal,
                direct_committed,
                provider_requests,
                provider_elapsed_seconds,
            ) = self._source_run_operational_metrics(run)

            validation_complete = (
                lease_terminal
                and evidence_created == evidence_terminal
                and report.evidence_cursor >= actual_sequence
                and readiness_sequence >= actual_sequence
            )
            # A zero-proof run has no per-run readiness row. In that case
            # readiness_sequence=actual_sequence=0 is the correct closed frontier.
            if actual_sequence == 0:
                validation_complete = (
                    lease_terminal
                    and evidence_created == evidence_terminal
                )

            registry.record_source_run_validation(
                run.source_key,
                reservoir_id=run.reservoir_id,
                lease_id=run.lease_id,
                baseline_signature=run.baseline_signature,
                model_signature=run.model_signature,
                evidence_tasks_created=evidence_created,
                evidence_tasks_terminal=evidence_terminal,
                direct_capsules_committed=direct_committed,
                provider_requests=provider_requests,
                provider_elapsed_seconds=provider_elapsed_seconds,
                accepted_host_years=accepted_host_years,
                final_accepted_eed=accepted_eed,
                max_evidence_sequence=actual_sequence,
                validation_complete=validation_complete,
            )
            if validation_complete and registry.close_source_run(
                run.source_key,
                reservoir_id=run.reservoir_id,
                lease_id=run.lease_id,
                baseline_signature=run.baseline_signature,
                model_signature=run.model_signature,
            ):
                closed += 1
        return closed

    def sync_once(self) -> IncrementalReadinessReport:
        authority_changed = self._refresh_authority()
        if authority_changed:
            # A prefix of a full authority rebuild is not a final reward. Drop
            # the old formal policy immediately and stay on bootstrap behavior
            # until every durable host-year has been re-evaluated.
            self._invalidate_learning_rewards()

        cursor = self.ledger.cursor()
        rows = self.evidence.host_years_after(
            cursor,
            limit=self.batch_size,
        )
        if rows:
            assert self.baseline is not None
            # Resolve candidate state from proof already durable in EvidenceStore
            # before advancing readiness. CandidateStore is a separate DB, so
            # this remains idempotent without a cross-database transaction.
            self.candidates.mark_annual_evidence_obtained_many(
                row.hostname for row in rows
            )
            (
                task_kinds,
                source_origins,
                run_origins,
            ) = self._resolved_attribution(rows)
            self.ledger.apply_batch(
                rows,
                baseline=self.baseline,
                weights=self.weights,
                source_origins=source_origins,
                task_kinds=task_kinds,
                run_origins=run_origins,
            )
        report = self.ledger.report(
            latest_evidence_sequence=self.evidence.max_host_year_sequence(),
            baseline_eed=self.baseline_eed,
            baseline_id=(self.authority.baseline_id if self.authority else ""),
            authority_digest=(self.authority.authority_digest if self.authority else ""),
            dispatch_threshold=self.dispatch_threshold,
        )
        self._reconcile_source_run_rewards(report)
        if report.evidence_cursor >= report.latest_evidence_sequence:
            # Provider-action and legacy source projections still require one
            # globally coherent readiness snapshot. V5 run closure above is
            # stricter: it is allowed only when that run's own sequence frontier
            # has been consumed and all attributable work is terminal.
            self._publish_source_rewards(
                report,
                reset=False,
            )
            self._publish_evidence_action_rewards(report)
        return report

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
        IncrementalReadinessRuntime.write_payload_atomic(report.as_dict(), path)

    @staticmethod
    def write_payload_atomic(
        payload: dict[str, object],
        path: Path,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
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
        self.candidates.close()
        self.evidence.close()

    def __enter__(self) -> "IncrementalReadinessRuntime":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
