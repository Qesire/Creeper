"""Offline-first residual-population search ledger and deterministic scheduler.

This module is deliberately independent from any search engine.  It owns the
durable *coverage* state that decides what should be searched next; concrete
Web/academic/repository providers only execute the returned QueryPlan.

The core unit is a SearchCell:

    mechanism x institution x period x artifact

This prevents synonym churn from masquerading as exploration and lets Creeper
stop searching a neighbourhood once its results are dominated by already-known
source families.
"""

from __future__ import annotations

import hashlib
import math
import sqlite3
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import Callable, Iterable, Sequence


class SearchCellState(StrEnum):
    OPEN = "OPEN"
    ACTIVE = "ACTIVE"
    SATURATED = "SATURATED"
    EXHAUSTED = "EXHAUSTED"


_MECHANISM_PHRASES: dict[str, tuple[str, ...]] = {
    "proxy_access": ("proxy trace", "HTTP access log", "cache trace"),
    "client_trace": ("web client trace", "HTTP client trace"),
    "dns_survey": ("DNS survey", "host survey", "zone inventory"),
    "ftp": ("FTP site list", "anonymous FTP list"),
    "bbs_telnet": ("BBS list", "telnet hosts"),
    "gopher": ("Gopher directory", "Gopher server list"),
    "mail": ("mail archive URLs", "mailing list URLs"),
    "usenet": ("Usenet URLs", "Usenet hostname list"),
    "search_engine": ("search engine index", "crawler seed list"),
    "crawler_frontier": ("crawler frontier", "crawl seed list"),
    "human_directory": ("web directory", "Internet directory"),
    "link_graph": ("hyperlink graph", "web link list"),
    "nic_registry": ("NIC host list", "registry host list"),
    "isp_inventory": ("ISP host inventory", "network host list"),
    "software_mirror": ("software mirror list", "mirror site list"),
}

_INSTITUTION_PHRASES: dict[str, str] = {
    "university": "university",
    "research_lab": "research laboratory",
    "isp": "ISP",
    "nren": "research network",
    "nic": "network information center",
    "government": "government",
    "software_archive": "software archive",
    "conference_project": "research project",
    "commercial": "commercial",
}

_ARTIFACT_PHRASES: dict[str, str] = {
    "log": "log",
    "trace": "trace dataset",
    "dump": "data dump",
    "list": "host list",
    "index": "index",
    "database": "database",
    "catalog": "catalog",
    "supplement": "supplementary data",
    "archive": "tar zip archive",
    "directory": "FTP directory",
    "cdrom": "CD-ROM data",
    "companion": "paper companion data",
}


@dataclass(frozen=True, slots=True)
class SearchCell:
    mechanism: str
    institution: str
    period: str
    artifact: str

    def __post_init__(self) -> None:
        for name, value in (
            ("mechanism", self.mechanism),
            ("institution", self.institution),
            ("period", self.period),
            ("artifact", self.artifact),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        if self.mechanism not in _MECHANISM_PHRASES:
            raise ValueError(f"unsupported mechanism: {self.mechanism}")
        if self.institution not in _INSTITUTION_PHRASES:
            raise ValueError(f"unsupported institution: {self.institution}")
        if self.artifact not in _ARTIFACT_PHRASES:
            raise ValueError(f"unsupported artifact: {self.artifact}")
        if not _valid_period(self.period):
            raise ValueError("period must overlap 1996-2001")

    @property
    def key(self) -> str:
        raw = "\x1f".join(
            (self.mechanism, self.institution, self.period, self.artifact)
        ).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()[:24]


@dataclass(frozen=True, slots=True)
class QueryPlan:
    cell: SearchCell
    query: str
    variant: int
    exclusions: tuple[str, ...]
    score: float


@dataclass(frozen=True, slots=True)
class SearchCellStats:
    cell: SearchCell
    state: SearchCellState
    attempts: int
    result_count: int
    duplicate_results: int
    unique_roots: int
    new_families: int
    qualified_roots: int
    accepted_novel_eed: float
    search_cost_seconds: float

    @property
    def duplicate_fraction(self) -> float:
        if self.result_count <= 0:
            return 0.0
        return min(1.0, self.duplicate_results / self.result_count)

    @property
    def new_family_fraction(self) -> float:
        if self.result_count <= 0:
            return 0.0
        return min(1.0, self.new_families / self.result_count)


@dataclass(frozen=True, slots=True)
class ResidualSearchPolicy:
    saturation_min_attempts: int = 3
    saturation_min_results: int = 30
    saturation_duplicate_fraction: float = 0.90
    saturation_max_new_family_fraction: float = 0.05
    max_exclusions: int = 8
    exclusion_min_hits: int = 2
    exploration_weight: float = 1.0
    novelty_weight: float = 1.5
    residual_weight: float = 2.0

    def __post_init__(self) -> None:
        if self.saturation_min_attempts < 1:
            raise ValueError("saturation_min_attempts must be positive")
        if self.saturation_min_results < 1:
            raise ValueError("saturation_min_results must be positive")
        if not 0 <= self.saturation_duplicate_fraction <= 1:
            raise ValueError("saturation_duplicate_fraction must be within [0,1]")
        if not 0 <= self.saturation_max_new_family_fraction <= 1:
            raise ValueError("saturation_max_new_family_fraction must be within [0,1]")
        if self.max_exclusions < 0 or self.exclusion_min_hits < 1:
            raise ValueError("invalid exclusion policy")


def _valid_period(period: str) -> bool:
    text = period.strip()
    if len(text) == 4 and text.isdigit():
        return 1996 <= int(text) <= 2001
    if "-" not in text:
        return False
    left, right = text.split("-", 1)
    if not (left.isdigit() and right.isdigit()):
        return False
    start, end = int(left), int(right)
    return start <= end and start <= 2001 and end >= 1996


def _normalized_family_key(value: str) -> str:
    return " ".join(value.lower().split())


class ResidualSearchLedger:
    """Durable coverage ledger; safe across process exits and offline periods."""

    def __init__(
        self,
        connection: sqlite3.Connection,
        *,
        clock: Callable[[], float] = time.time,
        policy: ResidualSearchPolicy | None = None,
    ) -> None:
        self.connection = connection
        self.clock = clock
        self.policy = policy or ResidualSearchPolicy()
        self._initialize()

    def _initialize(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS residual_search_cells (
                cell_key TEXT PRIMARY KEY,
                mechanism TEXT NOT NULL,
                institution TEXT NOT NULL,
                period TEXT NOT NULL,
                artifact TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'OPEN',
                attempts INTEGER NOT NULL DEFAULT 0,
                result_count INTEGER NOT NULL DEFAULT 0,
                duplicate_results INTEGER NOT NULL DEFAULT 0,
                unique_roots INTEGER NOT NULL DEFAULT 0,
                new_families INTEGER NOT NULL DEFAULT 0,
                qualified_roots INTEGER NOT NULL DEFAULT 0,
                accepted_novel_eed REAL NOT NULL DEFAULT 0,
                search_cost_seconds REAL NOT NULL DEFAULT 0,
                variant_cursor INTEGER NOT NULL DEFAULT 0,
                last_searched_at REAL,
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS residual_search_cell_families (
                cell_key TEXT NOT NULL,
                family_key TEXT NOT NULL,
                hit_count INTEGER NOT NULL DEFAULT 0,
                last_seen_at REAL NOT NULL,
                PRIMARY KEY (cell_key, family_key),
                FOREIGN KEY (cell_key)
                    REFERENCES residual_search_cells(cell_key)
                    ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_residual_search_cells_state
                ON residual_search_cells(state, attempts, updated_at);
            CREATE INDEX IF NOT EXISTS idx_residual_search_family_hits
                ON residual_search_cell_families(cell_key, hit_count DESC);
            """
        )

    def ensure_cell(self, cell: SearchCell) -> None:
        now = float(self.clock())
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO residual_search_cells(
                    cell_key, mechanism, institution, period, artifact, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(cell_key) DO NOTHING
                """,
                (
                    cell.key,
                    cell.mechanism,
                    cell.institution,
                    cell.period,
                    cell.artifact,
                    now,
                ),
            )

    def ensure_cells(self, cells: Iterable[SearchCell]) -> None:
        for cell in cells:
            self.ensure_cell(cell)

    def stats(self, cell: SearchCell) -> SearchCellStats:
        self.ensure_cell(cell)
        row = self.connection.execute(
            "SELECT * FROM residual_search_cells WHERE cell_key = ?",
            (cell.key,),
        ).fetchone()
        if row is None:
            raise RuntimeError("search cell disappeared after ensure")
        return SearchCellStats(
            cell=cell,
            state=SearchCellState(str(row["state"])),
            attempts=int(row["attempts"]),
            result_count=int(row["result_count"]),
            duplicate_results=int(row["duplicate_results"]),
            unique_roots=int(row["unique_roots"]),
            new_families=int(row["new_families"]),
            qualified_roots=int(row["qualified_roots"]),
            accepted_novel_eed=float(row["accepted_novel_eed"]),
            search_cost_seconds=float(row["search_cost_seconds"]),
        )

    def list_stats(
        self,
        *,
        states: Sequence[SearchCellState] | None = None,
    ) -> list[SearchCellStats]:
        sql = "SELECT * FROM residual_search_cells"
        params: list[object] = []
        if states:
            placeholders = ",".join("?" for _ in states)
            sql += f" WHERE state IN ({placeholders})"
            params.extend(state.value for state in states)
        rows = self.connection.execute(sql, params).fetchall()
        result: list[SearchCellStats] = []
        for row in rows:
            cell = SearchCell(
                mechanism=str(row["mechanism"]),
                institution=str(row["institution"]),
                period=str(row["period"]),
                artifact=str(row["artifact"]),
            )
            result.append(
                SearchCellStats(
                    cell=cell,
                    state=SearchCellState(str(row["state"])),
                    attempts=int(row["attempts"]),
                    result_count=int(row["result_count"]),
                    duplicate_results=int(row["duplicate_results"]),
                    unique_roots=int(row["unique_roots"]),
                    new_families=int(row["new_families"]),
                    qualified_roots=int(row["qualified_roots"]),
                    accepted_novel_eed=float(row["accepted_novel_eed"]),
                    search_cost_seconds=float(row["search_cost_seconds"]),
                )
            )
        return result

    def exclusions(self, cell: SearchCell) -> tuple[str, ...]:
        rows = self.connection.execute(
            """
            SELECT family_key
            FROM residual_search_cell_families
            WHERE cell_key = ? AND hit_count >= ?
            ORDER BY hit_count DESC, family_key
            LIMIT ?
            """,
            (
                cell.key,
                self.policy.exclusion_min_hits,
                self.policy.max_exclusions,
            ),
        ).fetchall()
        return tuple(str(row["family_key"]) for row in rows)

    def record_episode(
        self,
        cell: SearchCell,
        *,
        result_count: int,
        duplicate_results: int,
        unique_roots: int,
        new_families: int,
        qualified_roots: int,
        family_keys: Iterable[str] = (),
        accepted_novel_eed: float = 0.0,
        search_cost_seconds: float = 0.0,
    ) -> SearchCellStats:
        values = (
            result_count,
            duplicate_results,
            unique_roots,
            new_families,
            qualified_roots,
        )
        if any(isinstance(value, bool) or value < 0 for value in values):
            raise ValueError("episode counts must be non-negative integers")
        if duplicate_results > result_count or new_families > result_count:
            raise ValueError("duplicate/new-family counts cannot exceed results")
        if accepted_novel_eed < 0 or not math.isfinite(accepted_novel_eed):
            raise ValueError("accepted_novel_eed must be finite and non-negative")
        if search_cost_seconds < 0 or not math.isfinite(search_cost_seconds):
            raise ValueError("search_cost_seconds must be finite and non-negative")

        self.ensure_cell(cell)
        now = float(self.clock())
        families = {
            _normalized_family_key(item)
            for item in family_keys
            if isinstance(item, str) and item.strip()
        }
        with self.connection:
            self.connection.execute(
                """
                UPDATE residual_search_cells
                SET state = 'ACTIVE',
                    attempts = attempts + 1,
                    result_count = result_count + ?,
                    duplicate_results = duplicate_results + ?,
                    unique_roots = unique_roots + ?,
                    new_families = new_families + ?,
                    qualified_roots = qualified_roots + ?,
                    accepted_novel_eed = accepted_novel_eed + ?,
                    search_cost_seconds = search_cost_seconds + ?,
                    variant_cursor = variant_cursor + 1,
                    last_searched_at = ?,
                    updated_at = ?
                WHERE cell_key = ?
                """,
                (
                    result_count,
                    duplicate_results,
                    unique_roots,
                    new_families,
                    qualified_roots,
                    float(accepted_novel_eed),
                    float(search_cost_seconds),
                    now,
                    now,
                    cell.key,
                ),
            )
            for family in families:
                self.connection.execute(
                    """
                    INSERT INTO residual_search_cell_families(
                        cell_key, family_key, hit_count, last_seen_at
                    ) VALUES (?, ?, 1, ?)
                    ON CONFLICT(cell_key, family_key) DO UPDATE SET
                        hit_count = hit_count + 1,
                        last_seen_at = excluded.last_seen_at
                    """,
                    (cell.key, family, now),
                )

        stats = self.stats(cell)
        if self._should_saturate(stats):
            with self.connection:
                self.connection.execute(
                    """
                    UPDATE residual_search_cells
                    SET state = 'SATURATED', updated_at = ?
                    WHERE cell_key = ?
                    """,
                    (now, cell.key),
                )
            stats = self.stats(cell)
        return stats

    def mark_exhausted(self, cell: SearchCell) -> None:
        self.ensure_cell(cell)
        with self.connection:
            self.connection.execute(
                """
                UPDATE residual_search_cells
                SET state = 'EXHAUSTED', updated_at = ?
                WHERE cell_key = ?
                """,
                (float(self.clock()), cell.key),
            )

    def _should_saturate(self, stats: SearchCellStats) -> bool:
        policy = self.policy
        return (
            stats.attempts >= policy.saturation_min_attempts
            and stats.result_count >= policy.saturation_min_results
            and stats.duplicate_fraction >= policy.saturation_duplicate_fraction
            and stats.new_family_fraction <= policy.saturation_max_new_family_fraction
        )

    def variant_cursor(self, cell: SearchCell) -> int:
        self.ensure_cell(cell)
        row = self.connection.execute(
            "SELECT variant_cursor FROM residual_search_cells WHERE cell_key = ?",
            (cell.key,),
        ).fetchone()
        return int(row["variant_cursor"])


class SearchCellScheduler:
    """Choose relevant, non-saturated cells and emit bounded query variants."""

    def __init__(
        self,
        ledger: ResidualSearchLedger,
        *,
        policy: ResidualSearchPolicy | None = None,
    ) -> None:
        self.ledger = ledger
        self.policy = policy or ledger.policy

    def score(self, stats: SearchCellStats) -> float:
        if stats.state in {SearchCellState.SATURATED, SearchCellState.EXHAUSTED}:
            return float("-inf")
        exploration = 1.0 / math.sqrt(1.0 + stats.attempts)
        novelty = 1.0 - stats.duplicate_fraction
        if stats.search_cost_seconds > 0 and stats.accepted_novel_eed > 0:
            residual = stats.accepted_novel_eed / stats.search_cost_seconds
        elif stats.result_count > 0:
            residual = stats.qualified_roots / stats.result_count
        else:
            residual = 0.5
        return (
            self.policy.exploration_weight * exploration
            + self.policy.novelty_weight * novelty
            + self.policy.residual_weight * residual
        )

    def next_plans(self, *, limit: int = 1) -> tuple[QueryPlan, ...]:
        if isinstance(limit, bool) or limit < 1:
            raise ValueError("limit must be a positive integer")
        remaining = self.ledger.list_stats(
            states=(SearchCellState.OPEN, SearchCellState.ACTIVE)
        )
        selected: list[SearchCellStats] = []
        mechanism_counts: dict[str, int] = {}
        institution_counts: dict[str, int] = {}

        # Greedy diversity is batch-local only. It prevents a two-slot search
        # cycle from spending both requests on near-identical unexplored cells,
        # while a genuinely productive cell can still win when its measured
        # residual score exceeds the bounded diversity penalty.
        while remaining and len(selected) < limit:
            def adjusted(stats: SearchCellStats) -> tuple[float, float, int, str]:
                base = self.score(stats)
                mechanism_penalty = 0.75 * mechanism_counts.get(
                    stats.cell.mechanism, 0
                )
                institution_penalty = 0.20 * institution_counts.get(
                    stats.cell.institution, 0
                )
                return (
                    base - mechanism_penalty - institution_penalty,
                    base,
                    -stats.attempts,
                    stats.cell.key,
                )

            winner = max(remaining, key=adjusted)
            selected.append(winner)
            mechanism_counts[winner.cell.mechanism] = (
                mechanism_counts.get(winner.cell.mechanism, 0) + 1
            )
            institution_counts[winner.cell.institution] = (
                institution_counts.get(winner.cell.institution, 0) + 1
            )
            remaining.remove(winner)

        return tuple(self._query_plan(stats) for stats in selected)

    def _query_plan(self, stats: SearchCellStats) -> QueryPlan:
        cell = stats.cell
        mechanism_variants = _MECHANISM_PHRASES[cell.mechanism]
        cursor = self.ledger.variant_cursor(cell)
        variant = cursor % len(mechanism_variants)
        mechanism = mechanism_variants[variant]
        institution = _INSTITUTION_PHRASES[cell.institution]
        artifact = _ARTIFACT_PHRASES[cell.artifact]
        anchors = (
            f'"{cell.period}"',
            f'"{mechanism}"',
            institution,
            artifact,
            '(URL OR hostname OR host)',
        )
        exclusions = self.ledger.exclusions(cell)
        exclusion_text = " ".join(
            f'-"{family}"' for family in exclusions if len(family) <= 80
        )
        query = " ".join(anchors)
        if exclusion_text:
            query = f"{query} {exclusion_text}"
        return QueryPlan(
            cell=cell,
            query=query,
            variant=variant,
            exclusions=exclusions,
            score=self.score(stats),
        )


def default_search_cells() -> tuple[SearchCell, ...]:
    """Return a bounded, relevance-preserving seed space.

    This is intentionally not the full Cartesian product.  Each mechanism is
    paired only with institution/artifact combinations that can plausibly
    produce recoverable hostname or URL records.
    """

    profiles: tuple[tuple[str, tuple[str, ...], tuple[str, ...]], ...] = (
        ("proxy_access", ("university", "research_lab", "isp", "nren"), ("log", "trace")),
        ("client_trace", ("university", "research_lab", "isp"), ("trace", "companion")),
        ("dns_survey", ("nic", "nren", "research_lab", "isp"), ("dump", "list", "database")),
        ("ftp", ("university", "software_archive", "research_lab"), ("list", "directory")),
        ("bbs_telnet", ("university", "research_lab", "commercial"), ("list", "archive")),
        ("gopher", ("university", "research_lab", "government"), ("list", "directory")),
        ("mail", ("university", "research_lab", "conference_project"), ("archive", "dump")),
        ("usenet", ("university", "research_lab"), ("archive", "dump")),
        ("search_engine", ("commercial", "university", "research_lab"), ("index", "dump", "list")),
        ("crawler_frontier", ("university", "research_lab", "commercial"), ("dump", "list")),
        ("human_directory", ("university", "government", "commercial"), ("catalog", "database", "list")),
        ("link_graph", ("university", "research_lab"), ("database", "dump", "companion")),
        ("nic_registry", ("nic", "government", "nren"), ("dump", "list", "database")),
        ("isp_inventory", ("isp", "nren"), ("list", "database", "dump")),
        ("software_mirror", ("software_archive", "university", "research_lab"), ("list", "directory", "catalog")),
    )
    periods = ("1996", "1997", "1998", "1999", "2000", "2001")
    cells: list[SearchCell] = []
    for mechanism, institutions, artifacts in profiles:
        for institution in institutions:
            for period in periods:
                for artifact in artifacts:
                    cells.append(
                        SearchCell(
                            mechanism=mechanism,
                            institution=institution,
                            period=period,
                            artifact=artifact,
                        )
                    )
    return tuple(cells)
