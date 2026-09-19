"""Deterministic durable research-lead memory.

This module replaces the old LLM-facing source-research memory with a local
SQLite authority that can be consumed by deterministic residual search.

Three lead kinds are intentionally distinct:

* HARD_NEGATIVE: a known public artifact has irrecoverable hostname/URL
  identity loss and must never become a source proposal.
* EXACT_RECOVERY: prior research established a concrete historical identity
  (usually an exact filename/trace label); the existing structured provider
  stack should spend a small finite query program attempting to recover a live
  public copy.
* PROVENANCE_HOLD: a live-looking artifact is known, but its target-period
  provenance is unresolved; a matching result may be retained only in HOLD.

The ledger never grants evidence authority, novelty, or submission authority.
"""

from __future__ import annotations

import json
import re
import sqlite3
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import Callable

from creeper.source_discovery.residual_search import ResidualSearchLedger, SearchCell
from creeper.source_discovery.search_identity import CanonicalSearchResult


class ResearchLeadKind(StrEnum):
    HARD_NEGATIVE = "HARD_NEGATIVE"
    EXACT_RECOVERY = "EXACT_RECOVERY"
    PROVENANCE_HOLD = "PROVENANCE_HOLD"


@dataclass(frozen=True, slots=True)
class ResearchLeadDefinition:
    lead_id: str
    kind: ResearchLeadKind
    mechanism: str
    target_period: str
    identity: str
    instruction: str
    match_groups: tuple[tuple[str, ...], ...] = ()
    recovery_cell: SearchCell | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", ResearchLeadKind(self.kind))
        for name in (
            "lead_id",
            "mechanism",
            "target_period",
            "identity",
            "instruction",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be non-empty")
        object.__setattr__(
            self,
            "match_groups",
            tuple(tuple(group) for group in self.match_groups),
        )
        for group in self.match_groups:
            if not group or any(
                not isinstance(term, str) or not term.strip() for term in group
            ):
                raise ValueError("match_groups must contain non-empty string groups")
        if self.kind is ResearchLeadKind.EXACT_RECOVERY:
            if self.recovery_cell is None:
                raise ValueError("EXACT_RECOVERY requires a recovery_cell")
        elif self.recovery_cell is not None:
            raise ValueError("only EXACT_RECOVERY may define a recovery_cell")


CURATED_RESEARCH_LEADS: tuple[ResearchLeadDefinition, ...] = (
    ResearchLeadDefinition(
        lead_id="ucb-home-ip-1996-public-trace",
        kind=ResearchLeadKind.HARD_NEGATIVE,
        mechanism="client-side HTTP packet trace",
        target_period="1996-11",
        identity=(
            "public UCB Home IP trace irreversibly anonymizes destination server "
            "IP and request URL with keyed hashes"
        ),
        instruction=(
            "do not propose the public UCB Home IP trace as a hostname source"
        ),
        match_groups=(
            ("ucb", "home ip"),
            ("berkeley", "home ip"),
        ),
    ),
    ResearchLeadDefinition(
        lead_id="dec-proxy-v1.2-public-trace",
        kind=ResearchLeadKind.HARD_NEGATIVE,
        mechanism="proxy HTTP request trace",
        target_period="1996",
        identity=(
            "distributed DEC proxy trace replaces server names and URL "
            "components with opaque identifiers without a public reverse map"
        ),
        instruction=(
            "do not spend source budget on the anonymized distributed DEC trace"
        ),
        match_groups=(
            ("dec", "proxy trace"),
            ("digital", "proxy trace", "v1 2"),
        ),
    ),
    ResearchLeadDefinition(
        lead_id="nlanr-uc-20000714",
        kind=ResearchLeadKind.EXACT_RECOVERY,
        mechanism="sanitized Squid/proxy access log",
        target_period="2000-07-14",
        identity="published trace name uc.sanitized-access.20000714",
        instruction=(
            "recover a live public mirror and verify requested URL/server identity"
        ),
        match_groups=(("uc sanitized access 20000714",),),
        recovery_cell=SearchCell(
            mechanism="recover_nlanr_uc",
            institution="research_lab",
            period="2000",
            artifact="log",
        ),
    ),
    ResearchLeadDefinition(
        lead_id="canetii-19990919-20",
        kind=ResearchLeadKind.EXACT_RECOVERY,
        mechanism="sanitized Squid/proxy access log",
        target_period="1999-09-19/20",
        identity=(
            "published filenames access.1999-09-19.gz and access.1999-09-20.gz"
        ),
        instruction=(
            "recover public CA*netII rawlog mirrors and verify requested URL identity"
        ),
        match_groups=(
            ("access 1999 09 19 gz",),
            ("access 1999 09 20 gz",),
        ),
        recovery_cell=SearchCell(
            mechanism="recover_canetii",
            institution="nren",
            period="1999",
            artifact="log",
        ),
    ),
    ResearchLeadDefinition(
        lead_id="bu98flt",
        kind=ResearchLeadKind.EXACT_RECOVERY,
        mechanism="client-proxy HTTP request trace",
        target_period="1998-04-06/1998-05-21",
        identity="published trace label bu98flt",
        instruction=(
            "recover the actual BU98 filtered trace and inspect destination identity"
        ),
        match_groups=(("bu98flt",),),
        recovery_cell=SearchCell(
            mechanism="recover_bu98",
            institution="university",
            period="1998",
            artifact="trace",
        ),
    ),
    ResearchLeadDefinition(
        lead_id="dmoz-2001-content",
        kind=ResearchLeadKind.EXACT_RECOVERY,
        mechanism="human-curated web directory export",
        target_period="2001-01",
        identity="historical content.rdf.u8.gz dump",
        instruction=(
            "recover a provenance-safe January 2001 content.rdf.u8.gz copy"
        ),
        match_groups=(("content rdf u8 gz",),),
        recovery_cell=SearchCell(
            mechanism="recover_dmoz_2001",
            institution="software_archive",
            period="2001",
            artifact="dump",
        ),
    ),
    ResearchLeadDefinition(
        lead_id="ripe-isc-historical-hostcount",
        kind=ResearchLeadKind.EXACT_RECOVERY,
        mechanism="DNS hostcount/zone enumeration",
        target_period="1996-2001",
        identity=(
            "historical RIPE/ISC hostcount summaries are known; raw per-host "
            "output or mirrors are the useful reservoir"
        ),
        instruction=(
            "recover cited raw-output repositories, not aggregate count reports"
        ),
        match_groups=(
            ("ripe", "hostcount"),
            ("isc", "hostcount"),
        ),
        recovery_cell=SearchCell(
            mechanism="recover_ripe_hostcount",
            institution="nic",
            period="1996-2001",
            artifact="dump",
        ),
    ),
    ResearchLeadDefinition(
        lead_id="nus-nlanr-sample",
        kind=ResearchLeadKind.PROVENANCE_HOLD,
        mechanism="proxy trace teaching mirror",
        target_period="unknown",
        identity=(
            "live NUS mirror exposes NLANR teaching samples, but originating "
            "cache/date is not established"
        ),
        instruction=(
            "hold any matching artifact until its originating NLANR cache/date "
            "is resolved"
        ),
        match_groups=(
            ("nus", "nlanr"),
            ("national university of singapore", "nlanr"),
        ),
    ),
)

_LEADS_BY_ID = {lead.lead_id: lead for lead in CURATED_RESEARCH_LEADS}
_LEADS_BY_CELL = {
    lead.recovery_cell.key: lead
    for lead in CURATED_RESEARCH_LEADS
    if lead.recovery_cell is not None
}
_TOKEN_RE = re.compile(r"[a-z0-9]+")


def _normalize(value: str) -> str:
    return " ".join(_TOKEN_RE.findall(value.lower()))


def _result_text(result: CanonicalSearchResult) -> str:
    raw = result.raw
    return _normalize(
        " ".join(
            (
                result.canonical_url,
                raw.title,
                raw.description,
                raw.publisher,
                raw.resource_type,
                " ".join(raw.creators),
                " ".join(raw.identifiers),
            )
        )
    )


def _matches(lead: ResearchLeadDefinition, result: CanonicalSearchResult) -> bool:
    if not lead.match_groups:
        return False
    text = _result_text(result)
    for group in lead.match_groups:
        if all(_normalize(term) in text for term in group):
            return True
    return False


class ResearchLeadLedger:
    """Durable curated lead registry sharing the ControlStore SQLite authority."""

    def __init__(
        self,
        connection: sqlite3.Connection,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.connection = connection
        self.clock = clock
        self._initialize()

    def _initialize(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS source_research_leads_v1 (
                lead_id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                mechanism TEXT NOT NULL,
                target_period TEXT NOT NULL,
                identity_text TEXT NOT NULL,
                instruction TEXT NOT NULL,
                match_groups_json TEXT NOT NULL,
                cell_key TEXT,
                matched_count INTEGER NOT NULL DEFAULT 0,
                last_match_at REAL,
                last_source_key TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_source_research_leads_kind
                ON source_research_leads_v1(kind, lead_id);
            CREATE INDEX IF NOT EXISTS idx_source_research_leads_cell
                ON source_research_leads_v1(cell_key)
                WHERE cell_key IS NOT NULL;
            """
        )

    def seed_curated(self, residual: ResidualSearchLedger) -> int:
        if residual.connection is not self.connection:
            raise RuntimeError(
                "research-lead and residual ledgers must share one SQLite connection"
            )
        now = float(self.clock())
        inserted = 0
        with self.connection:
            for lead in CURATED_RESEARCH_LEADS:
                cell_key = (
                    None if lead.recovery_cell is None else lead.recovery_cell.key
                )
                inserted += int(
                    self.connection.execute(
                        """
                        INSERT OR IGNORE INTO source_research_leads_v1(
                            lead_id, kind, mechanism, target_period,
                            identity_text, instruction, match_groups_json,
                            cell_key, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            lead.lead_id,
                            lead.kind.value,
                            lead.mechanism,
                            lead.target_period,
                            lead.identity,
                            lead.instruction,
                            json.dumps(
                                lead.match_groups,
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ),
                            cell_key,
                            now,
                            now,
                        ),
                    ).rowcount
                )
                self.connection.execute(
                    """
                    UPDATE source_research_leads_v1
                    SET kind=?, mechanism=?, target_period=?,
                        identity_text=?, instruction=?, match_groups_json=?,
                        cell_key=?, updated_at=?
                    WHERE lead_id=?
                    """,
                    (
                        lead.kind.value,
                        lead.mechanism,
                        lead.target_period,
                        lead.identity,
                        lead.instruction,
                        json.dumps(
                            lead.match_groups,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                        cell_key,
                        now,
                        lead.lead_id,
                    ),
                )
                if lead.recovery_cell is not None:
                    residual.ensure_cell(lead.recovery_cell)
        return inserted

    def recovery_cell_keys(self) -> frozenset[str]:
        rows = self.connection.execute(
            """
            SELECT cell_key
            FROM source_research_leads_v1
            WHERE kind=? AND cell_key IS NOT NULL
            """,
            (ResearchLeadKind.EXACT_RECOVERY.value,),
        ).fetchall()
        return frozenset(str(row["cell_key"]) for row in rows)

    def lead_for_cell(self, cell_key: str) -> ResearchLeadDefinition | None:
        return _LEADS_BY_CELL.get(cell_key)

    def get(self, lead_id: str) -> ResearchLeadDefinition | None:
        return _LEADS_BY_ID.get(lead_id)

    def hard_negative_match(
        self,
        result: CanonicalSearchResult,
    ) -> ResearchLeadDefinition | None:
        for lead in CURATED_RESEARCH_LEADS:
            if (
                lead.kind is ResearchLeadKind.HARD_NEGATIVE
                and _matches(lead, result)
            ):
                return lead
        return None

    def provenance_hold_match(
        self,
        result: CanonicalSearchResult,
    ) -> ResearchLeadDefinition | None:
        for lead in CURATED_RESEARCH_LEADS:
            if (
                lead.kind is ResearchLeadKind.PROVENANCE_HOLD
                and _matches(lead, result)
            ):
                return lead
        return None

    def exact_recovery_match(
        self,
        cell_key: str,
        result: CanonicalSearchResult,
    ) -> ResearchLeadDefinition | None:
        lead = self.lead_for_cell(cell_key)
        if lead is None or lead.kind is not ResearchLeadKind.EXACT_RECOVERY:
            return None
        return lead if _matches(lead, result) else None

    def record_match_locked(
        self,
        lead_id: str,
        *,
        source_key: str | None = None,
    ) -> None:
        """Record a deterministic match inside an already-open transaction."""
        now = float(self.clock())
        changed = self.connection.execute(
            """
            UPDATE source_research_leads_v1
            SET matched_count = matched_count + 1,
                last_match_at = ?,
                last_source_key = COALESCE(?, last_source_key),
                updated_at = ?
            WHERE lead_id = ?
            """,
            (now, source_key, now, lead_id),
        ).rowcount
        if changed != 1:
            raise KeyError(f"unknown research lead: {lead_id}")

    def rows(self) -> tuple[sqlite3.Row, ...]:
        return tuple(
            self.connection.execute(
                "SELECT * FROM source_research_leads_v1 ORDER BY lead_id"
            ).fetchall()
        )
