"""Protocol-guarded residual-search calibration report facade.

The report is decision support for search calibration, so it must not silently
interpret pre-recovery coverage or a crash-stranded residual episode as a valid
completed finite query program.  The detailed aggregation implementation remains
in :mod:`residual_report_core`; this facade validates durable protocol state
before delegating.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from creeper.source_discovery import residual_report_core as _core
from creeper.source_discovery.residual_recovery import RESIDUAL_PROTOCOL_REVISION

for _name in dir(_core):
    if not _name.startswith("__"):
        globals()[_name] = getattr(_core, _name)

REPORT_VERSION = "residual-search-calibration-v2"


def _validate_protocol_state(connection: sqlite3.Connection) -> None:
    missing = sorted(_REQUIRED_TABLES - _table_names(connection))
    if missing:
        raise ResidualSearchReportError(
            "control database is missing residual-search tables: " + ", ".join(missing)
        )

    row = connection.execute(
        "SELECT value FROM residual_search_meta WHERE key='residual_protocol_revision'"
    ).fetchone()
    revision = None if row is None else str(row[0])
    if revision != RESIDUAL_PROTOCOL_REVISION:
        raise ResidualSearchReportError(
            "residual protocol recovery is required before calibration reporting: "
            f"expected {RESIDUAL_PROTOCOL_REVISION!r}, found {revision!r}"
        )

    unfinished = connection.execute(
        """
        SELECT COUNT(*)
        FROM source_search_episodes
        WHERE strategy LIKE 'RESIDUAL_CELL:%'
          AND finished_at IS NULL
        """
    ).fetchone()
    count = 0 if unfinished is None else int(unfinished[0])
    if count:
        raise ResidualSearchReportError(
            "residual protocol recovery is required before calibration reporting: "
            f"{count} unfinished residual search episode(s)"
        )


def build_residual_search_report(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    """Build a report only from protocol-current, fully recovered state."""

    _validate_protocol_state(connection)
    report = _core.build_residual_search_report(connection)
    report["report_version"] = REPORT_VERSION
    report["residual_protocol_revision"] = RESIDUAL_PROTOCOL_REVISION
    return report


def load_residual_search_report(path: Path) -> dict[str, Any]:
    """Open one control database read-only and build a guarded report."""

    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise ResidualSearchReportError(f"control database does not exist: {resolved}")
    connection = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True)
    try:
        return build_residual_search_report(connection)
    finally:
        connection.close()
