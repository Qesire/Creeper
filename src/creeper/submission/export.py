"""INTERNAL / LEGACY / NON-FORMAL submission export compatibility module.

Formal submission archives must be built with
``creeper.submission.exporter.build_submission_zip``.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.policies import EvidenceCapsule

EXPORTER_STATUS = "INTERNAL/LEGACY/NON-FORMAL"


def export_submission(
    capsules: Iterable[EvidenceCapsule],
    index: BaselineIndex,
    output_dir: Path,
    *,
    contributor: str = "local",
    submission_time: datetime | None = None,
) -> Path:
    """Reject legacy export calls so they cannot produce a formal package."""
    raise RuntimeError(
        "export_submission is INTERNAL/LEGACY/NON-FORMAL; "
        "use build_submission_zip for formal submissions"
    )
