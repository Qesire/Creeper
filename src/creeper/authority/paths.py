"""Locate the single annual baseline directory in a task package."""

from __future__ import annotations

from pathlib import Path


ANNUAL_YEARS = tuple(range(1996, 2002))


def find_baseline_dir(task_root: Path, *, require_annual: bool = True) -> Path:
    """Return the only ``merged*`` directory for a task package.

    Baseline construction requires all six annual files. Auxiliary adapters
    may opt out because their focused fixtures and some discovery packages
    contain only the non-authoritative auxiliary lists.
    """
    candidates = sorted(
        path
        for path in task_root.glob("merged*")
        if path.is_dir()
        and (
            not require_annual
            or all((path / f"{year}.txt").is_file() for year in ANNUAL_YEARS)
        )
    )
    if not candidates:
        suffix = " with annual files" if require_annual else ""
        raise FileNotFoundError(f"No merged baseline directory{suffix} found under {task_root}")
    if len(candidates) > 1:
        names = ", ".join(path.name for path in candidates)
        raise ValueError(f"Ambiguous merged baseline directories under {task_root}: {names}")
    return candidates[0]
