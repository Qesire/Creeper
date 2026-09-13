"""Independent annual EED and submission-gate metrics for readiness runs."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from creeper.authority.eed import calculate_eed_values
from creeper.authority.normalizer import normalize_official


ANNUAL_YEARS = tuple(range(1996, 2002))
DEFAULT_DISPATCH_THRESHOLD = Decimal("0.0525")


def _read_normalized(path: Path) -> set[str]:
    if not path.exists():
        return set()
    values: set[str] = set()
    with path.open("r", encoding="utf-8", errors="replace") as source:
        for line in source:
            value = normalize_official(line.strip())
            if value is not None:
                values.add(value)
    return values


def _fixed(value: Decimal, places: int = 10) -> str:
    return format(value, f".{places}f")


def build_readiness_report(
    *,
    accepted_dir: Path,
    baseline_dir: Path,
    model_path: Path,
    baseline_eed: Decimal | int | str,
    elapsed_seconds: Decimal | int | str,
    run_id: str,
    source_partition_seed: int = 0,
    code_revision: str | None = None,
    dispatch_threshold: Decimal | int | str = DEFAULT_DISPATCH_THRESHOLD,
) -> dict[str, object]:
    """Calculate annual novel EED from accepted annual files.

    The baseline subtraction is performed per year before EED calculation. This
    preserves the competition's host-year semantics: the same hostname in two
    annual files contributes once in each distinct year.
    """
    if not run_id.strip():
        raise ValueError("run_id must be non-empty")
    elapsed = Decimal(str(elapsed_seconds))
    if elapsed <= 0:
        raise ValueError("elapsed_seconds must be positive")
    baseline_total = Decimal(str(baseline_eed))
    if baseline_total < 0:
        raise ValueError("baseline_eed must be non-negative")
    dispatch = Decimal(str(dispatch_threshold))
    if not dispatch.is_finite() or not Decimal("0.05") <= dispatch <= Decimal("1"):
        raise ValueError("dispatch_threshold must be between 0.05 and 1")

    annual: dict[str, object] = {}
    total = Decimal("0")
    total_pairs = 0
    for year in ANNUAL_YEARS:
        accepted = _read_normalized(accepted_dir / f"{year}.txt")
        baseline = _read_normalized(baseline_dir / f"{year}.txt")
        novel = sorted(accepted - baseline)
        summary, rows = calculate_eed_values(
            novel,
            model_path,
            input_file=str(accepted_dir / f"{year}.txt"),
        )
        year_eed = Decimal(str(summary["equivalent_english_domains"]))
        total += year_eed
        total_pairs += len(novel)
        annual[str(year)] = {
            "accepted_pairs": len(accepted),
            "baseline_pairs": len(baseline),
            "novel_pairs": len(novel),
            "novel_eed": _fixed(year_eed),
            "eed_report": summary,
            "tld_breakdown": rows,
        }

    per_day = total * Decimal("86400") / elapsed
    five_percent = baseline_total * Decimal("0.05")
    eta = five_percent / per_day if per_day > 0 else None
    fraction = total / five_percent if five_percent > 0 else Decimal("0")
    return {
        "report_version": "eed-readiness-v1",
        "run_id": run_id,
        "track": "annual",
        "source_partition_seed": source_partition_seed,
        "code_revision": code_revision,
        "accepted_dir": str(accepted_dir),
        "baseline_dir": str(baseline_dir),
        "model_path": str(model_path),
        "elapsed_seconds": _fixed(elapsed),
        "accepted_annual_host_years": total_pairs,
        "annual_novel_eed": _fixed(total),
        "annual_eed_per_day": _fixed(per_day),
        "baseline_eed": format(baseline_total, "f"),
        "five_percent_delta": format(five_percent, "f"),
        "confirmed_fraction_of_five_percent": (
            "0" if fraction == 0 else _fixed(fraction)
        ),
        "formal_gate_reached": total / baseline_total >= Decimal("0.05")
        if baseline_total > 0 else False,
        "dispatch_threshold": format(dispatch, "f"),
        "submission_dispatch_ready": total / baseline_total >= dispatch
        if baseline_total > 0 else False,
        "eta_to_five_percent_days": None if eta is None else _fixed(eta),
        "annual": annual,
    }
