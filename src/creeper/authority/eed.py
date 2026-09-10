"""Equivalent-English Domain calculation compatible with the official tool."""

from __future__ import annotations

import json
from collections import Counter
from decimal import Decimal
from collections.abc import Iterable
from pathlib import Path

from .normalizer import normalize_official


def load_english_weights(path: Path) -> dict[str, Decimal]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    weights: dict[str, Decimal] = {}
    for tld, language, percentage in zip(
        raw["tld"], raw["lang"], raw["perc_of_tld"], strict=True
    ):
        if tld and language == "eng":
            weights[str(tld).lower()] = Decimal(str(percentage)) / Decimal("100")
    if not weights:
        raise ValueError("The model contains no English TLD weights")
    return weights


def calculate_eed(path: Path, model_path: Path) -> tuple[dict, list[dict]]:
    values: set[str] = set()
    with path.open("r", encoding="utf-8", errors="replace", newline="") as source:
        for line in source:
            value = line.strip().lower()
            if value:
                values.add(value)
    return calculate_eed_values(values, model_path, input_file=str(path.resolve()))


def calculate_eed_values(
    values: Iterable[str],
    model_path: Path,
    *,
    input_file: str | None = None,
) -> tuple[dict, list[dict]]:
    """Calculate official EED semantics from an in-memory hostname stream."""
    weights = load_english_weights(model_path)
    unique_values = {value.strip().lower() for value in values if value.strip()}

    tld_counts: Counter[str] = Counter()
    invalid_records = 0
    for value in unique_values:
        normalized = normalize_official(value)
        if normalized is None:
            invalid_records += 1
        else:
            tld_counts[normalized.rsplit(".", 1)[-1]] += 1

    rows: list[dict] = []
    equivalent_total = Decimal("0")
    matched_records = 0
    for tld, count in sorted(tld_counts.items(), key=lambda item: (-item[1], item[0])):
        weight = weights.get(tld, Decimal("0"))
        equivalent = Decimal(count) * weight
        if tld in weights:
            matched_records += count
        equivalent_total += equivalent
        rows.append(
            {
                "tld": f".{tld}",
                "unique_valid_domains": count,
                "english_share": format(weight, "f"),
                "english_share_percent": format(weight * Decimal("100"), "f"),
                "equivalent_english_domains": format(equivalent, "f"),
                "model_status": "matched" if tld in weights else "unmatched",
            }
        )

    valid_records = len(unique_values) - invalid_records
    summary = {
        "method": (
            "Each unique normalized valid hostname contributes the English primary-page-"
            "language share of its right-most TLD from the CC-MAIN-2024-10 model. "
            "Invalid and unmatched records contribute zero."
        ),
        "input_file": input_file or "<in-memory>",
        "unique_nonempty_records": len(unique_values),
        "unique_valid_domains": valid_records,
        "invalid_records": invalid_records,
        "model_matched_records": matched_records,
        "model_unmatched_valid_records": valid_records - matched_records,
        "model_coverage_percent_of_valid_records": format(
            Decimal("100") * Decimal(matched_records) / Decimal(valid_records)
            if valid_records
            else Decimal("0"),
            ".6f",
        ),
        "equivalent_english_domains": format(equivalent_total, "f"),
    }
    return summary, rows
