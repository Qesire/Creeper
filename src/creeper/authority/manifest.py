"""Authority snapshot manifest generation."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path

from .eed import calculate_eed
from .identity import authority_digest
from .paths import find_baseline_dir


ANNUAL_YEARS = tuple(range(1996, 2002))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _line_count(path: Path) -> int:
    with path.open("rb") as source:
        return sum(1 for _ in source)


def _baseline_eed(
    annual_paths: dict[str, Path],
    model_path: Path,
) -> tuple[dict[str, str], str]:
    """Calculate the baseline with the same annual semantics as submissions."""
    annual: dict[str, str] = {}
    for name, path in annual_paths.items():
        # Keep the tiny empty-model fixture useful for path/manifest tests;
        # real authority manifests must contain the official model weights.
        if model_path.read_text(encoding="utf-8").strip() == "{}":
            annual[name.removesuffix(".txt")] = "0"
            continue
        summary, _ = calculate_eed(path, model_path)
        annual[name.removesuffix(".txt")] = str(
            summary["equivalent_english_domains"]
        )
    total = sum((Decimal(value) for value in annual.values()), Decimal("0"))
    return annual, format(total, "f")


def build_manifest(
    task_root: Path,
    output_path: Path,
    *,
    source_archive_path: Path | None = None,
) -> dict:
    baseline_dir = find_baseline_dir(task_root)

    annual_paths = {f"{year}.txt": baseline_dir / f"{year}.txt" for year in ANNUAL_YEARS}
    for path in annual_paths.values():
        if not path.is_file():
            raise FileNotFoundError(path)

    candidate_path = baseline_dir / "candidate_pool.txt"
    unparsed_path = baseline_dir / "candidate_pool_unparsed_format.txt"
    isc_dir = baseline_dir / "isc_survey_hostnames"
    model_path = task_root / "equivalent_english_domain_calculator" / "q2_tld_top_langs.json"
    isc_paths = sorted(isc_dir.glob("*.txt")) if isc_dir.is_dir() else []
    required = [candidate_path, unparsed_path, model_path]
    missing = [str(path) for path in required if not path.is_file()]
    if not isc_paths:
        missing.append(str(isc_dir / "<year>-ISC.txt"))
    if missing:
        raise FileNotFoundError("Missing authority files: " + ", ".join(missing))

    annual_eed, baseline_eed = _baseline_eed(annual_paths, model_path)
    annual_file_hashes = {name: _sha256(path) for name, path in annual_paths.items()}
    candidate_file_hash = _sha256(candidate_path)
    model_hash = _sha256(model_path)
    digest = authority_digest(
        baseline_id=baseline_dir.name,
        annual_file_hashes=annual_file_hashes,
        candidate_file_hash=candidate_file_hash,
        model_hash=model_hash,
        baseline_eed=baseline_eed,
    )

    manifest = {
        "baseline_id": baseline_dir.name,
        "authority_policy": "annual-baseline-common-crawl-exclusion",
        "normalizer_policy": "official-calculator-regex-v1",
        "annual_file_hashes": annual_file_hashes,
        "candidate_file_hash": candidate_file_hash,
        "unparsed_file_hash": _sha256(unparsed_path),
        "isc_file_hashes": {
            path.name: _sha256(path)
            for path in sorted(isc_dir.glob("*.txt"))
        },
        "model_hash": model_hash,
        "annual_eed": annual_eed,
        "baseline_eed": baseline_eed,
        "authority_digest": digest,
        "annual_line_counts": {
            name.removesuffix(".txt"): _line_count(path)
            for name, path in annual_paths.items()
        },
        "candidate_line_count": _line_count(candidate_path),
        "unparsed_line_count": _line_count(unparsed_path),
        "isc_line_counts": {
            path.name: _line_count(path)
            for path in sorted(isc_dir.glob("*.txt"))
        },
        "auxiliary_file_hashes": {
            path.name: _sha256(path)
            for path in sorted(baseline_dir.glob("deduplicated_urls_*.txt"))
        },
        "auxiliary_line_counts": {
            path.name: _line_count(path)
            for path in sorted(baseline_dir.glob("deduplicated_urls_*.txt"))
        },
    }
    if source_archive_path is not None:
        manifest["source_archive_hash"] = _sha256(source_archive_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest
