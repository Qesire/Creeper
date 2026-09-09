"""V3 authority snapshot manifest generation."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


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


def build_manifest(
    task_root: Path,
    output_path: Path,
    *,
    source_archive_path: Path | None = None,
) -> dict:
    baseline_dir = task_root / "merged260909-3"
    if not baseline_dir.is_dir():
        raise FileNotFoundError(f"Missing V3 baseline directory: {baseline_dir}")

    annual_paths = {f"{year}.txt": baseline_dir / f"{year}.txt" for year in ANNUAL_YEARS}
    for path in annual_paths.values():
        if not path.is_file():
            raise FileNotFoundError(path)

    candidate_path = baseline_dir / "candidate_pool.txt"
    unparsed_path = baseline_dir / "candidate_pool_unparsed_format.txt"
    isc_dir = baseline_dir / "isc_survey_hostnames"
    model_path = task_root / "equivalent_english_domain_calculator" / "q2_tld_top_langs.json"
    required = [candidate_path, unparsed_path, model_path, isc_dir / "1996-ISC.txt", isc_dir / "1997-ISC.txt"]
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise FileNotFoundError("Missing V3 authority files: " + ", ".join(missing))

    manifest = {
        "baseline_id": baseline_dir.name,
        "authority_policy": "v3-common-crawl-exclusion",
        "normalizer_policy": "official-calculator-regex-v1",
        "annual_file_hashes": {name: _sha256(path) for name, path in annual_paths.items()},
        "candidate_file_hash": _sha256(candidate_path),
        "unparsed_file_hash": _sha256(unparsed_path),
        "isc_file_hashes": {
            path.name: _sha256(path)
            for path in sorted(isc_dir.glob("*.txt"))
        },
        "model_hash": _sha256(model_path),
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
