"""Formal submission archive exporter.

The compatibility entrypoint delegates to the bounded-memory streaming writer.
Production callers should pass production_config_path (or call
build_streaming_submission_zip directly with required artifact specs).
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from creeper.submission.artifact_manifest import ArtifactSpec
from creeper.submission.snapshot import SubmissionSnapshot
from creeper.submission.streaming_exporter import build_streaming_submission_zip


def build_submission_zip(
    snapshot: SubmissionSnapshot,
    name: str,
    output_dir: Path,
    *,
    source_root: Path,
    documentation_path: Path,
    production_config_path: Path | None = None,
    artifact_specs: Iterable[ArtifactSpec] = (),
) -> Path:
    specs = list(artifact_specs)
    if production_config_path is not None:
        specs.append(
            ArtifactSpec(
                logical_role="production_config",
                source_path=Path(production_config_path),
                archive_path=f"run/{Path(production_config_path).name}",
                required=True,
                # The explicit production_config_path argument is itself the
                # narrow policy opt-in when config lives outside source_root.
                allow_external=True,
            )
        )

    # SubmissionSnapshot is the legacy in-memory contract. Sort its already
    # materialized tuple only for compatibility. Runtime production should pass
    # EvidenceStore.iter_canonical_host_year_capsules() to the streaming API.
    records = iter(sorted(
        snapshot.novel_records,
        key=lambda item: (item.hostname, item.year),
    ))
    return build_streaming_submission_zip(
        snapshot,
        name,
        output_dir,
        source_root=source_root,
        documentation_path=documentation_path,
        evidence_records=records,
        artifact_specs=specs,
        require_production_config=production_config_path is not None,
    )
