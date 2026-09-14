"""Run a deterministic, local V5 full-loop acceptance canary.

The canary deliberately uses a synthetic source and evidence transport.  It
exercises the durable runtime and formal submission boundaries, but it says
nothing about network availability, competition throughput, or competition
score.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys
import time

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.identity import (
    AuthoritySnapshot,
    eed_model_authority_signature,
)
from creeper.evidence.providers.cdx import query_year
from creeper.evidence.policies import CDXQueryState, EvidenceQueryKey
from creeper.runtime.readiness import IncrementalReadinessRuntime
from creeper.runtime.source_producer import SourceProducer
from creeper.runtime.submission import (
    RuntimeSubmissionContext,
    build_runtime_snapshot,
    export_runtime_submission,
)
from creeper.scheduler.credits import CreditLedger
from creeper.scheduler.global_scheduler import GlobalScheduler
from creeper.scheduler.leases import WorkLease
from creeper.scheduler.priority import LeaseCandidate, ResourceCost
from creeper.source_discovery.activation import SourceActivationCompiler
from creeper.source_discovery.models import (
    MeasurementMode,
    ScoutMeasurement,
    SourceCandidate,
    SourceLevel,
    SourceState,
)
from creeper.source_discovery.production_value import ProductionValueModel
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.sources.domains import DomainState, SourceDomain
from creeper.sources.local.static_dataset import StaticDatasetAdapter
from creeper.sources.reservoirs import ReservoirState
from creeper.storage.candidate_store import CandidateStore
from creeper.storage.commit_writer import CommitWriter
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore, EvidenceTaskProvenance
from creeper.storage.telemetry_store import RuntimeTelemetryStore
from creeper.submission.artifact_manifest import ArtifactSpec


STAGES = (
    "discovery",
    "measurement",
    "activation",
    "production",
    "final_reward",
    "allocation_ranking_change",
    "formal_export",
    "independent_verifier",
)
_AUTHORITY_BASELINE_SIGNATURE = "authority_digest"
_PRIMARY_SOURCE = "https://synthetic.invalid/v5-canary/primary.txt"
_SECONDARY_SOURCE = "https://synthetic.invalid/v5-canary/secondary.txt"
_CANARY_SOURCE_HOSTS = tuple(f"canary-{index}.uk" for index in range(1, 6))


class CanaryCheckpointError(RuntimeError):
    """Raised when a formal canary checkpoint is absent or inconsistent."""


@dataclass(frozen=True)
class CanaryResult:
    stages: tuple[str, ...]
    archive_path: Path
    report_path: Path
    readiness_path: Path
    final_reward_eed: float
    ranking_before: tuple[str, ...]
    ranking_after: tuple[str, ...]
    verifier_ready: bool
    synthetic_only: bool = True

    def as_dict(self) -> dict[str, object]:
        payload = asdict(self)
        for key in ("archive_path", "report_path", "readiness_path"):
            payload[key] = str(payload[key])
        return payload


class _SyntheticSourceAdapter(StaticDatasetAdapter):
    """Finite provider with fixed latency so FINAL cost affects ranking."""

    def execute_stream(self, lease, emit_record):
        time.sleep(0.75)
        return super().execute_stream(lease, emit_record)


def _require_file(path: Path, label: str) -> Path:
    path = Path(path).expanduser().resolve()
    if not path.is_file():
        raise CanaryCheckpointError(f"{label} checkpoint is missing: {path}")
    return path


def _load_authority(
    *,
    baseline_manifest: Path,
    baseline_index: Path,
    eed_model: Path,
) -> tuple[AuthoritySnapshot, str]:
    manifest = _require_file(baseline_manifest, "baseline manifest")
    index_path = _require_file(baseline_index, "baseline index")
    model_path = _require_file(eed_model, "EED model")
    authority = AuthoritySnapshot.from_manifest_path(manifest)
    if eed_model_authority_signature(model_path) != authority.model_hash:
        raise CanaryCheckpointError("EED model does not match baseline authority")
    index = BaselineIndex(index_path, authority=authority)
    index.close()
    return authority, eed_model_authority_signature(model_path)


def validate_canary_checkpoints(
    *,
    runtime_root: Path,
    baseline_manifest: Path,
    baseline_index: Path,
    eed_model: Path,
    documentation: Path,
    output_dir: Path,
) -> None:
    """Require all durable inputs and completed runtime checkpoints.

    ``readiness.json`` and ``telemetry.sqlite3`` are produced by the canary
    only after the durable production path has completed.  They are checked
    again before the result is published so a partial run cannot look like a
    successful formal acceptance.
    """
    del output_dir  # Kept in the public input contract for explicitness.
    root = Path(runtime_root).expanduser().resolve()
    authority, model_signature = _load_authority(
        baseline_manifest=baseline_manifest,
        baseline_index=baseline_index,
        eed_model=eed_model,
    )
    _require_file(documentation, "documentation")
    readiness_path = _require_file(root / "readiness.json", "readiness")
    telemetry_path = _require_file(root / "telemetry.sqlite3", "telemetry")
    try:
        readiness = json.loads(readiness_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CanaryCheckpointError("readiness checkpoint is not valid JSON") from exc
    if not isinstance(readiness, dict):
        raise CanaryCheckpointError("readiness checkpoint must be a JSON object")
    if readiness.get("authority_digest") != authority.authority_digest:
        raise CanaryCheckpointError("readiness authority checkpoint mismatch")
    if readiness.get("model_signature") != model_signature:
        raise CanaryCheckpointError("readiness model checkpoint mismatch")
    frontier = readiness.get("evidence_sequence_frontier")
    if isinstance(frontier, bool) or not isinstance(frontier, int) or frontier < 1:
        raise CanaryCheckpointError("readiness evidence frontier is missing")
    if not isinstance(readiness.get("source_contribution"), dict):
        raise CanaryCheckpointError("readiness source contribution checkpoint is missing")
    with RuntimeTelemetryStore(telemetry_path) as telemetry:
        snapshot = telemetry.snapshot()
    required_counters = {"v5_canary_runs", "v5_canary_final_rewards"}
    required_gauges = {"v5_canary_independent_verifier_pass"}
    if not required_counters.issubset(snapshot.counters):
        raise CanaryCheckpointError("telemetry canary counter checkpoint is missing")
    if not required_gauges.issubset(snapshot.gauges):
        raise CanaryCheckpointError("telemetry verifier checkpoint is missing")
    if snapshot.gauges["v5_canary_independent_verifier_pass"] != 1.0:
        raise CanaryCheckpointError("telemetry verifier checkpoint is not PASS")


def _candidate(
    entrypoint: str,
    *,
    confidence: float,
    expected_volume: int,
) -> SourceCandidate:
    return SourceCandidate(
        canonical_entrypoint=entrypoint,
        source_family="SYNTHETIC_FIXTURE",
        level=SourceLevel.SOURCE,
        discovered_by="v5-canary",
        discovery_strategy="deterministic-fixture-search",
        expected_year_from=1997,
        expected_year_to=1997,
        expected_volume=expected_volume,
        temporal_semantics_prior=0.8,
        enumerability_prior=0.9,
        direct_evidence_prior=0.0,
        baseline_overlap_prior=0.0,
        access_cost_prior=0.1,
        adapter_cost_prior=0.1,
        confidence=confidence,
    )


def _advance_to_scouting(registry: SourceDiscoveryRegistry, source_key: str) -> None:
    for state in (
        SourceState.TRIAGED,
        SourceState.SCOUT_READY,
        SourceState.SCOUTING,
    ):
        registry.transition(source_key, state)


def _ranked_names(
    model: ProductionValueModel,
    primary: SourceCandidate,
    secondary: SourceCandidate,
) -> tuple[str, ...]:
    values = (
        ("synthetic-primary", model.estimate(primary).score),
        ("synthetic-secondary", model.estimate(secondary).score),
    )
    return tuple(name for name, _score in sorted(values, key=lambda item: (-item[1], item[0])))


def _synthetic_transport(hostname: str, year: int):
    return (
        [
            {
                "timestamp": f"{year}0101000000",
                "original": f"http://{hostname}/",
                "status": "200",
            }
        ],
        True,
    ),


def _drain_synthetic_evidence(
    control: ControlStore,
    evidence: EvidenceStore,
) -> int:
    """Resolve synthetic host tasks without changing their durable identity."""
    tasks = control.list_evidence_tasks()
    if not tasks:
        raise RuntimeError("synthetic production did not enqueue evidence tasks")
    keys = [task.key for task in tasks]
    claimed = control.claim_evidence_tasks(
        owner="v5-canary-evidence",
        limit=len(keys),
        keys=keys,
    )
    if len(claimed) != len(keys):
        raise RuntimeError("synthetic evidence task claim was incomplete")
    writer = CommitWriter(
        evidence,
        control,
        owner="v5-canary-evidence",
        flush_count=1,
    )
    provider_requests = 0
    expected_proofs = 0
    try:
        for task in claimed:
            key: EvidenceQueryKey = task.key
            scope = key.temporal_scope
            if scope.year_from == scope.year_to:
                result = query_year(
                    key.hostname,
                    scope.year_from,
                    _synthetic_transport,
                    provider=key.provider,
                    policy_version=key.policy_version,
                )
                provider_requests += 1
                writer.submit(result.capsule, result)
                expected_proofs += int(result.capsule is not None)
                continue

            results = [
                query_year(
                    key.hostname,
                    year,
                    _synthetic_transport,
                    provider=key.provider,
                    policy_version=key.policy_version,
                )
                for year in range(scope.year_from, scope.year_to + 1)
            ]
            provider_requests += len(results)
            capsules = [
                result.capsule
                for result in results
                if result.capsule is not None
            ]
            origin = control.primary_evidence_task_origin(key)
            if capsules:
                provenance = EvidenceTaskProvenance(
                    key=key,
                    source_key="" if origin is None else origin[0],
                    reservoir_id="" if origin is None else origin[1],
                    lease_id="" if origin is None else origin[2],
                    committed_at=float(control.clock()),
                )
                evidence.put_many_with_task_provenance(
                    (capsule, provenance) for capsule in capsules
                )
                control.attribute_task_host_years(
                    key,
                    (capsule.year for capsule in capsules),
                )
            expected_proofs += len(capsules)
            states = [result.state for result in results]
            if all(
                state in {
                    CDXQueryState.PASS,
                    CDXQueryState.EMPTY_EXHAUSTIVE,
                }
                for state in states
            ):
                final_state = (
                    CDXQueryState.PASS
                    if capsules
                    else CDXQueryState.EMPTY_EXHAUSTIVE
                )
            else:
                raise RuntimeError(
                    "synthetic range evidence unexpectedly remained incomplete"
                )
            control.finish_evidence_task(
                key,
                final_state,
                owner="v5-canary-evidence",
            )
    finally:
        writer.close()
    if evidence.count() != expected_proofs:
        raise RuntimeError("synthetic evidence proof count is incomplete")
    return provider_requests


def run_canary(
    *,
    runtime_root: Path,
    baseline_manifest: Path,
    baseline_index: Path,
    eed_model: Path,
    documentation: Path,
    output_dir: Path,
) -> CanaryResult:
    """Execute one deterministic synthetic full-loop canary."""
    runtime_root = Path(runtime_root).expanduser().resolve()
    output_dir = Path(output_dir).expanduser().resolve()
    runtime_root.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    authority, model_signature = _load_authority(
        baseline_manifest=baseline_manifest,
        baseline_index=baseline_index,
        eed_model=eed_model,
    )
    _require_file(documentation, "documentation")

    control = ControlStore(runtime_root / "control.sqlite3")
    evidence = EvidenceStore(runtime_root / "evidence.sqlite3")
    candidates = CandidateStore(runtime_root / "candidates.sqlite3")
    registry = SourceDiscoveryRegistry(control)
    registry.set_scout_authority(
        baseline_signature=authority.authority_digest,
        model_signature=model_signature,
    )
    try:
        # Discovery and authority-scoped measurement.
        episode = registry.begin_search_episode(
            strategy="deterministic-fixture-search",
            backend="synthetic",
            query="v5-canary",
            actor="canary",
            episode_id="search:v5-full-loop-canary",
        )
        primary, _ = registry.register_proposal(
            _candidate(_PRIMARY_SOURCE, confidence=0.95, expected_volume=5),
            episode_id=episode.episode_id,
        )
        secondary, _ = registry.register_proposal(
            _candidate(_SECONDARY_SOURCE, confidence=0.35, expected_volume=1),
            episode_id=episode.episode_id,
        )
        registry.finish_search_episode(episode.episode_id, search_cost_seconds=0.1)
        for candidate in (primary, secondary):
            _advance_to_scouting(registry, candidate.source_key)
            registry.record_scout_measurement(
                candidate.source_key,
                ScoutMeasurement(
                    sampled_records=5 if candidate.source_key == primary.source_key else 1,
                    unique_hosts=5 if candidate.source_key == primary.source_key else 1,
                    novel_hosts=5 if candidate.source_key == primary.source_key else 1,
                    direct_host_years=0,
                    requests=1,
                    bytes_read=500,
                    elapsed_seconds=1.0,
                    novel_eed=10.0 if candidate.source_key == primary.source_key else 0.2,
                    measurement_mode=MeasurementMode.HOST_ONLY,
                ),
                baseline_signature=authority.authority_digest,
                model_signature=model_signature,
            )
            registry.transition(candidate.source_key, SourceState.WARM)

        # Capture the proxy ordering before a FINAL outcome exists.
        value_model = ProductionValueModel(registry)
        ranking_before = _ranked_names(value_model, primary, secondary)

        # Durable activation compiles the measured primary source.
        registry.begin_activation(primary.source_key)
        compiler = SourceActivationCompiler(control, registry=registry)
        specs = compiler.compile_active(limit=1)
        if len(specs) != 1 or specs[0].source_key != primary.source_key:
            raise RuntimeError("synthetic activation did not produce one primary spec")
        spec = specs[0]

        source_file = runtime_root / "synthetic-primary.txt"
        source_file.write_text("\n".join(_CANARY_SOURCE_HOSTS) + "\n", encoding="utf-8")
        adapter = _SyntheticSourceAdapter(
            source_file,
            source_id=spec.reservoir_id,
            source_year=1997,
        )
        reservoir = control.get_reservoir(spec.reservoir_id)
        if reservoir is None or reservoir.state is not ReservoirState.READY:
            raise RuntimeError("activated synthetic reservoir is not READY")
        lease_template = WorkLease.create(
            reservoir_id=spec.reservoir_id,
            cursor_start=reservoir.cursor,
            max_records=5,
            max_requests=1,
            max_bytes=4096,
            max_seconds=5.0,
            expected_evidence_tasks=5,
            expected_novel_eed=10.0,
        )
        lease_candidate = LeaseCandidate(
            reservoir_id=spec.reservoir_id,
            expected_novel_eed=10.0,
            costs=ResourceCost(0.0, 1.0, 1.0, 1.0),
            reservoir=reservoir,
            lease=lease_template,
            evidence_mode="discovery_only",
            evidence_provider="wayback",
            expected_evidence_tasks=5,
            reservation_evidence_tasks=5,
            source_key=primary.source_key,
        )
        producer = SourceProducer(
            baseline=BaselineIndex(baseline_index, authority=authority),
            control_store=control,
            evidence_store=evidence,
            candidate_store=candidates,
            source_registry=registry,
            scheduler=GlobalScheduler(CreditLedger({"wayback": 5})),
            candidates=(lease_candidate,),
            adapters={spec.adapter_id: adapter},
            backlog_capacities={"wayback": 5},
            queue_capacities={
                "source_records": 2,
                "observations": 2,
                "evidence_tasks": 2,
                "commits": 2,
            },
            owner="v5-canary-source",
            domain_fanout_min_children=999,
            rdap_fanout_min_children=999,
        )
        try:
            producer_report = producer.run_once()
        finally:
            producer.baseline.close()
            adapter.close()
        if producer_report.leases_succeeded != 1:
            raise RuntimeError("synthetic production did not complete its source lease")
        synthetic_provider_requests = _drain_synthetic_evidence(
            control,
            evidence,
        )

        # Readiness consumes the durable evidence frontier and publishes FINAL.
        evidence.close()
        candidates.close()
        control.close()
        readiness_path = runtime_root / "readiness.json"
        with IncrementalReadinessRuntime(
            runtime_root,
            baseline_index=baseline_index,
            eed_model=eed_model,
            authority_manifest=baseline_manifest,
            batch_size=100,
        ) as readiness:
            readiness_report = readiness.sync_until_current()
            IncrementalReadinessRuntime.write_report_atomic(readiness_report, readiness_path)

        control = ControlStore(runtime_root / "control.sqlite3")
        registry = SourceDiscoveryRegistry(control)
        final_registry = ProductionValueModel(registry)
        primary_after = registry.get_candidate(primary.source_key)
        secondary_after = registry.get_candidate(secondary.source_key)
        if primary_after is None or secondary_after is None:
            raise RuntimeError("production candidates disappeared during readiness")
        final_estimate = final_registry.estimate(primary_after)
        ranking_after = _ranked_names(final_registry, primary_after, secondary_after)
        if final_estimate.closed_runs < 1:
            raise RuntimeError("FINAL reward did not close a production run")
        if ranking_before == ranking_after:
            raise RuntimeError("FINAL reward did not change allocation ranking")
        control.close()

        # Formal snapshot/export uses the readiness frontier and the real index.
        source_root = runtime_root / "source"
        source_root.mkdir(parents=True, exist_ok=True)
        production_config = source_root / "production.toml"
        production_config.write_text(
            'source_mode = "activated"\nprovider = "synthetic"\n',
            encoding="utf-8",
        )
        source_report = source_root / "source-report.json"
        source_report.write_text(
            json.dumps({"canary": "synthetic", "source": primary.source_key}, sort_keys=True),
            encoding="utf-8",
        )
        cdx_audit = source_root / "cdx-audit.json"
        cdx_audit.write_text(
            json.dumps(
                {
                    "provider": "synthetic",
                    "requests": synthetic_provider_requests,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        control = ControlStore(runtime_root / "control.sqlite3")
        evidence = EvidenceStore(runtime_root / "evidence.sqlite3")
        candidates = CandidateStore(runtime_root / "candidates.sqlite3")
        baseline = BaselineIndex(baseline_index, authority=authority)
        try:
            with readiness_path.open("r", encoding="utf-8") as source:
                readiness_payload = json.load(source)
            context = RuntimeSubmissionContext(
                baseline_manifest=authority.as_dict(),
                code_revision=hashlib.sha256(b"v5-full-loop-canary").hexdigest(),
                source_report_set=("source-report.json",),
                cdx_audit_set=("cdx-audit.json",),
                eed_report=readiness_payload,
                eed_model_path=Path(eed_model),
                baseline_eed=authority.baseline_eed,
                candidate_snapshot_id="candidate:v5-full-loop-canary",
                evidence_sequence_frontier=int(
                    readiness_payload["evidence_sequence_frontier"]
                ),
            )
            snapshot = build_runtime_snapshot(
                context=context,
                evidence_store=evidence,
                baseline=baseline,
                snapshot_id="submission:v5-full-loop-canary",
            )
            archive, verification = export_runtime_submission(
                snapshot=snapshot,
                evidence_store=evidence,
                candidate_store=candidates,
                baseline_manifest_path=Path(baseline_manifest),
                baseline_index_path=Path(baseline_index),
                eed_model_path=Path(eed_model),
                name="v5-full-loop-canary",
                output_dir=output_dir,
                source_root=source_root,
                documentation_path=Path(documentation),
                artifact_specs=(
                    ArtifactSpec(
                        logical_role="production_config",
                        source_path=production_config,
                        archive_path="run/production.toml",
                    ),
                    ArtifactSpec(
                        logical_role="source_report",
                        source_path=source_report,
                        archive_path="run/source-report.json",
                    ),
                    ArtifactSpec(
                        logical_role="cdx_audit",
                        source_path=cdx_audit,
                        archive_path="run/cdx-audit.json",
                    ),
                ),
                artifact_allowed_roots=(source_root,),
                telemetry_path=runtime_root / "telemetry.sqlite3",
            )
        finally:
            baseline.close()
            evidence.close()
            candidates.close()

        if not verification.ready:
            raise CanaryCheckpointError("independent verifier did not PASS")
        with RuntimeTelemetryStore(runtime_root / "telemetry.sqlite3") as telemetry:
            telemetry.add_counters(
                {"v5_canary_runs": 1, "v5_canary_final_rewards": 1}
            )
            telemetry.set_gauges(
                {
                    "v5_canary_independent_verifier_pass": 1,
                    "v5_canary_ranking_changed": 1,
                }
            )
        validate_canary_checkpoints(
            runtime_root=runtime_root,
            baseline_manifest=baseline_manifest,
            baseline_index=baseline_index,
            eed_model=eed_model,
            documentation=documentation,
            output_dir=output_dir,
        )
        report_path = output_dir / "v5-full-loop-canary.json"
        result = CanaryResult(
            stages=STAGES,
            archive_path=archive,
            report_path=report_path,
            readiness_path=readiness_path,
            final_reward_eed=float(final_estimate.expected_final_eed),
            ranking_before=ranking_before,
            ranking_after=ranking_after,
            verifier_ready=verification.ready,
        )
        report_path.write_text(
            json.dumps(result.as_dict(), ensure_ascii=False, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        return result
    finally:
        for store in (evidence, candidates, control):
            try:
                store.close()
            except Exception:
                pass


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--baseline-manifest", type=Path, required=True)
    parser.add_argument("--baseline-index", type=Path, required=True)
    parser.add_argument("--eed-model", type=Path, required=True)
    parser.add_argument("--documentation", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_canary(
            runtime_root=args.runtime_root,
            baseline_manifest=args.baseline_manifest,
            baseline_index=args.baseline_index,
            eed_model=args.eed_model,
            documentation=args.documentation,
            output_dir=args.output_dir,
        )
    except (CanaryCheckpointError, OSError, RuntimeError, ValueError) as exc:
        print(f"V5 canary FAILED closed: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result.as_dict(), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
