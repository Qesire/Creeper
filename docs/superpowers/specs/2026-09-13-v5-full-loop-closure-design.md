# V5 Full Loop Closure Design

**Date:** 2026-09-13  
**Baseline:** `c25f5f6bc7821bd94888dbd3e788ac3d706b0560`  
**Goal:** Close the production, platform-year, authority, formal-submission, and recovery paths so a live Creeper run can prove discovery → measurement → activation → production → exact annual evidence → FINAL reward/cost → allocation change → independently verified submission.

## Scope and acceptance boundary

This design covers the P0 and P1 gaps identified by the V5 review. Existing authority rules, proof-first evidence commits, candidate-store reconciliation, provider pagination fencing, and streaming ZIP construction remain the foundation. The work is split into independently testable slices, but the final acceptance test must exercise one connected runtime path across historical evidence, platform-year evidence, FINAL learning, formal export, and the independent verifier.

The implementation is accepted only when all of the following are true:

1. Historical-index harvest produces a durable production exposure with source and reservoir identity, captures its provider requests, bytes, elapsed time, evidence frontier, and terminal state, and contributes a FINAL EED/cost outcome to `ProductionValueModel`.
2. Platform-year work is admitted automatically from durable source intelligence under its own bounded budget, carries source/task lineage, and writes FINAL EED/cost and attribution data when complete.
3. The formal archive contains the exact reviewed-contract registry and reviewed artifact identity data used by activation; the independent verifier reconstructs external direct authority from those packaged files.
4. Readiness reports preserve observed overlap as an audit statistic while formal submission gates only on overlap remaining in exported annual files.
5. Export and verification use bounded Python memory for multi-million logical host-year streams; a complete export → independent-verifier test passes at least 1,000,000 logical rows.
6. Activation, source-run, platform-task, exposure, and formal-snapshot state all have terminal failure/recovery semantics that converge after restart.

## Architecture

### 1. One production exposure contract

Add a durable `ProductionExposure` record as the common accounting boundary for every production lane:

```text
exposure_id
source_key
reservoir_id
lease_id / task_id
lane = sequential | historical_region | platform_year
authority: baseline_signature, model_signature, contract_digest
source_requests
provider_requests
source_bytes
provider_bytes
elapsed_seconds
evidence_frontier
accepted_host_years
final_accepted_eed
terminal_state
terminal_reason
created_at / updated_at / closed_at
```

The existing sequential source-run API may remain as a compatibility facade, but it must persist through this exposure contract or expose the same terminal state and accounting fields. `SourceRunOutcome` and `ProductionValueModel` will consume only authority-matched, terminal FINAL exposures. Aborted or expired exposures preserve durable evidence and incurred cost, but their unobserved remaining yield is never converted into a zero reward. A terminal exposure is idempotently finalized by `(exposure_id, authority)` and can be reconciled after restart.

Historical region harvest will create one exposure per owned region lease, associate every proof-first capsule with that exposure lineage, run readiness over the resulting evidence frontier, and finalize EED/cost before releasing ownership. The outer reservoir ownership fence remains separate from exposure accounting so successful ownership release does not imply a fake sequential source run.

### 2. Automatic platform-year admission and lineage

Introduce a small admission boundary between durable source intelligence and `platform_year_harvests`. The producer reads bounded, authority-scoped source/index observations, applies a deterministic value policy, and enqueues an idempotent `(provider, subject, target_year, request_template_hash, source_key, reservoir_id, exposure_id)` task. Admission has a separate configured budget and emits counters for admitted, skipped, and blocked tasks.

Platform-year rows gain durable lineage and authority fields. Each page commit writes capsules first, records task provenance for every accepted host-year, then commits the page cursor/state and cumulative counters. Completion remains provider-driven: `COMPLETE` is legal only after a provider response explicitly has no continuation and the last page has committed. Restart recovery may replay a page, but unique evidence and lineage writes remain idempotent.

On terminal completion, the platform task computes its baseline-external annual contribution through the same evidence/readiness authority as other lanes, finalizes its exposure, and publishes the result to the source/value ledger. Platform evidence therefore reaches both overall readiness and source/task-level FINAL learning.

### 3. Activation and source-run lifecycle

Change activation state progression to:

```text
WARM → ACTIVATING → ACTIVE
                 ↘ HOLD/REJECTED
```

`ACTIVATING` owns the compile, reviewed artifact identity check, contract binding, adapter creation, reservoir/index creation, and durable activation write. Permanent contract, identity, or unsupported-adapter errors become a terminal `REJECTED`/`HOLD` record with reason and release the active slot. Transient identity/network failures retain retry metadata and exponential backoff. Compilation is per-source: one broken candidate cannot abort the complete ACTIVE workset.

Source exposure state is explicit and finite:

```text
RUNNING → READ_COMPLETE → VALIDATING → FINAL_CLOSED
      ↘ ABORTED
      ↘ EXPIRED
```

The exception path records `ABORTED` or `EXPIRED` whenever a run was begun, including failures before `record_source_run_read`. Recovery reconciles open exposure rows against lease/task state and never leaves an indefinitely open source run. Only `FINAL_CLOSED` rows feed the mature production value model.

### 4. Formal authority, overlap, and immutable frontier

Formal export freezes a self-contained authority set containing:

```text
production configuration
reviewed_contract_registry.json
reviewed artifact identity declarations
authority digest
baseline/model signatures
evidence_sequence_frontier
candidate snapshot identity
```

The verifier receives the packaged registry through the archive manifest and reconstructs both built-in CDX/CDXJ contracts and external reviewed contracts locally. It never consults a mutable runtime registry.

Readiness stores two distinct quantities:

```text
observed_baseline_overlap  # audit statistic over processed evidence
output_baseline_overlap    # overlap in the final annual stream; formal gate requires 0
```

Before export, a formal checkpoint freezes the EvidenceStore frontier `N` and the candidate snapshot identity. The exporter iterates `max_sequence=N` or an equivalent immutable copied read snapshot. Manifest EED, annual files, contribution reports, and verifier input all derive from that same frontier.

### 5. Bounded-memory formal pipeline and telemetry

The verifier will replace whole-file `read().decode().splitlines()` and annual/evidence sets with ordered streaming readers and bounded state. It may retain only fixed-size chunks, current host-year validation state, scalar counters, and compact authority indexes whose size is independent of the evidence row count. Duplicate, ordering, coverage, baseline, and manifest invariants are checked as streams.

The submission CLI passes a metrics sink through export and writes the resulting `submission_stream_peak_buffer_bytes` to operational telemetry only after independent verification succeeds. Readiness, active-source, historical, platform-task, and FINAL-value gauges use the same telemetry store and remain outside EvidenceStore and formal authority.

## Implementation slices and tests

The implementation will proceed in this order:

1. **Exposure and historical closure:** failing tests for historical exposure creation, proof lineage, terminal finalization, restart reconciliation, and FINAL value/ranking change; then minimal storage/runtime changes.
2. **Platform admission and attribution:** failing tests for automatic producer admission, separate budget, page restart/idempotence, task lineage, and final EED/cost publication.
3. **Formal authority and snapshot semantics:** failing tests for packaged external registry reconstruction, observed/output overlap separation, frozen evidence frontier, and candidate snapshot consistency.
4. **Streaming verifier and telemetry:** failing tests for bounded verifier memory at a one-million-row fixture, semantic equivalence with the current verifier, and post-verification telemetry publication.
5. **Activation/source-run recovery and live gate:** failing tests for `ACTIVATING`, isolated compile failure, aborted/expired source runs, followed by the real live full-loop canary.

Each slice must retain the existing unit/integration suite, use the existing SQLite additive-migration patterns, and verify one crash/restart boundary. No slice may promote a scout proxy to mature value before a terminal FINAL exposure exists.

## Failure and recovery rules

- Evidence is committed before any attribution, page cursor, or exposure FINAL state that refers to it.
- Every lease, page, task, exposure, activation, and snapshot has an explicit terminal state or a deterministic recovery action.
- Replaying a committed page, exposure finalization, attribution, or snapshot checkpoint is idempotent.
- A stale authority, mutable reviewed artifact, missing continuation token, or frontier mismatch fails closed.
- A failed or incomplete exposure retains observed cost and evidence but contributes no speculative unobserved reward.
- Formal verification is required before a submission archive and its telemetry publication are considered successful.

## Risks and boundaries

The two-database boundary between ControlStore/CandidateStore and EvidenceStore cannot be made one SQLite transaction without expanding scope. The design therefore uses an explicit evidence frontier plus durable checkpoint/reconciliation, with verifier failure on any mismatch. The one-million-row test uses deterministic synthetic capsules for scale and semantic fixtures for authority; it does not claim network throughput or competition score. The live canary remains a separate final acceptance action after all deterministic tests pass.
