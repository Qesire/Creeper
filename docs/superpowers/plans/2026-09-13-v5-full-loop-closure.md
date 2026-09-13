# V5 Full Loop Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use subagent-driven execution to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close every V5 production, attribution, recovery, formal-authority, and scale gap so a live full-loop canary reaches an independently verified submission.

**Architecture:** Add one durable production-exposure accounting boundary in the ControlStore, use it from sequential, historical-region, and platform-year lanes, and keep evidence proof-first with idempotent lineage. Package all authority inputs into the formal archive, freeze an evidence frontier before export, and make export and verification stream ordered host-year records with bounded memory. Harden activation and source-run state machines, then connect operational metrics and the final canary.

**Tech Stack:** Python 3.12, `sqlite3` WAL, existing `EvidenceStore`, `ControlStore`, `CandidateStore`, `BaselineIndex`, `SourceDiscoveryRegistry`, `unittest`, `tracemalloc`, deterministic ZIP export.

## Global Constraints

- Preserve the authority boundary: independent verification must rebuild authority from packaged artifacts, never from mutable runtime configuration.
- Preserve proof-first ordering: evidence rows are durable before attribution, page completion, or FINAL exposure publication.
- Use additive SQLite migrations and idempotent `(identity, authority)` writes; never reset existing runtime databases.
- Only authority-matched terminal FINAL exposures may feed `ProductionValueModel`.
- Aborted and expired work retains observed cost and evidence but never invents unobserved reward.
- Keep exporter and verifier Python memory bounded independently of logical host-year row count.
- Use the existing `unittest` suite and write each new behavior test before production code.
- Do not run the live canary or remove PR draft/merge gates until all deterministic checks pass.

---

### Task 1: Add the durable production-exposure accounting boundary

**Files:**
- Create: `src/creeper/runtime/exposure.py`
- Modify: `src/creeper/storage/control_store.py` near the existing durable lease and platform-task schema
- Modify: `src/creeper/source_discovery/registry.py` source-run compatibility methods
- Create: `tests/unit/test_production_exposure.py`
- Create: `tests/integration/test_production_exposure_recovery.py`

**Interfaces:**
- Produces `ProductionExposureState` with `RUNNING`, `READ_COMPLETE`, `VALIDATING`, `FINAL_CLOSED`, `ABORTED`, and `EXPIRED` values.
- Produces immutable `ProductionExposure` with `exposure_id`, `source_key`, `reservoir_id`, `lease_id`, `task_id`, `lane`, authority signatures, source/provider counters, evidence frontier, accepted host-year count, FINAL EED, terminal reason, and timestamps.
- Adds `ControlStore.begin_production_exposure(...) -> ProductionExposure`.
- Adds `ControlStore.record_production_exposure_progress(exposure_id, ..., state) -> ProductionExposure`.
- Adds `ControlStore.finalize_production_exposure(exposure_id, *, final_accepted_eed, accepted_host_years, evidence_frontier, authority) -> bool`.
- Adds `ControlStore.abort_production_exposure(exposure_id, *, state, reason) -> bool`.
- Adds `ControlStore.recover_open_production_exposures() -> int`.
- Makes `SourceDiscoveryRegistry.list_source_run_outcomes(..., closed_only=True)` return only `FINAL_CLOSED` exposures for mature value calculations while preserving existing compatibility fields.

- [ ] **Step 1: Write the failing tests** for unique exposure creation, authority mismatch rejection, idempotent FINAL publication, abort/expire terminal states, and restart reconciliation of a lease with an open exposure.
- [ ] **Step 2: Run the focused tests to verify RED.**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_production_exposure tests.integration.test_production_exposure_recovery -v`

Expected: FAIL because the exposure state/model and ControlStore methods do not exist.

- [ ] **Step 3: Add the model and additive schema migration.** Store one row per exposure, use a uniqueness key covering lane identity and authority, reject negative counters, and make repeated terminal writes return the existing terminal result.
- [ ] **Step 4: Add the ControlStore transitions and registry compatibility projection.** Recovery must map expired leases to `EXPIRED` and never set `final_accepted_eed` for an exposure that did not reach validation.
- [ ] **Step 5: Run the focused tests and the existing source-value tests.**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_production_exposure tests.integration.test_production_exposure_recovery tests.unit.test_source_value tests.unit.test_source_value_model -v`

Expected: PASS.

- [ ] **Step 6: Commit.** `git add src/creeper/runtime/exposure.py src/creeper/storage/control_store.py src/creeper/source_discovery/registry.py tests/unit/test_production_exposure.py tests/integration/test_production_exposure_recovery.py && git commit -m "feat: add durable production exposure accounting"`

### Task 2: Close historical-region production learning

**Files:**
- Modify: `src/creeper/historical_index_service.py`
- Modify: `src/creeper/source_discovery/harvest.py`
- Modify: `src/creeper/runtime/readiness.py`
- Create: `tests/integration/test_historical_production_exposure.py`
- Modify: `tests/unit/test_historical_index_service.py`

**Interfaces:**
- `RegionHarvestExecutor.harvest(region_key, *, exposure_id: str | None = None) -> RegionHarvestReport | None` associates proof-first capsules with the exposure when supplied.
- `HistoricalIndexService` creates one `lane="historical_region"` exposure per claimed region lease, updates source/provider counters and evidence frontier, and calls `finalize_production_exposure(...)` after readiness validation.
- `_release_harvest_reservoirs()` releases the ownership fence only after exposure terminal publication and uses the existing lease abort/release operation only for ownership, never as the reward state.

- [ ] **Step 1: Write a failing integration test** with a fake historical index that emits one novel host-year, then assert that EvidenceStore, origin attribution, exposure FINAL EED/cost, and `ProductionValueModel` all contain the same source lineage.
- [ ] **Step 2: Add a failing restart test** that interrupts after evidence commit and before region release, runs recovery, and asserts exactly one terminal exposure and no duplicate capsule or reward.
- [ ] **Step 3: Run the focused tests to verify RED.**

Run: `PYTHONPATH=src python3 -m unittest tests.integration.test_historical_production_exposure tests.unit.test_historical_index_service -v`

Expected: FAIL because the historical lane currently writes direct evidence without a production exposure.

- [ ] **Step 4: Thread exposure identity and counters through region harvest.** Keep `attribute_direct_origin_unit_host_years` and add exposure linkage without changing exact hostname-year semantics.
- [ ] **Step 5: Finalize historical exposures from the authority-matched readiness frontier.** Do not close an exposure while evidence tasks or readiness processing remain incomplete; preserve observed cost on failures.
- [ ] **Step 6: Run focused historical, readiness, and source-value tests.**

Run: `PYTHONPATH=src python3 -m unittest tests.integration.test_historical_production_exposure tests.unit.test_historical_index_service tests.unit.test_incremental_readiness tests.unit.test_source_value -v`

Expected: PASS.

- [ ] **Step 7: Commit.** `git add src/creeper/historical_index_service.py src/creeper/source_discovery/harvest.py src/creeper/runtime/readiness.py tests/integration/test_historical_production_exposure.py tests/unit/test_historical_index_service.py && git commit -m "feat: close historical production reward loop"`

### Task 3: Add automatic platform-year admission and task lineage

**Files:**
- Create: `src/creeper/evidence/platform_admission.py`
- Modify: `src/creeper/storage/control_store.py` platform-year schema and lifecycle methods
- Modify: `src/creeper/evidence/platform_harvest.py`
- Modify: `src/creeper/storage/evidence_store.py`
- Create: `tests/unit/test_platform_admission.py`
- Modify: `tests/integration/test_platform_year_harvest.py`

**Interfaces:**
- Adds `PlatformYearAdmissionPolicy` with a separate integer budget and authority signatures.
- Adds `PlatformYearAdmission.admit(observations: Iterable[PlatformYearObservation]) -> PlatformAdmissionReport`.
- Admission identity is `(provider, subject, target_year, request_template_hash, source_key, reservoir_id, exposure_id)`.
- Platform rows persist `source_key`, `reservoir_id`, `exposure_id`, `authority_digest`, and task terminal reason.
- `PlatformYearHarvestWorker` writes task provenance before `finish_platform_year_harvest_page(...)` and finalizes the task exposure only after the last page and readiness frontier are complete.

- [ ] **Step 1: Write failing tests** proving durable source/index observations admit a task automatically, separate budget blocks excess admission, repeated admission is idempotent, and a page replay does not duplicate evidence or lineage.
- [ ] **Step 2: Run focused tests to verify RED.**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_platform_admission tests.integration.test_platform_year_harvest -v`

Expected: FAIL because no producer admission edge or lineage fields exist.

- [ ] **Step 3: Implement the admission policy and additive ControlStore fields.** Reject unscoped observations, enforce the independent budget, and preserve provider continuation semantics.
- [ ] **Step 4: Commit proof-first task provenance and exposure progress in the worker.** A provider page with no continuation is the only input allowed to reach `COMPLETE`.
- [ ] **Step 5: Run focused platform tests and the control-store suite.**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_platform_admission tests.integration.test_platform_year_harvest tests.unit.test_control_store -v`

Expected: PASS.

- [ ] **Step 6: Commit.** `git add src/creeper/evidence/platform_admission.py src/creeper/storage/control_store.py src/creeper/evidence/platform_harvest.py src/creeper/storage/evidence_store.py tests/unit/test_platform_admission.py tests/integration/test_platform_year_harvest.py && git commit -m "feat: connect platform harvest admission and lineage"`

### Task 4: Wire platform admission into the supervised runtime

**Files:**
- Modify: `src/creeper/autopilot.py`
- Modify: `src/creeper/platform_harvest_cli.py`
- Modify: `conf/autopilot.example.toml`
- Modify: `conf/creeper.activated.example.toml`
- Modify: `tests/unit/test_autopilot.py`
- Create: `tests/integration/test_platform_admission_autopilot.py`

**Interfaces:**
- `AutopilotConfig` gains explicit platform admission configuration and a separate `platform_year_budget`.
- `build_child_specs()` starts the admission producer and platform worker with independent bounded budgets.
- The producer consumes persisted source/index intelligence from the ControlStore and never depends on manual `enqueue_platform_year_harvest()` calls in production mode.

- [ ] **Step 1: Write failing config and integration tests** asserting an enabled runtime creates a platform task from a durable eligible observation and that platform budget exhaustion does not consume the Wayback source producer budget.
- [ ] **Step 2: Run tests to verify RED.**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_autopilot tests.integration.test_platform_admission_autopilot -v`

Expected: FAIL because only the worker is supervised.

- [ ] **Step 3: Add strict configuration parsing and the admission child spec.** Keep existing configs opt-in compatible unless the new section is explicitly enabled.
- [ ] **Step 4: Add bounded producer scheduling and telemetry counters.** Use durable idempotency so a restart cannot multiply tasks.
- [ ] **Step 5: Run focused autopilot and platform tests.**

Run: `PYTHONPATH=src python3 -m unittest tests.unit.test_autopilot tests.integration.test_platform_admission_autopilot tests.integration.test_platform_year_harvest -v`

Expected: PASS.

- [ ] **Step 6: Commit.** `git add src/creeper/autopilot.py src/creeper/platform_harvest_cli.py conf/autopilot.example.toml conf/creeper.activated.example.toml tests/unit/test_autopilot.py tests/integration/test_platform_admission_autopilot.py && git commit -m "feat: supervise automatic platform admission"`

### Task 5: Package and reconstruct reviewed external authority

**Files:**
- Modify: `src/creeper/evidence/contract_registry.py`
- Modify: `src/creeper/evidence/classification.py`
- Modify: `src/creeper/submission/artifact_manifest.py`
- Modify: `src/creeper/submission/streaming_exporter.py`
- Modify: `src/creeper/submission/verify.py`
- Create: `tests/integration/test_packaged_reviewed_authority.py`
- Modify: `tests/integration/test_submission_verifier.py`

**Interfaces:**
- `ReviewedContractRegistry.to_manifest_payload() -> dict[str, object]` and `ReviewedContractRegistry.from_manifest_payload(payload) -> ReviewedContractRegistry` are deterministic and validate every binding.
- `classify_acquisition_lane(capsule, *, reviewed_contracts: ReviewedContractRegistry | None = None)` accepts the packaged registry for external direct evidence.
- The archive contains `artifacts/runtime/reviewed_contract_registry.json` and its artifact-manifest row, with a digest included in `MANIFEST.json`.
- `verify_submission_archive(...)` reconstructs the registry from the archive before validating direct capsules.

- [ ] **Step 1: Write a failing verifier test** that packages a reviewed JSONL contract, accepts its direct capsule after unpackaged runtime registry removal, and rejects a changed registry or artifact identity.
- [ ] **Step 2: Run the focused test to verify RED.**

Run: `PYTHONPATH=src python3 -m unittest tests.integration.test_packaged_reviewed_authority tests.integration.test_submission_verifier -v`

Expected: FAIL because formal classification only knows built-in contracts.

- [ ] **Step 3: Add canonical registry serialization and strict archive inclusion.** Preserve exact locator, artifact identity, contract digest, custodian, edition, and review note.
- [ ] **Step 4: Pass the reconstructed registry into formal semantic validation.** A missing, malformed, or digest-mismatched registry fails closed.
- [ ] **Step 5: Run authority and existing submission tests.**

Run: `PYTHONPATH=src python3 -m unittest tests.integration.test_packaged_reviewed_authority tests.integration.test_submission_verifier tests.unit.test_source_activation -v`

Expected: PASS.

- [ ] **Step 6: Commit.** `git add src/creeper/evidence/contract_registry.py src/creeper/evidence/classification.py src/creeper/submission/artifact_manifest.py src/creeper/submission/streaming_exporter.py src/creeper/submission/verify.py tests/integration/test_packaged_reviewed_authority.py tests/integration/test_submission_verifier.py && git commit -m "fix: make reviewed authority independently reconstructable"`

### Task 6: Separate observed overlap and freeze the formal frontier

**Files:**
- Modify: `src/creeper/runtime/readiness.py`
- Modify: `src/creeper/submission/snapshot.py`
- Modify: `src/creeper/runtime/submission.py`
- Modify: `src/creeper/submission_cli.py`
- Modify: `src/creeper/submission/precheck.py`
- Modify: `src/creeper/submission/streaming_exporter.py`
- Create: `tests/integration/test_formal_frontier_and_overlap.py`
- Modify: `tests/unit/test_runtime_validation.py`

**Interfaces:**
- `SubmissionSnapshot` gains `observed_baseline_overlap`, `output_baseline_overlap`, `evidence_sequence_frontier`, and `candidate_snapshot_id` while retaining a compatibility reader for existing reports.
- `EvidenceStore.iter_canonical_host_year_capsules(*, max_sequence: int | None = None)` yields only rows at or below the frozen frontier.
- `build_runtime_snapshot(...)` captures the EvidenceStore maximum sequence and CandidateStore snapshot identity in one explicit checkpoint record.
- `precheck_submission()` gates only on `output_baseline_overlap == 0`; observed overlap remains an audit field.

- [ ] **Step 1: Write failing tests** where readiness observes baseline overlap but the exported stream has zero remaining overlap, and where evidence arriving after checkpoint `N` is excluded from the formal package.
- [ ] **Step 2: Run focused tests to verify RED.**

Run: `PYTHONPATH=src python3 -m unittest tests.integration.test_formal_frontier_and_overlap tests.unit.test_runtime_validation -v`

Expected: FAIL because one overlap field is currently used for both meanings and export reads a later frontier.

- [ ] **Step 3: Add the two overlap fields and bounded frontier query.** Make the checkpoint durable and fail closed if the CandidateStore identity cannot be reconciled to the checkpoint.
- [ ] **Step 4: Thread the frozen frontier through runtime export, manifest reports, and precheck.** All annual files, EED, contribution, and verifier inputs must derive from `N`.
- [ ] **Step 5: Run focused submission and readiness tests.**

Run: `PYTHONPATH=src python3 -m unittest tests.integration.test_formal_frontier_and_overlap tests.integration.test_runtime_submission tests.unit.test_snapshot_builder tests.unit.test_runtime_validation -v`

Expected: PASS.

- [ ] **Step 6: Commit.** `git add src/creeper/runtime/readiness.py src/creeper/submission/snapshot.py src/creeper/runtime/submission.py src/creeper/submission_cli.py src/creeper/submission/precheck.py src/creeper/submission/streaming_exporter.py src/creeper/storage/evidence_store.py tests/integration/test_formal_frontier_and_overlap.py tests/unit/test_runtime_validation.py && git commit -m "fix: freeze formal evidence frontier and overlap semantics"`

### Task 7: Make independent verification bounded-memory

**Files:**
- Modify: `src/creeper/submission/verify.py`
- Create: `src/creeper/submission/streaming_reader.py`
- Create: `tests/integration/test_streaming_verifier_scale.py`
- Modify: `tests/integration/test_submission_verifier.py`

**Interfaces:**
- `iter_archive_lines(bundle, name, *, chunk_size: int = 64 * 1024) -> Iterator[str]` decodes complete lines without materializing an archive entry.
- `verify_submission_archive(...) -> VerificationReport` retains the existing public result shape while validating annual files, evidence, manifests, baseline, and EED in one ordered pass.
- The verifier uses scalar counters and current host-year state; it must not construct full annual sets, an annual-pairs set, or an evidence mapping.

- [ ] **Step 1: Write a one-million-row test** that builds a deterministic valid archive, runs export then independent verification under `tracemalloc`, and asserts semantic PASS plus a configured memory ceiling independent of row count.
- [ ] **Step 2: Run the scale test to verify RED or expose current memory growth.**

Run: `PYTHONPATH=src python3 -m unittest tests.integration.test_streaming_verifier_scale -v`

Expected: FAIL or exceed the memory ceiling because the verifier currently uses `splitlines()` and full Python sets/maps.

- [ ] **Step 3: Add chunked archive readers and ordered annual/evidence validation.** Detect missing, duplicate, unordered, mismatched, invalid, baseline-overlapping, and unreferenced rows while streaming.
- [ ] **Step 4: Preserve authority, lane, contribution, and EED checks without retaining all records.** Use the packaged registry from Task 5 and the frozen frontier from Task 6.
- [ ] **Step 5: Run the scale test and all semantic verifier tests.**

Run: `PYTHONPATH=src python3 -m unittest tests.integration.test_streaming_verifier_scale tests.integration.test_submission_verifier tests.integration.test_streaming_submission_export -v`

Expected: PASS with peak memory bounded by the reader/chunk budget.

- [ ] **Step 6: Commit.** `git add src/creeper/submission/verify.py src/creeper/submission/streaming_reader.py tests/integration/test_streaming_verifier_scale.py tests/integration/test_submission_verifier.py && git commit -m "perf: stream independent submission verification"`

### Task 8: Harden activation and source-run failure convergence

**Files:**
- Modify: `src/creeper/source_discovery/models.py`
- Modify: `src/creeper/source_discovery/registry.py`
- Modify: `src/creeper/source_discovery/activation.py`
- Modify: `src/creeper/source_cli.py`
- Modify: `src/creeper/runtime/source_producer.py`
- Create: `tests/integration/test_activation_recovery.py`
- Modify: `tests/integration/test_source_cli.py`
- Modify: `tests/integration/test_source_producer.py`

**Interfaces:**
- `SourceState` gains `ACTIVATING` and terminal `HOLD`/`REJECTED` semantics with durable reason and retry metadata.
- `SourceActivationCompiler.compile_active()` returns per-source successes and failures rather than raising on the first bad ACTIVE source.
- `SourceProducer.run_once()` always closes or aborts a begun source exposure in a `finally` boundary, including adapter lookup and pipeline failures before read completion.
- `SourceDiscoveryRegistry.reconcile_open_source_runs()` maps lease failure to `ABORTED`/`EXPIRED` and leaves no permanently open source-run row.

- [ ] **Step 1: Write failing tests** for WARM→ACTIVATING→ACTIVE, permanent reviewed-identity rejection freeing the active slot, one broken ACTIVE source not blocking healthy sources, and an exception after `begin_source_run()` producing a terminal aborted exposure.
- [ ] **Step 2: Run focused tests to verify RED.**

Run: `PYTHONPATH=src python3 -m unittest tests.integration.test_activation_recovery tests.integration.test_source_cli tests.integration.test_source_producer -v`

Expected: FAIL because activation marks ACTIVE before compile and source-run exceptions leave open rows.

- [ ] **Step 3: Add durable activation transitions and per-source compile isolation.** Classify identity/network failures as retryable only when they are verifiably transient.
- [ ] **Step 4: Add terminal source-run reconciliation in producer cleanup and startup recovery.** Preserve measured counters and evidence; do not publish speculative zero reward.
- [ ] **Step 5: Run focused discovery and producer tests.**

Run: `PYTHONPATH=src python3 -m unittest tests.integration.test_activation_recovery tests.integration.test_source_cli tests.integration.test_source_producer tests.unit.test_source_activation tests.unit.test_source_discovery_registry -v`

Expected: PASS.

- [ ] **Step 6: Commit.** `git add src/creeper/source_discovery/models.py src/creeper/source_discovery/registry.py src/creeper/source_discovery/activation.py src/creeper/source_cli.py src/creeper/runtime/source_producer.py tests/integration/test_activation_recovery.py tests/integration/test_source_cli.py tests/integration/test_source_producer.py && git commit -m "fix: converge activation and source-run failures"`

### Task 9: Publish operational telemetry and formal verification metrics

**Files:**
- Modify: `src/creeper/submission_cli.py`
- Modify: `src/creeper/submission/streaming_exporter.py`
- Modify: `src/creeper/platform_harvest_cli.py`
- Modify: `src/creeper/autopilot.py`
- Modify: `src/creeper/runtime/telemetry.py`
- Create: `tests/integration/test_v5_telemetry.py`
- Modify: `tests/unit/test_readiness_service.py`

**Interfaces:**
- `run_export(...)` passes a metrics dictionary into `build_streaming_submission_zip(...)` and records `submission_stream_peak_buffer_bytes` only after independent verification returns ready.
- Telemetry publishes bounded gauges for readiness frontier, active sources, historical exposures, platform task states, FINAL source value, admission counters, and verifier peak buffer.
- Telemetry writes remain operational-only and cannot alter EvidenceStore or formal manifest authority.

- [ ] **Step 1: Write failing telemetry tests** asserting the metrics sink is populated, persisted only after verifier PASS, and includes discovery/historical/platform gauges after restart.
- [ ] **Step 2: Run focused tests to verify RED.**

Run: `PYTHONPATH=src python3 -m unittest tests.integration.test_v5_telemetry tests.unit.test_readiness_service -v`

Expected: FAIL because the exporter sink is currently not connected to the CLI/telemetry store.

- [ ] **Step 3: Thread the metrics sink through CLI verification and telemetry persistence.** Use monotonic counters and bounded state-count queries.
- [ ] **Step 4: Run focused telemetry, export, and autopilot tests.**

Run: `PYTHONPATH=src python3 -m unittest tests.integration.test_v5_telemetry tests.integration.test_streaming_submission_export tests.unit.test_autopilot -v`

Expected: PASS.

- [ ] **Step 5: Commit.** `git add src/creeper/submission_cli.py src/creeper/submission/streaming_exporter.py src/creeper/platform_harvest_cli.py src/creeper/autopilot.py src/creeper/runtime/telemetry.py tests/integration/test_v5_telemetry.py tests/unit/test_readiness_service.py && git commit -m "feat: publish V5 closure telemetry"`

### Task 10: Run complete deterministic verification and the live full-loop canary

**Files:**
- Create: `tests/integration/test_v5_full_loop_closure.py`
- Modify: `tests/integration/test_v5_full_loop_integration.py`
- Modify: `docs/competition-rules-v3-audit.md`
- Modify: `README.md`
- Create: `scripts/run_v5_full_loop_canary.py`

**Interfaces:**
- The integration fixture creates a source discovery result, performs scout measurement, activates a source, harvests historical and platform-year evidence, completes FINAL exposure accounting, demonstrates a changed allocation order, freezes a formal frontier, exports a package, and verifies it independently.
- `scripts/run_v5_full_loop_canary.py` accepts an explicit runtime root, baseline manifest/index, EED model, documentation path, and output directory; it refuses live execution if readiness, authority, or telemetry checkpoints are missing.

- [ ] **Step 1: Write the failing end-to-end test** with assertions for every edge in the target loop and a restart between each durable boundary.
- [ ] **Step 2: Run the complete focused suite to verify the missing edges are exposed.**

Run: `PYTHONPATH=src python3 -m unittest tests.integration.test_v5_full_loop_closure -v`

Expected: FAIL until Tasks 1–9 are integrated.

- [ ] **Step 3: Implement the deterministic fixture and canary command.** Use synthetic providers and the real BaselineIndex/EED calculator; never treat synthetic evidence as a competition submission.
- [ ] **Step 4: Run the complete suite with dependencies installed.**

Run: `uv sync && PYTHONPATH=src uv run python3 -m unittest discover -s tests -p 'test_*.py' -v`

Expected: PASS with no import errors; any failure is fixed before claiming completion.

- [ ] **Step 5: Run the one-million-row export → verifier check and inspect memory/telemetry output.**

Run: `PYTHONPATH=src uv run python3 -m unittest tests.integration.test_streaming_verifier_scale tests.integration.test_v5_full_loop_closure -v`

Expected: PASS; verifier peak memory remains within the test ceiling and telemetry includes the measured stream buffer.

- [ ] **Step 6: Run the documented live canary only after deterministic checks pass.** Record the exact revision, authority digest, evidence frontier, FINAL reward, allocation order before/after, archive hash, independent verifier result, and telemetry database path.
- [ ] **Step 7: Update the audit and README with measured results, then commit.** `git add tests/integration/test_v5_full_loop_closure.py tests/integration/test_v5_full_loop_integration.py docs/competition-rules-v3-audit.md README.md scripts/run_v5_full_loop_canary.py && git commit -m "test: prove V5 full-loop closure"`

## Execution protocol

Dispatch one fresh subagent per task with the exact task text and write scope. Keep tasks serial where they share `ControlStore`, `registry.py`, `verify.py`, or `autopilot.py`; a reviewer checks the diff and focused tests before the next task starts. After every task, run `git diff --check`, inspect the changed schema/API, and record the test command and result. If a test fails for an unexpected reason, invoke systematic debugging before changing implementation or test expectations.

## Plan self-review

- Scope coverage: Tasks 1–2 close historical FINAL learning; Tasks 3–4 close platform admission, lineage, and separate budget; Task 5 closes formal reviewed authority; Task 6 closes overlap and frontier semantics; Task 7 closes verifier scale; Task 8 closes activation/source-run recovery; Task 9 closes telemetry; Task 10 proves the complete canary.
- Placeholder scan: no `TBD`, `TODO`, or unspecified implementation step is used; every task names files, interfaces, tests, commands, and expected outcomes.
- Type consistency: `ProductionExposureState`, `ProductionExposure`, `evidence_sequence_frontier`, `output_baseline_overlap`, and packaged registry interfaces are introduced before their downstream consumers.
- Safety: live network work is isolated to the final canary, and no merge-gate change is included in implementation tasks.
