# Production EED Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the approved production path from discovered ACTIVE sources through durable production leases, range-aware evidence, and self-calculated annual EED.

**Architecture:** Preserve current SQLite/WAL control and evidence stores. Add a compiler/factory boundary between discovery and production, extend the existing producer with a long-lived watch loop, use ranged temporal scopes as durable evidence tasks that fan out to exact-year tasks, and make runtime submission derive EED from persisted evidence plus the latest baseline.

**Tech Stack:** Python 3.12, `unittest`, `uv`, SQLite WAL, `fsspec`, optional `s3fs`, HTTPX, aiolimiter, tenacity, warcio, existing Creeper source/evidence/runtime modules.

## Global Constraints

- Annual years are exactly `1996` through `2001`.
- Range probes discover candidate years only; exact-year evidence is required for annual acceptance.
- `fsspec` is a core dependency; S3 requires an explicit installed `s3fs` backend.
- `--once` remains deterministic and backward-compatible; `--watch` is additive.
- `SourceProducer` never performs Evidence Provider network I/O.
- EED is recomputed from persisted evidence and the latest baseline; external context cannot override it.
- Candidate and annual metrics remain separate.
- All durable work is bounded, idempotent, and resumable.

---

### Task 1: Declare and verify archive filesystem dependencies

**Files:**
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Modify: `src/creeper/sources/archive/warc.py`
- Test: `tests/unit/test_warc_remote_open.py`

**Interfaces:**
- Consumes: `fsspec_open` and existing WARC/ARC source paths.
- Produces: clean-installable HTTPS remote WARC behavior and explicit S3 backend errors.

- [ ] **Step 1: Write failing dependency/runtime tests.** Add a test that imports the remote WARC path in a clean project environment and a test that an unavailable S3 backend raises a message naming `s3fs`.
- [ ] **Step 2: Run the focused tests and verify the missing-backend failure is observable.**
- [ ] **Step 3: Add `fsspec` to project dependencies and add an optional S3 extra containing `s3fs`; keep the core install free of mandatory S3 credentials/backends.**
- [ ] **Step 4: Update WARC remote opening to distinguish HTTP(S) from S3 and fail closed with the exact install hint when S3 support is unavailable.**
- [ ] **Step 5: Run `uv lock --check` and the focused WARC tests.**
- [ ] **Step 6: Run `uv sync --locked` and confirm the package builds from the lock file.**

### Task 2: Compile ACTIVE discovery candidates into production reservoirs

**Files:**
- Create: `src/creeper/source_discovery/activation.py`
- Create: `src/creeper/sources/production.py`
- Modify: `src/creeper/source_discovery/registry.py`
- Modify: `src/creeper/storage/control_store.py`
- Test: `tests/unit/test_source_activation.py`
- Test: `tests/integration/test_source_activation.py`

**Interfaces:**
- Consumes: `SourceCandidate`, `SourceState.ACTIVE`, `ScoutMeasurement`, `ControlStore`, and adapter metadata.
- Produces: `ProductionSourceSpec`, `SourceActivationCompiler.compile(candidate)`, and an idempotent persisted `Reservoir`.

- [ ] **Step 1: Write a failing test for an ACTIVE WARC candidate compiling to one durable WARC Reservoir and SourceDomain.**
- [ ] **Step 2: Write a failing test that a second compile returns the existing reservoir without duplicating it.**
- [ ] **Step 3: Write a failing test that an unsupported candidate remains unactivated with a precise reason.**
- [ ] **Step 4: Implement `ProductionSourceSpec` and adapter-family detection for WARC/ARC, structured archive/tabular, and local static sources.**
- [ ] **Step 5: Implement idempotent compiler persistence and candidate-to-reservoir lineage.**
- [ ] **Step 6: Run focused activation tests, then existing source discovery/control-store tests.**

### Task 3: Add a production adapter factory and connect the producer composition root

**Files:**
- Modify: `src/creeper/sources/production.py`
- Modify: `src/creeper/sources/archive/warc_source.py`
- Modify: `src/creeper/source_cli.py`
- Test: `tests/integration/test_source_cli.py`
- Test: `tests/integration/test_source_producer.py`

**Interfaces:**
- Consumes: persisted `ProductionSourceSpec`/`Reservoir` rows.
- Produces: `ProductionAdapterFactory.open(reservoir)` returning the existing `SourceProducer` adapter contract: `execute(WorkLease)` and `extract_hosts(SourceRecord)`.

- [ ] **Step 1: Write a failing integration test that a compiled local WARC reservoir is loaded by the CLI without a manually constructed `StaticDatasetAdapter`.**
- [ ] **Step 2: Write a failing test that the resulting observation carries `year_hint_mask` but `direct_year_mask == 0`.**
- [ ] **Step 3: Implement the factory and WARC-to-`SourceRecord` adapter wrapper.**
- [ ] **Step 4: Preserve the existing static dataset configuration as a compatibility adapter.**
- [ ] **Step 5: Run the focused source CLI/producer tests and the existing WARC source tests.**

### Task 4: Make SourceProducer long-lived

**Files:**
- Modify: `src/creeper/runtime/source_producer.py`
- Modify: `src/creeper/source_cli.py`
- Modify: `conf/creeper.example.toml`
- Test: `tests/unit/test_source_producer_watch.py`
- Test: `tests/integration/test_source_producer.py`

**Interfaces:**
- Consumes: `SourceProducer.run_once()` and durable Reservoir state.
- Produces: `SourceProducer.run_forever(stop_event, idle_backoff)` and CLI `--watch` mode.

- [ ] **Step 1: Write a failing test that watch mode executes multiple disjoint leases and resumes the stored cursor.**
- [ ] **Step 2: Write a failing test that admission-blocked and exhausted states use idle backoff without busy looping.**
- [ ] **Step 3: Write a failing test that a stop event prevents a new lease claim.**
- [ ] **Step 4: Implement bounded idle backoff and signal-safe stop handling; keep `--once` unchanged.**
- [ ] **Step 5: Run the focused watch/producer tests and the existing durable lease tests.**

### Task 5: Make range probes durable and asynchronous

**Files:**
- Modify: `src/creeper/evidence/planner.py`
- Modify: `src/creeper/evidence/policies.py`
- Modify: `src/creeper/evidence/providers/async_cdx.py`
- Modify: `src/creeper/evidence/worker.py`
- Modify: `src/creeper/storage/control_store.py`
- Modify: `src/creeper/storage/evidence_queue.py`
- Test: `tests/unit/test_evidence_planner.py`
- Test: `tests/integration/test_async_cdx_http_client.py`
- Test: `tests/integration/test_async_evidence_worker.py`

**Interfaces:**
- Consumes: contiguous missing-year ranges and existing durable `EvidenceQueryKey` temporal scopes.
- Produces: `RangeEvidenceQueryResult`, provider `query_range(key)`, and atomic range-finish/follow-up-exact-task creation.

- [ ] **Step 1: Write failing planner tests proving `[1996,1997,2000,2001]` becomes two range tasks and missing years are never queried individually first.**
- [ ] **Step 2: Write failing provider tests for range-empty, partial-hit, incomplete, and repeated-resume-key responses.**
- [ ] **Step 3: Write failing worker tests proving range PASS atomically creates only candidate exact-year tasks and range EMPTY creates none.**
- [ ] **Step 4: Implement range result types and the async range probe using the existing HTTPX page iterator.**
- [ ] **Step 5: Implement atomic durable task graph transition and worker dispatch by temporal scope.**
- [ ] **Step 6: Run focused range/provider/worker tests and existing exact-year tests.**

### Task 6: Make runtime Submission EED self-contained

**Files:**
- Modify: `src/creeper/runtime/submission.py`
- Modify: `src/creeper/submission/builder.py`
- Modify: `src/creeper/submission/precheck.py`
- Test: `tests/integration/test_runtime_submission.py`
- Test: `tests/unit/test_snapshot_builder.py`

**Interfaces:**
- Consumes: `EvidenceStore`, `BaselineIndex`, current EED model path, and baseline EED authority value.
- Produces: `build_runtime_snapshot(..., eed_model_path, baseline_eed)` that derives `novel_eed` and `growth_rate` internally.

- [ ] **Step 1: Write a failing test where context supplies an incorrect `novel_eed` and `growth_rate`, but the snapshot uses the recomputed values.**
- [ ] **Step 2: Write a failing test where baseline refresh changes novelty and the recomputed EED without requerying evidence.**
- [ ] **Step 3: Implement calculation from the normalized novel annual pair set using the official EED model.**
- [ ] **Step 4: Keep context values only as rejected legacy inputs or remove them from the runtime constructor after all callers migrate.**
- [ ] **Step 5: Run runtime submission, snapshot, EED golden, and verifier tests.**

### Task 7: Full verification and handoff

**Files:**
- Modify: `docs/implementation-status-v2.1.md` or its current successor with measured status
- Create: `docs/competition-readiness/production-closure-verification.md`

- [ ] **Step 1: Run `uv lock --check`.**
- [ ] **Step 2: Run `uv run python -m unittest discover -s tests -t .`.**
- [ ] **Step 3: Run `uv build`.**
- [ ] **Step 4: Run `git diff --check` and inspect all changed files.**
- [ ] **Step 5: Record which blockers are closed and which remain empirical, without claiming EED/day competitiveness until a real source run exists.**
- [ ] **Step 6: Commit the implementation in task-sized commits.**

## Deferred after this plan

- Remaining Novel EED stock and reserve-hours controller.
- Dynamic Domain/Reservoir bandit scheduling.
- Making Agent optional in the complete discovery composition root.
- 12–24 hour real-source EED/day soak and actual 5% submission batch.
