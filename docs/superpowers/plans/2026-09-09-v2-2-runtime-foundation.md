# Creeper V2.2 Runtime Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Build the synchronous V2.2 runtime foundation so production work is finite, recoverable, and scored by expected downstream novel EED.

**Architecture:** Preserve authority, evidence policy, source parsers, and submission. Add a SQLite-WAL control plane, bounded queues, streaming baseline batches, and a single evidence writer. Use current synchronous providers in this phase.

**Tech Stack:** Python 3.12 standard library, SQLite WAL, unittest, uv.

## Global Constraints

- Retain SQLite BaselineIndex; do not add LMDB, httpx, async workers, new sources, Agent integration, or bandits.
- Keep annual and Candidate metrics separate.
- JSONL is audit export only; SQLite task state is the resume authority.
- Evidence terminal states are PASS, EMPTY_EXHAUSTIVE, and INVALID.
- Every production action is bounded by WorkLease and queue capacity.
- Run the full offline unittest suite and uv build before each integration handoff.

---

### Task 1: Define exact evidence keys and correct CDX completion

**Files:**
- Modify: src/creeper/evidence/policies.py
- Modify: src/creeper/evidence/providers/cdx.py
- Modify: tests/unit/test_evidence_policy.py
- Modify: tests/integration/test_cdx_http_client.py

**Interfaces:**
- Produces: TemporalScope(year_from, year_to).
- Produces: EvidenceQueryKey(hostname, temporal_scope, provider, policy_version).
- Produces: EvidenceQueryResult.key.
- Consumed by: Tasks 2 and 7.

- [ ] **Step 1: Write failing tests**

Add these tests to tests/unit/test_evidence_policy.py:

    def test_completed_second_empty_page_is_exhaustive(self):
        result = query_year("example.com", 1997, lambda h, y: [([], False), ([], True)])
        self.assertEqual(result.state, CDXQueryState.EMPTY_EXHAUSTIVE)

    def test_no_page_transport_is_incomplete(self):
        result = query_year("example.com", 1997, lambda h, y: [])
        self.assertEqual(result.state, CDXQueryState.INCOMPLETE)

    def test_query_key_normalizes_and_separates_provider_policy(self):
        scope = TemporalScope(1997, 1997)
        key = EvidenceQueryKey(" EXAMPLE.COM ", scope, "wayback", "v1")
        self.assertEqual(key.hostname, "example.com")
        self.assertNotEqual(key, EvidenceQueryKey("example.com", scope, "arquivo", "v1"))

Add a WaybackCDXClient integration fixture where a resume-key page is empty and incomplete, then a final empty page is complete; assert EMPTY_EXHAUSTIVE and pages_seen equals 2.

- [ ] **Step 2: Verify RED**

Run: uv run python -m unittest tests.unit.test_evidence_policy tests.integration.test_cdx_http_client -v

Expected: missing key types and current two-page empty query returns INCOMPLETE.

- [ ] **Step 3: Implement minimal production behavior**

Add frozen TemporalScope and EvidenceQueryKey dataclasses. In EvidenceQueryKey.__post_init__, normalize hostname using normalize_official and require 1996 <= year_from <= year_to <= 2001.

Extend EvidenceQueryResult:

    key: EvidenceQueryKey
    hostname: str
    year: int
    state: CDXQueryState

In query_year, build one exact-year key, track last_page_complete as bool | None, and return:
- PASS immediately after a valid record;
- EMPTY_EXHAUSTIVE only if last_page_complete is True;
- INCOMPLETE if no page was yielded or the final page is incomplete.

Make query_missing_years accept provider and policy_version keyword arguments and forward both values.

- [ ] **Step 4: Verify GREEN**

Run: uv run python -m unittest tests.unit.test_evidence_policy tests.integration.test_cdx_http_client -v
Run: uv run python -m unittest discover -s tests -v

Expected: all focused and existing evidence tests pass.

- [ ] **Step 5: Commit**

    git add src/creeper/evidence/policies.py src/creeper/evidence/providers/cdx.py tests/unit/test_evidence_policy.py tests/integration/test_cdx_http_client.py
    git commit -m "fix: make evidence query completion and identity exact"

### Task 2: Persist evidence tasks and batch evidence commits

**Files:**
- Create: src/creeper/storage/control_store.py
- Create: src/creeper/storage/commit_writer.py
- Modify: src/creeper/storage/evidence_store.py
- Modify: src/creeper/evidence/batch.py
- Create: tests/unit/test_control_store.py
- Modify: tests/unit/test_evidence_store.py
- Modify: tests/integration/test_evidence_batch.py
- Create: tests/integration/test_commit_writer.py

**Interfaces:**
- Consumes: EvidenceQueryKey and EvidenceCapsule from Task 1.
- Produces: ControlStore.enqueue_evidence_tasks, claim_evidence_tasks, finish_evidence_task.
- Produces: EvidenceStore.put_many and CommitWriter.flush.
- Consumed by: Tasks 6 and 7.

- [ ] **Step 1: Write failing tests**

Create test_control_store.py:

    key_a = EvidenceQueryKey("example.com", TemporalScope(1997, 1997), "wayback", "v1")
    key_b = EvidenceQueryKey("example.com", TemporalScope(1997, 1997), "arquivo", "v1")
    self.assertEqual(store.enqueue_evidence_tasks([key_a, key_b]), 2)
    self.assertEqual(len(store.claim_evidence_tasks(owner="worker-1", limit=10)), 2)

Extend test_evidence_store.py with:

    store.put_many([capsule_v1, capsule_v2])
    self.assertEqual(store.count(), 2)

where capsules use the same payload hash but different policy versions.

Extend integration batch tests: same hostname/year with different provider or policy executes twice; identical full key skips only after terminal state; INCOMPLETE and TRANSIENT_ERROR claim again.

Create commit writer test: two submit calls followed by flush write through one put_many call and a repeated flush is idempotent.

- [ ] **Step 2: Verify RED**

Run: uv run python -m unittest tests.unit.test_control_store tests.unit.test_evidence_store tests.integration.test_evidence_batch tests.integration.test_commit_writer -v

Expected: import failures for control store, put_many, and writer APIs.

- [ ] **Step 3: Implement durable task and write paths**

Create ControlStore with WAL and tables:
- evidence_tasks(hostname, year_from, year_to, provider, policy_version, state, attempt, retry_at, lease_owner, lease_until)
- runtime_checkpoints(key, value)

Use the five identity columns as the primary key. claim_evidence_tasks selects PENDING, INCOMPLETE, or TRANSIENT_ERROR whose lease is absent or expired, assigns owner and lease_until in one transaction, and increments attempt.

Add EvidenceStore.put_many(capsules) with one transaction. Migrate the old evidence table by creating evidence_capsules_v2 with primary key hostname, year, provider, payload_hash, policy_version, copying all old rows, and using the v2 table for all reads and writes. Keep put(capsule) as put_many([capsule]).

Create CommitWriter with submit(capsule, result), flush(), and close(). flush writes the accumulated capsules with put_many, then finishes their evidence tasks through ControlStore.

Refactor EvidenceBatchRunner to enqueue finite batches, claim by complete key, and export JSONL after state changes. Remove JSONL scanning and list(dict.fromkeys(...)) from its production path.

- [ ] **Step 4: Verify GREEN**

Run: uv run python -m unittest tests.unit.test_control_store tests.unit.test_evidence_store tests.integration.test_evidence_batch tests.integration.test_commit_writer -v
Run: uv run python -m unittest discover -s tests -v

Expected: exact-key resume, policy provenance, retry behavior, and batched persistence pass.

- [ ] **Step 5: Commit**

    git add src/creeper/storage/control_store.py src/creeper/storage/commit_writer.py src/creeper/storage/evidence_store.py src/creeper/evidence/batch.py tests/unit/test_control_store.py tests/unit/test_evidence_store.py tests/integration/test_evidence_batch.py tests/integration/test_commit_writer.py
    git commit -m "feat: persist evidence tasks and batch evidence commits"

### Task 3: Add SourceDomain, Reservoir, and WorkLease models

**Files:**
- Create: src/creeper/sources/domains.py
- Create: src/creeper/sources/reservoirs.py
- Create: src/creeper/scheduler/leases.py
- Modify: src/creeper/storage/control_store.py
- Modify: src/creeper/sources/base.py
- Modify: src/creeper/scheduler/budgets.py
- Modify: src/creeper/records/models.py
- Create: tests/unit/test_runtime_models.py
- Create: tests/unit/test_work_leases.py

**Interfaces:**
- Produces: SourceDomain, Reservoir, ReservoirEstimate, WorkLease, LeaseState, LeaseResult.
- Produces: ReservoirAdapter.estimate and ReservoirAdapter.execute.
- Produces: ControlStore.save_domain, save_reservoir, save_lease, and recover_expired_leases.
- Consumed by: Tasks 5, 6, and 7.

- [ ] **Step 1: Write failing tests**

Create tests for these exact limits:

    lease = WorkLease.create(
        reservoir_id="arquivo:demo", max_records=10, max_requests=2,
        max_bytes=4096, max_seconds=30,
    )
    self.assertTrue(lease.allows(records=9, requests=1, bytes_read=4095, elapsed_seconds=29.9))
    self.assertFalse(lease.allows(records=10, requests=1, bytes_read=1, elapsed_seconds=1))
    self.assertEqual(lease.expire(now), LeaseState.EXPIRED)

Also assert a reservoir cannot enter RUNNING without a granted lease, a terminal lease cannot be resumed, and retry creates a distinct lease id.

- [ ] **Step 2: Verify RED**

Run: uv run python -m unittest tests.unit.test_runtime_models tests.unit.test_work_leases -v

Expected: model imports fail.

- [ ] **Step 3: Implement model and adapter boundaries**

Use StrEnum for DomainState, ReservoirState, and LeaseState. WorkLease includes cursor start/end, all four limits, expected external evidence tasks, expected novel EED, owner, expiry, and state. allows rejects any exhausted dimension.

Extend SourceRecord and HostObservation with defaulted record_type, source_time, artifact_ref, direct_year_mask, and year_hint_mask fields. Keep existing field order and defaults compatible with existing adapters.

Retain SourceBudget as a documented legacy compatibility helper. Add ReservoirAdapter protocol:

    class ReservoirAdapter(Protocol):
        def estimate(self) -> ReservoirEstimate: ...
        def execute(self, lease: WorkLease) -> Iterator[SourceRecord]: ...

Extend ControlStore's Task 2 schema with source_domains, reservoirs, and
work_leases tables. Store enum values as text, retain every cursor boundary,
and reject a reservoir whose domain has not first been saved. save_lease must
reject a transition to RUNNING unless the lease is already GRANTED. On startup,
recover_expired_leases changes only active leases whose expiry is before now to
EXPIRED; it must never reopen a terminal lease.

- [ ] **Step 4: Verify GREEN**

Run: uv run python -m unittest tests.unit.test_runtime_models tests.unit.test_work_leases tests.unit.test_auxiliary_source tests.unit.test_arquivo_source -v
Run: uv run python -m unittest discover -s tests -v

Expected: V2.2 model semantics pass without breaking adapters.

- [ ] **Step 5: Commit**

    git add src/creeper/sources/domains.py src/creeper/sources/reservoirs.py src/creeper/scheduler/leases.py src/creeper/storage/control_store.py src/creeper/sources/base.py src/creeper/scheduler/budgets.py src/creeper/records/models.py tests/unit/test_runtime_models.py tests/unit/test_work_leases.py tests/unit/test_control_store.py
    git commit -m "feat: add source domains reservoirs and work leases"

### Task 4: Stream baseline resolution

**Files:**
- Modify: src/creeper/authority/baseline_index.py
- Modify: tests/unit/test_baseline_index.py
- Create: tests/unit/test_baseline_streaming.py

**Interfaces:**
- Produces: BaselineIndex.iter_resolve_batches(hostnames, input_batch_size=50000, chunk_size=900).
- Consumed by: Task 7.

- [ ] **Step 1: Write failing streaming tests**

Use a guarded generator that fails if more than two values are consumed before the first output:

    first = next(index.iter_resolve_batches(guarded_hosts(), input_batch_size=2))
    self.assertEqual(set(first), {"present.example", "absent.example"})

Also assert invalid inputs are omitted, duplicate inputs in one batch resolve once, and union of output maps equals resolve_batch for the same finite input.

- [ ] **Step 2: Verify RED**

Run: uv run python -m unittest tests.unit.test_baseline_streaming -v

Expected: iter_resolve_batches is absent.

- [ ] **Step 3: Implement bounded iteration**

Use itertools.islice to collect at most input_batch_size raw hosts. Delegate each finite group to existing resolve_batch and yield it before reading the next group. Reject input_batch_size below 1 and retain the 900 SQL placeholder limit.

- [ ] **Step 4: Verify GREEN**

Run: uv run python -m unittest tests.unit.test_baseline_streaming tests.unit.test_baseline_index -v
Run: uv run python -m unittest discover -s tests -v

Expected: streaming is bounded and annual/Candidate masks are unchanged.

- [ ] **Step 5: Commit**

    git add src/creeper/authority/baseline_index.py tests/unit/test_baseline_index.py tests/unit/test_baseline_streaming.py
    git commit -m "feat: stream baseline resolution in bounded batches"

### Task 5: Add credits, bounded queues, and governor control outputs

**Files:**
- Create: src/creeper/scheduler/credits.py
- Create: src/creeper/runtime/queues.py
- Modify: src/creeper/runtime/resource_governor.py
- Create: tests/unit/test_credit_accounting.py
- Create: tests/unit/test_bounded_queues.py
- Modify: tests/unit/test_resource_governor.py

**Interfaces:**
- Produces: ResourceCredits and CreditLedger.reserve_evidence/release_evidence.
- Produces: BoundedQueues and ResourceGovernor.credits.
- Consumed by: Tasks 6 and 7.

- [ ] **Step 1: Write failing tests**

    ledger = CreditLedger({"wayback": 5})
    ledger.note_queued("wayback", 2)
    self.assertFalse(ledger.reserve_evidence("wayback", 4))
    self.assertTrue(ledger.reserve_evidence("wayback", 3))

Assert queue.Queue raises queue.Full at configured capacity. Assert DRAIN_ONLY returns zero source_fetch credits but positive parse and commit credits.

- [ ] **Step 2: Verify RED**

Run: uv run python -m unittest tests.unit.test_credit_accounting tests.unit.test_bounded_queues tests.unit.test_resource_governor -v

Expected: new modules and governor credits method are absent.

- [ ] **Step 3: Implement finite resource controls**

Use per-provider capacity, queued, claimed, and reserved counters. A reservation is valid only when capacity minus the other three counters covers it.

Create BoundedQueues with named queue.Queue(maxsize=...) fields for source records, observations, evidence tasks, and commits. Add ResourceGovernor.credits(sample, capacities), preserving evaluate. NORMAL, THROTTLED, DRAIN_ONLY, and EMERGENCY_STOP must map to explicit credit values; DRAIN_ONLY blocks source fetching but keeps parse and commit positive.

- [ ] **Step 4: Verify GREEN**

Run: uv run python -m unittest tests.unit.test_credit_accounting tests.unit.test_bounded_queues tests.unit.test_resource_governor -v
Run: uv run python -m unittest discover -s tests -v

Expected: all queues and upstream grants are bounded.

- [ ] **Step 5: Commit**

    git add src/creeper/scheduler/credits.py src/creeper/runtime/queues.py src/creeper/runtime/resource_governor.py tests/unit/test_credit_accounting.py tests/unit/test_bounded_queues.py tests/unit/test_resource_governor.py
    git commit -m "feat: add runtime credits and bounded queues"

### Task 6: Build deterministic downstream-EED lease scheduling

**Files:**
- Create: src/creeper/scheduler/global_scheduler.py
- Modify: src/creeper/scheduler/priority.py
- Create: tests/unit/test_scheduler_eed.py
- Modify: tests/unit/test_scheduler.py

**Interfaces:**
- Consumes: Reservoir, WorkLease, and CreditLedger.
- Produces: GlobalScheduler.rank and GlobalScheduler.grant_next.
- Consumed by: Task 7.

- [ ] **Step 1: Write failing scheduler tests**

    direct = LeaseCandidate(
        reservoir_id="direct", expected_novel_eed=10,
        costs=ResourceCost(general_network=1, evidence_network=0, cpu=1, ssd=1),
    )
    cdx_heavy = LeaseCandidate(
        reservoir_id="cdx", expected_novel_eed=20,
        costs=ResourceCost(general_network=1, evidence_network=100, cpu=1, ssd=1),
    )
    self.assertEqual(scheduler.rank([cdx_heavy, direct])[0].reservoir_id, "direct")

Add a grant test proving discovery-only work is rejected when evidence credits cannot reserve expected tasks, while a direct-year candidate is grantable at the same provider pressure.

- [ ] **Step 2: Verify RED**

Run: uv run python -m unittest tests.unit.test_scheduler_eed -v

Expected: GlobalScheduler and resource-cost types are absent.

- [ ] **Step 3: Implement the phase-1 scheduler**

Define non-negative ResourceCost and LeaseCandidate. Priority is expected_novel_eed divided by the maximum normalized cost. Resolve ties by reservoir_id.

grant_next reserves external evidence credits before moving a reservoir to LEASED. Direct-year candidates reserve zero evidence credits. Retain rank_sources only for pilots and label its reason as engineering_only_baseline_external.

- [ ] **Step 4: Verify GREEN**

Run: uv run python -m unittest tests.unit.test_scheduler_eed tests.unit.test_scheduler -v
Run: uv run python -m unittest discover -s tests -v

Expected: production ranking uses expected novel EED, while legacy pilot ranking stays deterministic.

- [ ] **Step 5: Commit**

    git add src/creeper/scheduler/global_scheduler.py src/creeper/scheduler/priority.py tests/unit/test_scheduler_eed.py tests/unit/test_scheduler.py
    git commit -m "feat: schedule work leases by expected novel EED"

### Task 7: Assemble the synchronous bounded runtime and CLI

**Files:**
- Create: src/creeper/runtime/pipeline.py
- Modify: src/creeper/cli.py
- Modify: conf/creeper.example.toml
- Modify: src/creeper/sources/local/static_dataset.py
- Create: tests/integration/test_sync_runtime.py
- Modify: tests/unit/test_auxiliary_source.py
- Modify: tests/integration/test_submission_package.py

**Interfaces:**
- Consumes: Tasks 1 through 6.
- Produces: SyncRuntime.run_once and creeper run --once.

- [ ] **Step 1: Write failing offline vertical-slice test**

Create a fake ReservoirAdapter yielding two SourceRecords under a WorkLease, a temporary baseline index, and injected synchronous evidence transport.

    report = runtime.run_once()
    self.assertEqual(report.leases_succeeded, 1)
    self.assertEqual(report.evidence_tasks_completed, 1)
    self.assertEqual(report.evidence_capsules_committed, 1)
    self.assertLessEqual(report.max_evidence_queue_depth, 2)

Add a CLI test calling main(["run", "--once", ...]) with a tiny local static
source file and asserting JSON reports the completed lease. Do not make real
network calls.

- [ ] **Step 2: Verify RED**

Run: uv run python -m unittest tests.integration.test_sync_runtime -v

Expected: runtime module and run command are absent.

- [ ] **Step 3: Implement SyncRuntime.run_once**

The method must:
1. obtain one grantable lease from GlobalScheduler;
2. execute only records inside its WorkLease limits;
3. process bounded observations through iter_resolve_batches;
4. emit direct capsules or durable EvidenceTasks;
5. claim no more than configured evidence tasks and call the existing synchronous provider;
6. submit accepted capsules to CommitWriter, flush, and release credits;
7. set final lease state and return queue high-water marks.

Add creeper run --once options for baseline index, control database, evidence database, and config. Reject any zero or negative queue capacity or lease maximum.

Make the existing StaticDatasetAdapter implement ReservoirAdapter for a local
text file. Its cursor is an inclusive line number; execute must stop when the
lease reaches max_records, max_bytes, or max_seconds and report the next line
as its next cursor. The phase-1 configuration supports only this local-static
adapter, which is sufficient to make the CLI loop executable without adding an
external source.

- [ ] **Step 4: Verify GREEN**

Run: uv run python -m unittest tests.integration.test_sync_runtime tests.integration.test_submission_package tests.integration.test_submission_verifier -v
Run: uv run python -m unittest discover -s tests -v

Expected: an offline source-to-evidence-to-submission-compatible path is bounded and repeatable.

- [ ] **Step 5: Commit**

    git add src/creeper/runtime/pipeline.py src/creeper/cli.py conf/creeper.example.toml src/creeper/sources/local/static_dataset.py tests/integration/test_sync_runtime.py tests/unit/test_auxiliary_source.py tests/integration/test_submission_package.py
    git commit -m "feat: add bounded synchronous runtime loop"

### Task 8: Add offline CI, operations documentation, and final gates

**Files:**
- Create: .github/workflows/ci.yml
- Modify: README.md
- Modify: docs/implementation-status-v2.1.md
- Create: docs/v2-2-runtime-operations.md
- Create: tests/unit/test_ci_layout.py

**Interfaces:**
- Consumes: creeper run --once from Task 7.
- Produces: repeatable offline CI and operator runbook.

- [ ] **Step 1: Write failing CI layout test**

    workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
    self.assertIn("uv run python -m unittest discover -s tests -v", workflow)
    self.assertIn("uv build", workflow)
    self.assertNotIn("run_evidence_pilot.py", workflow)

- [ ] **Step 2: Verify RED**

Run: uv run python -m unittest tests.unit.test_ci_layout -v

Expected: workflow file is absent.

- [ ] **Step 3: Implement offline CI and documentation**

Create CI for push and pull_request using Python 3.12 and uv. Run the full unittest discovery and uv build only; never invoke a real-network pilot.

Document configuration limits for run --once, control-store backup scope, state-to-credit behavior, annual/Candidate metric separation, queue high-water marks, and run-stage throughput. Update README and implementation status to call the result a synchronous bounded V2.2 runtime foundation.

- [ ] **Step 4: Verify final integration**

Run: uv run python -m unittest discover -s tests -v
Run: uv build
Run: git diff --check
Run: git status --short --branch

Expected: all tests and build pass, no whitespace errors exist, and only intended tracked changes remain.

- [ ] **Step 5: Commit**

    git add .github/workflows/ci.yml README.md docs/implementation-status-v2.1.md docs/v2-2-runtime-operations.md tests/unit/test_ci_layout.py
    git commit -m "ci: verify V2.2 runtime foundation offline"

## Completion checklist

- [ ] The eight task commits are present in dependency order.
- [ ] The full offline suite and uv build pass after Task 8.
- [ ] The runtime vertical slice makes no external network call.
- [ ] Performance reporting records stage throughput and queue high-water marks but makes no annual EED/day claim without accepted real evidence.
- [ ] Push the reviewed implementation only after user requests publication.
