# Durable Sequential Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use subagent-driven development (recommended) or executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Turn the one-lease offline smoke test into a restart-safe, lossless sequential runtime that advances durable Reservoir cursors and can produce an offline submission snapshot.

**Architecture:** ControlStore becomes the durable authority for Reservoir ownership and progress. GlobalScheduler only ranks candidates and accounts for bounded provider backlog. EvidencePlanner is the pure Host-Year decision boundary; SyncRuntime applies direct facts and external queries through bounded drain cycles.

**Tech Stack:** Python 3.12, stdlib sqlite3 and queue, unittest, existing SQLite BaselineIndex, EvidenceStore, ControlStore, and submission builder.

## Global Constraints

- Keep baseline authority in SQLite; do not introduce LMDB.
- Keep the runtime synchronous; do not add httpx, asyncio, provider pools, rate limiters, RangeProbe, or external source adapters.
- ControlStore is the only authority for Reservoir and lease ownership.
- General cursor values remain opaque strings. StaticDatasetAdapter alone interprets them as decimal byte offsets.
- Queue saturation must pause and drain; it must never discard work.
- direct_year_mask creates direct evidence. source_year and year_hint_mask are hints that require external evidence.
- Keep control.sqlite3 and evidence.sqlite3 separate in this phase; recovery must remain idempotent across their crash boundary.
- Follow TDD for every behavior: add a test, observe the expected failure, implement the minimum, then rerun focused and regression tests.

---

### Task 1: Restore persisted model states as facts

**Files:**
- Modify: src/creeper/storage/control_store.py
- Test: tests/unit/test_control_store.py

**Interfaces:**
- Consumes: SourceDomain, DomainState, Reservoir, and ReservoirState.
- Produces: get_domain(domain_id) and get_reservoir(reservoir_id) that preserve every stored enum state.

- [ ] **Step 1: Write the failing recovery test**

~~~python
def test_restores_non_initial_domain_and_reservoir_states(self):
    productive = self.domain.transition(DomainState.EXPLORING).transition(
        DomainState.PRODUCTIVE
    )
    ready = self.reservoir.transition(ReservoirState.QUALIFYING).transition(
        ReservoirState.READY
    )
    self.store.save_domain(productive)
    self.store.save_reservoir(ready)

    self.assertEqual(self.store.get_domain(productive.domain_id).state,
                     DomainState.PRODUCTIVE)
    self.assertEqual(self.store.get_reservoir(ready.reservoir_id).state,
                     ReservoirState.READY)
~~~

- [ ] **Step 2: Run the test and verify RED**

Run:

~~~bash
uv run python -m unittest tests.unit.test_control_store.ControlStoreTests.test_restores_non_initial_domain_and_reservoir_states -v
~~~

Expected: StateTransitionError caused by replaying an illegal initial-state transition.

- [ ] **Step 3: Deserialize exact enum states**

Construct SourceDomain with state equal to DomainState(row state), and Reservoir with state equal to ReservoirState(row state). Do not invoke transition methods during database reads.

- [ ] **Step 4: Run focused tests and verify GREEN**

~~~bash
uv run python -m unittest tests.unit.test_control_store -v
~~~

Expected: all ControlStore tests pass.

- [ ] **Step 5: Commit**

~~~bash
git add src/creeper/storage/control_store.py tests/unit/test_control_store.py
git commit -m "fix: restore persisted domain and reservoir states"
~~~

### Task 2: Atomically grant fresh cursor-backed leases

**Files:**
- Modify: src/creeper/storage/control_store.py
- Modify: src/creeper/scheduler/global_scheduler.py
- Test: tests/unit/test_control_store.py
- Test: tests/unit/test_scheduler_eed.py

**Interfaces:**
- Consumes: ranked LeaseCandidate values and a persisted READY Reservoir.
- Produces:

~~~python
ControlStore.grant_fresh_lease(
    reservoir_id: str,
    *,
    owner: str,
    max_records: int,
    max_requests: int,
    max_bytes: int,
    max_seconds: float,
    resource_class: str,
    expected_evidence_tasks: int,
    expected_novel_eed: float,
    now: float,
) -> WorkLease | None
~~~

A returned lease is newly created, GRANTED, and starts at the durable Reservoir cursor. None means the Reservoir was not READY.

- [ ] **Step 1: Write the failing double-grant test**

~~~python
def test_fresh_grant_uses_cursor_and_prevents_second_claim(self):
    ready = self.ready_reservoir(cursor="128")
    self.store.save_reservoir(ready)

    first = self.store.grant_fresh_lease(
        ready.reservoir_id, owner="worker-a", now=100.0, **self.lease_limits()
    )
    second = self.store.grant_fresh_lease(
        ready.reservoir_id, owner="worker-b", now=100.0, **self.lease_limits()
    )

    self.assertEqual(first.cursor_start, "128")
    self.assertEqual(first.state, LeaseState.GRANTED)
    self.assertIsNone(second)
    self.assertEqual(self.store.get_reservoir(ready.reservoir_id).state,
                     ReservoirState.LEASED)
~~~

- [ ] **Step 2: Run the test and verify RED**

~~~bash
uv run python -m unittest tests.unit.test_control_store.ControlStoreTests.test_fresh_grant_uses_cursor_and_prevents_second_claim -v
~~~

Expected: AttributeError because grant_fresh_lease does not exist.

- [ ] **Step 3: Implement transactional grant**

Use BEGIN IMMEDIATE. Read the Reservoir, require READY, create a new WorkLease with WorkLease.create and now, grant it to owner, insert it, update the Reservoir to LEASED, and commit. Roll back on every exception. Never reuse candidate.lease.lease_id.

- [ ] **Step 4: Write the failing expired-recovery test**

~~~python
def test_expired_lease_restores_reservoir_at_prior_cursor(self):
    lease = self.grant_and_start(cursor="256", expires_at=10.0)
    changed = self.store.recover_expired_leases(now=11.0)

    self.assertEqual(changed, 1)
    self.assertEqual(self.store.get_lease(lease.lease_id).state, LeaseState.EXPIRED)
    restored = self.store.get_reservoir(lease.reservoir_id)
    self.assertEqual((restored.state, restored.cursor),
                     (ReservoirState.READY, "256"))
~~~

- [ ] **Step 5: Run the recovery test and verify RED**

~~~bash
uv run python -m unittest tests.unit.test_control_store.ControlStoreTests.test_expired_lease_restores_reservoir_at_prior_cursor -v
~~~

Expected: Reservoir remains LEASED or RUNNING.

- [ ] **Step 6: Recover lease and Reservoir in one transaction**

Select expired GRANTED or RUNNING lease ids, mark them EXPIRED, and return their Reservoirs to READY without changing cursor. Preserve terminal Reservoir states.

- [ ] **Step 7: Make GlobalScheduler pure**

Keep score and rank. Remove the in-memory Reservoir ownership map and pre-created lease reuse. Add a test proving two calls to rank do not mutate candidates or Reservoirs. Runtime will reserve credits before the durable claim and release them if grant_fresh_lease returns None.

- [ ] **Step 8: Run focused tests and commit**

~~~bash
uv run python -m unittest tests.unit.test_control_store tests.unit.test_scheduler_eed tests.unit.test_work_leases -v
git add src/creeper/storage/control_store.py src/creeper/scheduler/global_scheduler.py tests/unit/test_control_store.py tests/unit/test_scheduler_eed.py
git commit -m "feat: grant durable cursor-backed work leases"
~~~

### Task 3: Make static dataset progress linear

**Files:**
- Modify: src/creeper/sources/local/static_dataset.py
- Test: tests/unit/test_auxiliary_source.py

**Interfaces:**
- Consumes: cursor_start as None or a decimal byte offset.
- Produces: next_cursor as the byte offset before the first unconsumed complete line, or None at EOF.

- [ ] **Step 1: Write the failing byte-cursor test**

~~~python
def test_static_dataset_resumes_from_byte_cursor(self):
    path.write_bytes(b"one.example\ntwo.example\nthree.example\n")
    first_records, first_result = adapter.execute(
        lease(cursor_start=None, max_records=2)
    )
    second_records, second_result = adapter.execute(
        lease(cursor_start=first_result.next_cursor, max_records=2)
    )

    self.assertEqual([item.payload for item in first_records],
                     ["one.example", "two.example"])
    self.assertEqual([item.payload for item in second_records],
                     ["three.example"])
    self.assertIsNone(second_result.next_cursor)
~~~

- [ ] **Step 2: Run the test and verify RED**

~~~bash
uv run python -m unittest tests.unit.test_auxiliary_source.AuxiliarySourceTests.test_static_dataset_resumes_from_byte_cursor -v
~~~

Expected: current line-number cursor replays or skips the wrong record.

- [ ] **Step 3: Implement binary seek**

Open the source in binary mode, seek to int(cursor_start or zero), record line_start before readline, and keep complete-line byte accounting. If accepting a line would exceed a lease limit, return line_start as next_cursor. After acceptance, use source.tell. Return None only at EOF. Put the byte offset in the SourceRecord locator.

- [ ] **Step 4: Add byte-limit protection**

Write a test where the next line exceeds remaining max_bytes. Assert it is not emitted and next_cursor equals that line start. If zero records can ever fit, return the unchanged starting cursor so the caller can detect a non-progressing lease.

- [ ] **Step 5: Run focused tests and commit**

~~~bash
uv run python -m unittest tests.unit.test_auxiliary_source -v
git add src/creeper/sources/local/static_dataset.py tests/unit/test_auxiliary_source.py
git commit -m "feat: resume static reservoirs by byte cursor"
~~~

### Task 4: Add EvidencePlanner and batch local state operations

**Files:**
- Create: src/creeper/evidence/planner.py
- Modify: src/creeper/records/models.py
- Modify: src/creeper/storage/evidence_store.py
- Modify: src/creeper/storage/control_store.py
- Modify: src/creeper/storage/commit_writer.py
- Create: tests/unit/test_evidence_planner.py
- Modify: tests/unit/test_evidence_store.py
- Modify: tests/unit/test_control_store.py
- Modify: tests/integration/test_commit_writer.py

**Interfaces:**

~~~python
@dataclass(frozen=True)
class EvidencePlan:
    direct_capsules: tuple[EvidenceCapsule, ...]
    external_keys: tuple[EvidenceQueryKey, ...]

class EvidencePlanner:
    def plan(
        self,
        observation: HostObservation,
        *,
        official_mask: int,
        local_mask: int,
        provider: str,
        policy_version: str,
    ) -> EvidencePlan: ...

EvidenceStore.resolve_year_masks(
    hostnames: Iterable[str],
) -> dict[str, int]

ControlStore.finish_evidence_tasks(
    results: Iterable[EvidenceQueryResult],
    *,
    owner: str,
) -> int
~~~

- [ ] **Step 1: Write failing planner tests**

~~~python
def test_direct_mask_creates_capsule_without_external_key(self):
    plan = planner.plan(
        observation(direct_year_mask=YEAR_BITS[1997]),
        official_mask=0,
        local_mask=0,
        provider="wayback",
        policy_version="v1",
    )
    self.assertEqual([item.year for item in plan.direct_capsules], [1997])
    self.assertEqual(plan.external_keys, ())

def test_hint_mask_creates_external_key_only_when_missing(self):
    plan = planner.plan(
        observation(year_hint_mask=YEAR_BITS[1998]),
        official_mask=0,
        local_mask=0,
        provider="wayback",
        policy_version="v1",
    )
    self.assertEqual(
        [(key.hostname, key.temporal_scope.year_from)
         for key in plan.external_keys],
        [("new.example", 1998)],
    )
~~~

Also assert official and local masks suppress output and source_year acts as a hint, never direct evidence.

- [ ] **Step 2: Run planner tests and verify RED**

~~~bash
uv run python -m unittest tests.unit.test_evidence_planner -v
~~~

Expected: missing planner module.

- [ ] **Step 3: Implement the pure planner**

For each missing direct bit create an EvidenceCapsule with provider direct plus source id, temporal semantics source_direct_year, observation locator, and a SHA-256 of canonical hostname, year, source id, and locator. For missing hint bits and source_year create EvidenceQueryKey values. Direct takes precedence if the same year appears in both masks. Add only defaulted provenance fields to HostObservation so legacy constructors remain valid.

- [ ] **Step 4: Write failing batch-storage tests**

~~~python
def test_resolve_year_masks_returns_masks_for_every_requested_host(self):
    self.store.put_many([
        capsule("a.example", 1996),
        capsule("a.example", 1998),
    ])
    self.assertEqual(
        self.store.resolve_year_masks(["a.example", "missing.example"]),
        {
            "a.example": YEAR_BITS[1996] | YEAR_BITS[1998],
            "missing.example": 0,
        },
    )

def test_finish_evidence_tasks_updates_multiple_owned_results(self):
    results = self.claim_two_results(owner="worker")
    self.assertEqual(
        self.store.finish_evidence_tasks(results, owner="worker"),
        2,
    )
~~~

- [ ] **Step 5: Run batch tests and verify RED**

~~~bash
uv run python -m unittest tests.unit.test_evidence_store tests.unit.test_control_store tests.integration.test_commit_writer -v
~~~

Expected: missing batch methods.

- [ ] **Step 6: Implement bounded batches**

Normalize and deduplicate one caller batch in resolve_year_masks, query hostname and year with SQLite IN chunks no larger than 900, OR YEAR_BITS in Python, and return zero for every valid requested hostname without evidence. Implement finish_evidence_tasks with one transaction and executemany; require owner for every row. Keep finish_evidence_task as a one-item compatibility wrapper. Make CommitWriter.flush call the batch method after put_many.

- [ ] **Step 7: Run focused tests and commit**

~~~bash
uv run python -m unittest tests.unit.test_evidence_planner tests.unit.test_evidence_store tests.unit.test_control_store tests.integration.test_commit_writer -v
git add src/creeper/evidence/planner.py src/creeper/records/models.py src/creeper/storage/evidence_store.py src/creeper/storage/control_store.py src/creeper/storage/commit_writer.py tests/unit/test_evidence_planner.py tests/unit/test_evidence_store.py tests/unit/test_control_store.py tests/integration/test_commit_writer.py
git commit -m "feat: plan evidence in bounded durable batches"
~~~

### Task 5: Advance sequential leases without queue loss

**Files:**
- Modify: src/creeper/runtime/pipeline.py
- Modify: tests/integration/test_sync_runtime.py
- Create: tests/integration/test_sequential_runtime.py

**Interfaces:**
- Consumes: grant_fresh_lease, LeaseResult.next_cursor, EvidencePlanner, resolve_year_masks, and batch task methods.
- Produces: SyncRuntimeReport with reservoir_exhausted and complete high-water/accounting fields.

- [ ] **Step 1: Write failing multi-lease test**

~~~python
def test_two_runs_advance_without_repeating_records(self):
    runtime = self.runtime(
        lines=["one.example", "two.example", "three.example"],
        lease_records=2,
        evidence_capacity=2,
    )
    first = runtime.run_once()
    second = runtime.run_once()

    self.assertEqual(
        self.transport_calls,
        [
            ("one.example", 1997),
            ("two.example", 1997),
            ("three.example", 1997),
        ],
    )
    self.assertFalse(first.reservoir_exhausted)
    self.assertTrue(second.reservoir_exhausted)
~~~

- [ ] **Step 2: Write failing saturation test**

~~~python
def test_full_evidence_queue_drains_without_dropping_hosts(self):
    runtime = self.runtime(
        lines=["a.example", "b.example", "c.example"],
        lease_records=3,
        evidence_capacity=1,
        evidence_queue_capacity=1,
    )
    runtime.run_once()
    self.assertEqual(
        self.transport_calls,
        [
            ("a.example", 1997),
            ("b.example", 1997),
            ("c.example", 1997),
        ],
    )
~~~

- [ ] **Step 3: Write failing restart test**

Run one partial lease, close the stores, construct a new ControlStore, EvidenceStore, GlobalScheduler, and SyncRuntime over the same paths, and assert the new runtime begins at the persisted cursor without repeating prior transport calls.

- [ ] **Step 4: Run tests and verify RED**

~~~bash
uv run python -m unittest tests.integration.test_sync_runtime tests.integration.test_sequential_runtime -v
~~~

Expected: repeated lease ids or records, ignored next_cursor, and lost work when the evidence queue fills.

- [ ] **Step 5: Implement durable acquisition and finalization**

Iterate scheduler.rank output. Reserve predicted provider work, call grant_fresh_lease, and release the reservation when durable claim returns None. Persist RUNNING before adapter execution. Add a ControlStore completion operation that, in one transaction, marks the lease SUCCEEDED, stores next_cursor, and transitions the Reservoir to READY when next_cursor is non-null or EXHAUSTED at EOF. On an exception mark the lease ABORTED and restore READY at its original cursor.

- [ ] **Step 6: Implement bounded drain cycles**

Collect observations only up to the smallest relevant batch or queue capacity. Batch-resolve official and local masks, call EvidencePlanner for every observation, batch-enqueue external keys, then claim, query, and commit before accepting more observations when evidence capacity is reached. Submit direct capsules through the commit path. Never break out of planning because a queue is full.

- [ ] **Step 7: Run focused tests and commit**

~~~bash
uv run python -m unittest tests.integration.test_sync_runtime tests.integration.test_sequential_runtime -v
git add src/creeper/runtime/pipeline.py src/creeper/storage/control_store.py tests/integration/test_sync_runtime.py tests/integration/test_sequential_runtime.py
git commit -m "feat: advance durable leases without queue loss"
~~~

### Task 6: Add crash recovery and submission snapshot integration

**Files:**
- Modify: src/creeper/storage/evidence_store.py
- Create: src/creeper/runtime/submission.py
- Modify: src/creeper/runtime/pipeline.py
- Modify: src/creeper/cli.py
- Modify: tests/integration/test_sequential_runtime.py
- Create: tests/integration/test_runtime_submission.py

**Interfaces:**

~~~python
@dataclass(frozen=True)
class RuntimeSubmissionContext:
    baseline_manifest: dict[str, object]
    code_revision: str
    source_report_set: tuple[str, ...]
    cdx_audit_set: tuple[str, ...]
    eed_report: dict[str, object]

def build_runtime_snapshot(
    *,
    context: RuntimeSubmissionContext,
    evidence_store: EvidenceStore,
    baseline: BaselineIndex,
    snapshot_id: str,
) -> SubmissionSnapshot: ...
~~~

- [ ] **Step 1: Write the failing crash-boundary test**

~~~python
def test_restart_after_capsule_write_keeps_fact_and_retries_task(self):
    key = self.enqueue_and_claim("new.example", 1997)
    self.evidence_store.put(self.passing_capsule(key))
    self.reopen_stores_after_lease_expiry()

    self.assertEqual(self.evidence_store.count(), 1)
    reclaimed = self.control.claim_evidence_tasks(owner="new", limit=1)
    self.assertEqual(reclaimed[0].key, key)
~~~

Retry and write the same capsule again; assert EvidenceStore count remains one.

- [ ] **Step 2: Write the failing direct-runtime test**

~~~python
def test_direct_year_commits_without_provider_capacity_or_transport(self):
    runtime = self.runtime(
        direct_year_mask=YEAR_BITS[1997],
        evidence_capacity=0,
    )
    report = runtime.run_once()

    self.assertEqual(report.evidence_capsules_committed, 1)
    self.assertEqual(self.transport_calls, [])
~~~

- [ ] **Step 3: Write the failing snapshot test**

~~~python
def test_runtime_snapshot_contains_only_novel_committed_evidence(self):
    snapshot = build_runtime_snapshot(
        context=self.valid_context(),
        evidence_store=self.evidence_store,
        baseline=self.baseline,
        snapshot_id="offline-1",
    )
    self.assertTrue(snapshot.ready)
    self.assertEqual(
        [(item.hostname, item.year) for item in snapshot.novel_records],
        [("new.example", 1997)],
    )
~~~

- [ ] **Step 4: Run tests and verify RED**

~~~bash
uv run python -m unittest tests.integration.test_sequential_runtime tests.integration.test_runtime_submission -v
~~~

Expected: direct path still requires provider work and runtime submission bridge is missing.

- [ ] **Step 5: Implement idempotent direct and snapshot paths**

Direct capsules enter EvidenceStore without provider reservation, queueing, claim, transport, or task completion. Add EvidenceStore.all_capsules ordered by hostname, year, and provider. Implement build_runtime_snapshot as a thin adapter over submission.builder.build_snapshot using RuntimeSubmissionContext. Do not create a ZIP or contact external systems.

- [ ] **Step 6: Expose optional snapshot status**

Allow SyncRuntime to receive an optional RuntimeSubmissionContext and add snapshot_ready plus novel_records to SyncRuntimeReport. Let the CLI optionally load this context and report its status. Existing offline configuration remains valid without it.

- [ ] **Step 7: Run full verification**

~~~bash
uv run python -m unittest discover -s tests -v
uv build
git diff --check
~~~

Expected: all tests pass, package build succeeds, and the diff has no whitespace errors.

- [ ] **Step 8: Commit**

~~~bash
git add src/creeper/storage/evidence_store.py src/creeper/runtime/submission.py src/creeper/runtime/pipeline.py src/creeper/cli.py tests/integration/test_sequential_runtime.py tests/integration/test_runtime_submission.py
git commit -m "feat: snapshot durable sequential runtime evidence"
~~~

## Plan self-review

- Spec coverage: Task 1 covers exact state restoration. Task 2 covers atomic ownership and restart recovery. Task 3 covers linear byte cursors. Task 4 covers direct versus hinted planning and batch storage. Task 5 covers cursor progression and no-drop backpressure. Task 6 covers crash idempotency and submission snapshots.
- Scope check: async providers, RangeProbe, Agent, source expansion, database merging, and hierarchical portfolio scheduling remain excluded.
- Type consistency: grant_fresh_lease, EvidencePlan, EvidencePlanner, resolve_year_masks, finish_evidence_tasks, RuntimeSubmissionContext, and build_runtime_snapshot are defined before later tasks consume them.

