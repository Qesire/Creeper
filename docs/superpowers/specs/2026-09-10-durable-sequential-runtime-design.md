# Durable Sequential Runtime Design

## Goal

Make a finite reservoir advance safely across many sequential leases and
process restarts. The runtime must neither repeat an already-completed range
nor silently discard work when a downstream queue reaches its configured
capacity. This work deliberately excludes async providers, RangeProbe, source
discovery expansion, and hierarchical portfolio scheduling.

## Scope and non-goals

This is V2.2 Phase 1B. It upgrades the existing synchronous `run --once`
vertical slice into a durable sequential runner.

Included:

- durable reservoir cursor and lifecycle progression;
- atomic, fresh lease grants;
- byte-offset cursors for the local static-file adapter;
- lossless bounded draining;
- explicit evidence planning with direct and external paths;
- batched local state operations;
- a submission-ready sequential vertical slice and restart invariants.

Excluded:

- `httpx`, asyncio, provider pools, rate limiting, or RangeProbe;
- additional source adapters, HistoricalSearchBroker, Agent, or SourceDomain
  portfolio statistics;
- combining `control.sqlite3` and `evidence.sqlite3` into one database.

## Authority and state model

`ControlStore` is the durable authority for reservoir ownership and progress.
`GlobalScheduler` remains pure selection logic: it ranks candidates by the
existing expected-novel-EED / bottleneck-cost score, but does not establish
ownership by mutating an in-memory state map.

### Atomic lease grant

`ControlStore.grant_lease(...)` executes one `BEGIN IMMEDIATE` transaction:

1. Read the target reservoir.
2. Require its state to be `READY`.
3. Construct a fresh `WorkLease` from its persisted cursor and the caller's
   bounded lease limits.
4. Insert the lease as `GRANTED`.
5. Change the reservoir to `LEASED`.
6. Commit and return the lease.

An already leased, running, paused, exhausted, or aborted reservoir cannot be
granted again. A scheduler must not reuse a pre-created `WorkLease` object.

### Execution and cursor completion

The runtime persists `RUNNING` before invoking the adapter. It consumes the
returned `LeaseResult` as follows:

| Result | Lease state | Reservoir state | Cursor |
| --- | --- | --- | --- |
| `next_cursor` is non-null | `SUCCEEDED` | `READY` | persist `next_cursor` |
| `next_cursor` is null | `SUCCEEDED` | `EXHAUSTED` | retain final cursor |
| runtime failure | `ABORTED` | `READY` | retain starting cursor |

The design treats a successful partial lease as progress, not exhaustion. A
subsequent lease always starts from the reservoir cursor in `ControlStore`.

### Deserialization is not a transition

`get_domain()` and `get_reservoir()` construct the respective immutable model
with the exact enum stored in SQLite. They never replay the business
transition graph. Transition methods remain reserved for new runtime actions.

## Cursor contract

`WorkLease.cursor_start`, `cursor_end`, and `LeaseResult.next_cursor` remain
opaque strings at the general adapter interface.

`StaticDatasetAdapter` defines its cursor as a decimal byte offset:

- `None` means offset zero;
- it opens the file in binary mode and calls `seek(offset)`;
- it emits complete newline-delimited records without exceeding the lease's
  record, byte, request, or elapsed-time limits;
- its returned non-null cursor is the `tell()` offset before the first
  unconsumed record;
- it returns null only when EOF is reached.

This makes repeated leases over a local file linear in file size rather than
re-scanning the skipped prefix for every lease.

## Lossless bounded execution

Queues are hard limits, not discard signals. A full evidence queue causes the
runtime to stop pulling upstream records, drain and commit the queued evidence
work, then resume the same lease. It never skips an observation merely because
a queue is full.

The synchronous phase can use short producer/drain cycles. This preserves the
existing `BoundedQueues` interface and establishes semantics that later map
directly to asynchronous producer/consumer queues.

## Evidence planning

`EvidencePlanner` becomes the single pure component that evaluates an
observation against:

- official annual mask;
- local evidence mask;
- `direct_year_mask`;
- `year_hint_mask` and a legacy `source_year` hint.

It returns a plan containing two disjoint sets:

- direct `EvidenceCapsule` values, produced only from observations whose
  source supplies explicit direct-year provenance; and
- external `EvidenceQueryKey` values for missing hinted years.

Direct capsules use source provenance and a stable payload hash; they do not
enter a provider queue, reserve provider credits, or invoke CDX. A year hint
does not itself constitute direct evidence.

## Batched local state operations

The hot path operates in bounded batches:

- `EvidenceStore.resolve_year_masks(hostnames)` resolves local evidence masks
  in a single batched query per hostname batch;
- `ControlStore.enqueue_evidence_tasks(keys)` receives the whole planned
  batch;
- `ControlStore.finish_evidence_tasks(results, owner=...)` updates all claimed
  terminal/retryable tasks in one SQLite transaction;
- `CommitWriter.flush()` writes capsules and completes their task results in
  batches.

The two databases remain separate in this phase. Crash recovery relies on
evidence-store idempotency and durable task ownership. A crash after capsule
write but before task completion may re-query evidence; it must not duplicate
an evidence fact or lose a task. A later storage-design phase may consolidate
them or introduce a durable outbox.

## Submission integration

After a successful sequential lease, the runtime can build a
`SubmissionSnapshot` from the latest baseline and committed evidence. The
offline `run --once` command reports the snapshot status, but does not create
or submit an external competition package automatically.

## Acceptance tests

The implementation is complete only when offline tests demonstrate:

1. non-initial Domain and Reservoir states round-trip through ControlStore;
2. one reservoir cannot receive two concurrent grants;
3. two consecutive leases advance without repeated records;
4. a restarted runtime resumes from persisted cursor;
5. static byte cursors do not rescan a prior prefix;
6. queue saturation drains and resumes without dropping planned work;
7. direct evidence commits without CDX/provider credits;
8. hints produce external evidence tasks only for missing years;
9. task enqueue, local evidence masks, and task completion use bounded batch
   operations;
10. a crash boundary preserves an idempotent evidence fact and retryable task;
11. the source-to-submission sequential path produces a valid offline snapshot.

