# Creeper V2.2 Runtime Foundation Design

## Goal

Convert Creeper from a bounded, script-driven V2.1 experiment framework into a
long-running, pull-based V2.2 runtime foundation. The first implementation
round must preserve the existing official-authority, evidence-policy, source
parser, and submission logic while making every production action a finite,
recoverable unit of work.

The system objective is downstream accepted novel EED per bottleneck cost. It
is not baseline-external hostname throughput.

## Scope and sequencing

### Phase 1: synchronous runtime foundation

Phase 1 introduces the persistent control plane and bounded execution model:

- Correct CDX pagination completion semantics and complete evidence query
  identity.
- SourceDomain, Reservoir, and WorkLease state models.
- SQLite-WAL control state for reservoirs, leases, and evidence tasks.
- Streaming baseline resolution in finite batches.
- Bounded in-process queues and downstream evidence credits.
- A single batching commit writer for evidence persistence.
- A deterministic scheduler skeleton that scores expected novel EED against
  the most constrained resource.
- A synchronous `creeper run --once` vertical slice using existing providers.

Phase 1 intentionally does not add `httpx`, asynchronous workers, RangeProbe,
new external SourceDomains, bandits, or Agent integration.

### Phase 2: asynchronous evidence providers

Phase 2 may add `httpx`, provider-specific pools, token buckets, adaptive
concurrency, provider-level pressure, and RangeProbe followed by selective
YearProbe. It must retain the Phase 1 task and acceptance contracts.

## Preserved components

The following modules remain the authority for their existing responsibilities:

- `authority/`: official normalization, annual/candidate baseline, and EED.
- `evidence/policies.py`: deterministic evidence acceptance rules.
- `sources/archive/`: Arquivo and CDXJ parsing.
- `submission/`: snapshot construction, export, and verification.

The SQLite baseline index remains in place. Its measured batch lookup rate is
above the project's 100k hosts/s engineering floor, so it is not a Phase 1
optimization target.

## Runtime architecture

```text
GlobalScheduler
  -> WorkLease
  -> ReservoirAdapter.execute(lease)
  -> bounded SourceRecord queue
  -> HostObservation queue
  -> BaselineIndex.iter_resolve_batches()
  -> direct EvidenceCapsule or persisted EvidenceTask
  -> synchronous provider worker
  -> bounded commit queue
  -> CommitWriter x1
  -> EvidenceStore
  -> novelty view and SubmissionSnapshot
```

Every queue has an explicit capacity. Discovery-only work must reserve
evidence credits before a lease is granted. Direct-year sources may continue
when external evidence credits are unavailable because they do not create
external provider tasks.

## Core data model

### SourceDomain

`SourceDomain` represents a data-generation mechanism rather than an
individual dataset. It contains `domain_id`, `family`, `discovery_mechanism`,
temporal scope, and one of `UNEXPLORED`, `EXPLORING`, `PRODUCTIVE`,
`DECLINING`, `DORMANT`, or `EXHAUSTED`.

Only existing families are registered in Phase 1, such as archive CDXJ and
local auxiliary datasets. Phase 1 creates no new external source adapters.

### Reservoir

`Reservoir` is a finite, controllable collection within a SourceDomain. It
contains `reservoir_id`, `domain_id`, `adapter_id`, `root_locator`,
enumeration kind, capacity lower/upper bounds, cursor, evidence mode, and
state. Its normal state transition is:

```text
DISCOVERED -> QUALIFYING -> READY -> LEASED -> RUNNING -> READY | EXHAUSTED
```

`PAUSED`, `PREEMPTED`, and `ABORTED` are valid runtime exits. A running
adapter cannot enlarge its own reservoir or lease.

### WorkLease

`WorkLease` replaces the production use of `SourceBudget`. It has a stable
lease id, reservoir id, start/end cursors, record/request/byte/time limits,
resource class, expected external-evidence tasks, expected novel EED, owner,
expiry, and state.

```text
CREATED -> GRANTED -> RUNNING -> SUCCEEDED | PAUSED | PREEMPTED | ABORTED | EXPIRED
```

Retries create a new lease rather than reopening a terminal lease. The old
`SourceBudget` may remain as an adapter-compatibility helper but is not a
scheduler primitive.

### Evidence query and evidence fact identity

An `EvidenceQueryKey` is immutable and consists of:

```text
(normalized hostname, normalized closed year range, provider, policy_version)
```

Exact-year queries use a one-year closed range. Only an identical complete key
may reuse a terminal checkpoint. `PASS`, `EMPTY_EXHAUSTIVE`, and `INVALID` are
terminal; `INCOMPLETE` and `TRANSIENT_ERROR` are retryable.

An evidence fact retains the full query provenance. Its deduplication identity
includes hostname, year, provider, payload hash, and policy version. A payload
accepted under a later policy must not silently erase the earlier policy's
provenance.

### Control store

A SQLite-WAL control database owns `source_domains`, `reservoirs`,
`work_leases`, `evidence_tasks`, and runtime checkpoints. `evidence_tasks` is
the resume authority, with state, attempts, retry time, lease owner, and lease
expiry. JSONL remains an append-only audit export, never the task scheduler's
source of truth.

## Execution contracts

### Streaming baseline resolution

`BaselineIndex` gains an iterator API that consumes a finite input batch and
yields resolved batches without materializing an entire Reservoir. Each batch
normalizes, de-duplicates only within its bounded unit, resolves annual mask
and candidate membership, yields results, and releases memory.

One dedicated read-only resolver stage is sufficient initially; existing
measurements do not justify an LMDB migration or a broad connection pool.

### CDX completion correctness

For a query with no valid evidence:

- a final observed page marked complete produces `EMPTY_EXHAUSTIVE`;
- a final observed page marked incomplete produces `INCOMPLETE`;
- a transport that yields no pages produces `INCOMPLETE`;
- any valid matching record produces `PASS` immediately.

Thus a normal resume-key sequence of `(page, incomplete)` followed by
`(page, complete)` is exhaustive if neither page contains evidence.

### Credits and bounded queues

The initial runtime defines capacities for `source_record_queue`,
`observation_queue`, `evidence_task_queue`, and `commit_queue`. A credit model
tracks source-fetch, parse, commit, and per-provider evidence capacity.

For a discovery-only lease, the scheduler grants it only when:

```text
total evidence capacity
- queued and claimed evidence tasks
- reservations held by other granted leases
>= this lease's expected evidence tasks
```

In drain-only mode, upstream fetching receives zero credits while parsing and
committing receive credits. Provider pressure is kept provider-specific in the
model even though Phase 1 executes providers synchronously.

### Commit writer

Workers never write `EvidenceStore` directly in production flow. They submit
capsules to one bounded queue. `CommitWriter` inserts idempotently in one
transaction per configured count or time window, then updates task state and
exports audit records. The batch size and time window are configuration values,
not fixed constants.

### Scheduler objective

The scheduler ranks leases by:

```text
expected novel EED / max(normalized general-network, evidence-network, CPU, SSD cost)
```

Accepted novel EED is the observed reward. Before evidence is available, an
estimate may use novel host count, observed evidence-pass probability, and
official EED weight, but `baseline_external/hour` cannot be a final reward.

## Adapter boundary

`SourceAdapter.enumerate()` is retained only for legacy pilot compatibility.
Production adapters implement a bounded reservoir contract:

```python
estimate() -> ReservoirEstimate
execute(lease: WorkLease) -> Iterator[SourceRecord]
```

An execution result records its next cursor, records, requests, bytes, and
elapsed time. The scheduler alone grants subsequent leases.

`SourceRecord` and `HostObservation` retain their existing fields and gain the
minimum V2.2 metadata needed for compatibility: record type, source time,
artifact reference, direct-year mask, and year-hint mask.

## Test and acceptance requirements

Phase 1 is accepted only when tests prove:

- two-page empty CDX pagination becomes `EMPTY_EXHAUSTIVE`;
- incomplete or no-page CDX results remain retryable;
- query identity differs by temporal scope, provider, and policy version;
- a terminal result skips only the same full query key;
- policy provenance survives evidence fact persistence;
- batch evidence writes are idempotent and transactional;
- a large generator is consumed in bounded batches rather than materialized;
- leases obey record, request, byte, and time limits, and recover after expiry;
- no discovery-only lease is granted beyond evidence credits;
- scheduler ranking prefers expected novel EED per bottleneck cost;
- the synchronous `run --once` path completes an offline source-to-submission
  vertical slice;
- all existing unit, integration, golden, submission, and build checks remain
  green.

## Non-goals

Phase 1 does not claim annual production throughput, introduce new sources,
or treat Candidate output as annual leaderboard score. Candidate and annual
metrics remain distinct until the competition's score-combination rule is
verified.
