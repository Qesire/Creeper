# Production EED Closure Design

**Status:** Approved design for implementation

**Goal:** Close the production path from an activated discovered source to independently calculated annual EED and a self-contained submission snapshot.

## Context

The current `main` branch has durable source and evidence components, but the two sides are not yet connected. Discovery can mark a candidate `ACTIVE` without creating a production `Reservoir`; the producer CLI manually constructs one static dataset; the producer runs only one lease; asynchronous evidence accepts only exact-year tasks; and runtime submission context can provide the final EED values from outside the evidence store.

The design keeps the existing single-host architecture and preserves the annual/candidate track separation.

## Design

```text
ACTIVE SourceCandidate
        ↓
SourceActivationCompiler
        ↓
ProductionSourceSpec + durable SourceDomain/Reservoir
        ↓
SourceProducer --watch
        ↓
EvidenceTask(range or exact-year)
        ↓
AsyncEvidenceWorker
        ↓
EvidenceStore
        ↓
Latest baseline diff + official EED model
        ↓
Self-contained SubmissionSnapshot
```

### 1. Remote archive dependencies

`fsspec` becomes a declared core dependency because the WARC/ARC remote reader imports it. HTTPS remote sources must work in a clean locked installation. S3 support is explicit: the runtime either loads an installed `s3fs` backend or fails with a precise dependency error; it must not silently claim S3 support when the backend is absent.

### 2. Source activation compiler

`SourceActivationCompiler` converts a measured, eligible `SourceCandidate` into a durable production specification. It is idempotent by source key and records the source lineage. The compiler selects an adapter family from the resource type and preserves:

```text
source key
source family
canonical entrypoint
temporal scope
enumeration kind
evidence mode
adapter id
root locator
cursor
capacity estimate
```

The compiler supports the first production adapter families already represented in the repository: local/static line data, WARC/ARC metadata, and structured archive/tabular records. Unsupported candidates remain discovery records and are not silently activated.

### 3. Continuous source producer

The existing `--once` behavior remains available for deterministic tests. A new `--watch` mode repeatedly claims fresh durable leases, executes bounded source work, observes admission/backpressure, and sleeps with bounded exponential idle backoff. SIGINT and SIGTERM stop new claims and allow the current bounded lease to finish. A restart reads the durable Reservoir cursor and never restarts an exhausted source.

### 4. Durable range evidence graph

The existing `EvidenceQueryKey` temporal scope already supports a closed year range. The planner will emit range tasks for contiguous missing-year spans. The provider executes a range probe and returns candidate years without granting evidence. The worker then atomically:

```text
range PASS with candidate years → finish range task + enqueue exact-year tasks
range EMPTY_EXHAUSTIVE          → finish range task, enqueue no exact tasks
range INCOMPLETE/TRANSIENT      → retain retryable range task
range INVALID                   → finish terminal invalid task
```

Exact-year tasks remain the only tasks that can commit annual evidence capsules. This preserves the evidence boundary while avoiding `hostname × 6 years` queries for range-empty hosts.

### 5. Self-contained EED authority

Runtime snapshot construction will derive novel capsules from the EvidenceStore and latest BaselineIndex, then calculate the annual EED from the configured official model. The caller may provide provenance metadata, but not authoritative `novel_eed` or `growth_rate` values. Growth is derived from the exact current baseline EED and the recomputed novel EED. Precheck remains the final structural gate.

### 6. Explicit non-goals for this batch

This batch does not implement a full Domain/Reservoir stock bandit, reserve-hours controller, optional Agent composition overhaul, or 24-hour production campaign. Those depend on the closed production path and will be measured after this design is implemented.

## Correctness invariants

1. Discovery `ACTIVE` without a supported adapter never creates production work.
2. Every production lease has a durable Reservoir cursor and a unique lease owner.
3. A range probe can only create exact-year follow-up tasks; it cannot create annual evidence directly.
4. Incomplete range results never infer absent years.
5. Exact-year evidence remains subject to hostname, target-year, status, provider, and policy checks.
6. Replaying a range or exact task is idempotent and cannot increase EED twice.
7. Submission EED equals the official calculation over the latest baseline-relative novel evidence set.
8. Annual and Candidate metrics remain separate.

## Verification strategy

- Dependency tests use a clean locked environment and exercise local plus HTTP remote WARC paths.
- Activation tests prove candidate-to-reservoir idempotency and unsupported-source fail-closed behavior.
- Producer tests prove watch idle/backoff, signal stop, cursor resume, and no double lease claim.
- Range tests prove range-empty, partial-hit, incomplete, retryable, and follow-up exact-year behavior.
- Submission tests prove caller-supplied EED values cannot override recomputed EED.
- The complete suite, build, lock validation, and diff checks run before commit.
