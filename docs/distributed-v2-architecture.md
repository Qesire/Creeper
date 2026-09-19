# Creeper Distributed v2 — production control-plane design

## Status

This document defines the production architecture for Creeper after the
single-process/source-local execution model. It is intentionally stricter than
the retired `distributed-vNext` experiment: there is one authority model and
one work lifecycle for local and remote execution.

The design preserves all current Creeper correctness boundaries:

- baseline authority remains immutable and local to the authority plane;
- direct-year evidence is still granted only by existing evidence contracts;
- deterministic residual search remains deterministic;
- LLM work remains adapter-only;
- workers never decide admission, evidence authority, novelty, or submission;
- duplicate delivery and worker death must be safe.

## 1. Architecture

```text
                      +-------------------------+
                      |  API / Control Plane    |
                      |  stateless replicas     |
                      +-----------+-------------+
                                  |
                                  v
                      +-------------------------+
                      | PostgreSQL Authority    |
                      | serializable state:     |
                      | tasks / leases / fence  |
                      | result inbox / outbox   |
                      | provider budgets        |
                      | worker capabilities     |
                      +-----------+-------------+
                                  |
               transactional outbox (optional notifications)
                                  |
                      +-----------v-------------+
                      | NATS JetStream          |
                      | wake-up / event fanout  |
                      | NOT source of truth     |
                      +----+----------+----------+
                           |          |
                +----------+--+    +--+-----------+
                | worker A    |    | worker B      |
                | Singapore   |    | US / EU       |
                | source/bulk |    | search/evid.  |
                +-------------+    +---------------+
```

Workers use outbound connections only. They never talk to one another.
Authority API replicas are stateless; PostgreSQL is the durable source of
truth. NATS is optional and only reduces polling latency. If NATS is completely
unavailable, workers fall back to bounded polling and correctness is unchanged.

## 2. Why PostgreSQL is the authority

The production queue is not implemented in Redis/NATS/Kafka. Work admission,
claiming, lease generation, result idempotency, evidence admission, and outbox
publication must share one transaction boundary.

Claims use a deterministic ordering and:

```sql
SELECT ...
FROM fabric_tasks
WHERE ...
ORDER BY priority DESC, available_at, created_at, task_id
FOR UPDATE SKIP LOCKED
LIMIT 1;
```

The claim transaction increments a monotonically increasing `lease_epoch`.
Every worker mutation must present the current `task_id + worker_id +
lease_epoch`; stale workers are fenced even if they resume after a network
partition.

## 3. Delivery semantics

The fabric is **at-least-once execution + exactly-once durable effect**.

Exactly-once execution is neither assumed nor required.

Each logical unit has a deterministic `work_key`:

```text
sha256(
  work_class
  + producer
  + algorithm_version
  + partition_key
  + canonical input identity
  + canonical coverage
)
```

`UNIQUE(work_key)` makes work admission idempotent.

Worker result batches have:

```text
(task_id, lease_epoch, sequence_no, payload_digest)
```

A replay with the same digest succeeds idempotently. A replay of the same
sequence with a different digest fails closed.

## 4. Task state machine

```text
READY -> LEASED -> READY       expired lease / retryable failure
  |        |
  |        +------> FAILED     retry budget exhausted / permanent failure
  |
  +---------------> SUCCEEDED
  |
  +---------------> CANCELLED
```

Important invariants:

1. only `READY` tasks may be claimed;
2. claim increments `lease_epoch`;
3. renew/commit/complete require current owner + epoch + unexpired lease;
4. expiry never deletes progress already committed as durable result batches;
5. cursor/checkpoint advancement and result-batch insertion are one transaction;
6. a stale epoch can never advance a cursor or complete a task.

## 5. Work classes and capabilities

The distributed fabric uses semantic work classes, not process-specific jobs.

| Work class | Required capability | Current Creeper mapping |
| --- | --- | --- |
| `RESIDUAL_SEARCH` | `SEARCH_STRUCTURED` | QueryPlan + deterministic providers |
| `SOURCE_TRIAGE` | `HTTP_FETCH` | HttpSourceTriageExecutor |
| `SOURCE_SCOUT` | `SOURCE_SCOUT` | measured/structural scout |
| `ADAPTER_COMPILE` | `ADAPTER_LLM` | unknown-format only |
| `RESERVOIR_PRODUCE` | `STREAM_BULK` | production adapters |
| `EVIDENCE_COMPLETE` | `EVIDENCE_QUERY` | archive/provider evidence |
| `REDUCE_COMMIT` | `AUTHORITY_REDUCE` | authority-local reducer only |

`REDUCE_COMMIT` is never leased to an untrusted remote worker. Remote workers
produce observations/proposals/evidence capsules; the authority reducer applies
current baseline, evidence-contract, novelty, and submission rules.

## 6. Worker registration and routing

A worker registers:

- stable worker id;
- protocol version;
- software edition / git revision;
- region and network class;
- CPU/memory concurrency;
- explicit capabilities;
- provider allowlist;
- per-provider egress ceilings;
- optional labels.

Claims match:

- required capabilities subset;
- provider/region qualification;
- queue / partition affinity;
- optional memory/network floor;
- protocol compatibility.

Workers do not receive tasks they cannot execute.

## 7. Provider budget authority

Provider rate limits are global, not per worker.

A worker must acquire a provider permit before a formal provider request. The
authority atomically checks:

- provider token bucket / next request time;
- global inflight;
- worker provider allowlist;
- provider x region qualification;
- daily worker egress budget;
- current task lease epoch.

Permits are short-lived and fenced to one task epoch. Completion reports status,
latency, response bytes and throttling. Provider health feeds routing, but never
changes evidence semantics.

## 8. Transactional outbox / inbox

Every state transition that should wake another component writes an outbox row
in the same PostgreSQL transaction. A relay publishes unpublished rows to NATS
(or another broker) and marks them published.

Consumers maintain a durable inbox keyed by `consumer_id + event_id`.
Therefore duplicate broker delivery is harmless.

Topics are hints such as:

- `fabric.task.ready.<capability>`
- `fabric.task.completed`
- `fabric.provider.cooldown`
- `fabric.worker.changed`

No consumer is allowed to treat an event as proof that database state changed;
it always re-reads authority state.

## 9. Partitioning

Do not distribute by random worker hashing.

Use stable semantic partitions:

- residual search: `provider + SearchCell.key`;
- source processing: canonical source key;
- bulk reservoir: reservoir id + range/cursor partition;
- evidence completion: provider + hostname prefix bucket;
- reducers: authority-local shard.

A partition can have many tasks, but one logical `work_key` is unique globally.

## 10. Failure model

### Worker process dies

Lease expires. Another worker claims the task with a larger epoch. Old worker
results are rejected by fencing.

### Network partition after result commit

Worker retries the same result batch. Payload digest matches, so authority
returns the prior acknowledgment.

### Network partition before result commit

No durable effect occurred. Retry is safe.

### Authority API replica dies

Clients retry another replica. PostgreSQL transaction decides the result.

### PostgreSQL primary failover

Use managed PostgreSQL or Patroni-class HA. API retries serialization /
connection failures. No in-memory task ownership exists.

### NATS loss

Workers poll. Outbox rows remain durable and can be replayed.

### Duplicate event

Inbox idempotency suppresses repeated consumer effects.

## 11. Security

Production workers never receive baseline files or authority database access.

Transport requirements:

- TLS;
- per-worker credentials;
- request timestamp + nonce or mTLS identity;
- short clock-skew window;
- replay nonce persistence;
- worker revocation at authority;
- least-privilege provider allowlists.

Cloud credentials are never stored in task payloads.

## 12. Storage separation

PostgreSQL stores control/evidence metadata, not large source payloads.

Large immutable artifacts use object storage by content digest:

```text
sha256/<first2>/<full-digest>
```

Authority stores only:

- digest;
- byte length;
- content type;
- provenance;
- producer task id;
- retention class.

Temporary samples used for adapter compilation remain bounded and follow the
existing minimal-storage policy.

## 13. Migration from current single-node runtime

### Phase A — shadow fabric

Current services submit deterministic work to fabric while local executors
remain available. Results are compared but only current authority commits
contest output.

### Phase B — worker cutover

Triage, scout, residual provider search, reservoir production and evidence
queries are leased through fabric. Local execution uses a `local-worker`
registered through the same API.

### Phase C — authority reducer cutover

All worker outputs enter the authority reducer/inbox. Existing direct writes
from worker services are disabled.

### Phase D — optional broker

Enable NATS JetStream wake-ups after PostgreSQL-only operation is stable.
Broker failure must never block correctness.

## 14. What is deliberately rejected

- distributed SQLite;
- Redis/NATS/Kafka as the authority of task ownership;
- worker-to-worker coordination;
- random best-effort task submission without deterministic work keys;
- lease renewal without fencing epochs;
- exactly-once execution claims;
- two-phase commit between database and broker;
- remote workers granting annual evidence authority;
- ordinary LLM source search.

## 15. Compatibility

Development and one-machine operation use the same protocol through the local
SQLite backend. SQLite remains a **development/local compatibility backend**,
not a multi-host production authority.

The production backend is PostgreSQL.
