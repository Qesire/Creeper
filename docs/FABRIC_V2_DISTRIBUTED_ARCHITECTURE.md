# Creeper Fabric v2 distributed architecture

Status: implementation contract for the production distributed runtime.

## 1. Authority model

Fabric uses a database-first control plane.

- PostgreSQL is the production authority backend.
- SQLite/WAL is a single-authority development and disaster-recovery backend.
- Transports (HTTP polling today; NATS/JetStream may be added later) are hints,
  never the source of truth.
- Work admission is idempotent by `WorkKey`.
- Execution is at-least-once.
- Side effects become exactly-once at the authority/domain boundary through
  generation fencing, ordered ResultBatch sequence numbers and domain
  idempotency identities.

A production deployment may run multiple stateless Authority API instances.
PostgreSQL claims use `FOR UPDATE SKIP LOCKED`; no elected dispatcher is
required.

## 2. Worker ownership

Each process has:

- stable `worker_id`;
- unique `worker_instance_id` per process incarnation;
- declared capabilities;
- declared producer implementations;
- provider allowlist and optional daily egress budget.

Every lease contains a monotonically increasing generation. The tuple

`task_id + worker_id + worker_instance_id + generation`

is the fencing identity for all worker writes and provider permits. A new
worker incarnation releases leases held by the old incarnation.

Workers expose no Creeper listening port and never communicate peer-to-peer.

## 3. Work and results

Fabric Core does not know source-research semantics. It knows only
`WorkDefinition`, `TaskLease`, `ResultBatch` and `ArtifactRef`.

Production task classes are deterministic/evidence work only:

- `RESIDUAL_QUERY`
- `SOURCE_SHARD`
- `HOST_BATCH`
- `EVIDENCE_BATCH`
- `REGION_PROBE`

There is deliberately no WEB_DISCOVERY, SEARCH_QUERY, SOURCE_RESEARCH or LLM
task class.

Large objects live in object storage/local shared artifact storage. The
authority database stores only a content-addressed ArtifactRef manifest.

## 4. Delivery

Worker output follows a durable outbox/inbox pattern.

1. Producer creates a ResultBatch.
2. Worker commits the batch to its local SQLite spool before network delivery.
3. Authority verifies active lease generation and expected sequence.
4. Authority stores the batch and its control-plane outbox event in one DB
   transaction.
5. Worker deletes the local spool entry only after Authority ACK.
6. Domain bridge consumes authority inbox batches idempotently and marks them
   consumed only after domain commit succeeds.

Lost ACKs therefore replay the same batch identity instead of repeating
external provider work.

## 5. Provider coordination

Every formal provider HTTP attempt requires an Authority permit.

The DB owns:

- global RPS schedule;
- max global inflight;
- cooldown after 429/503;
- Provider x Region qualification;
- worker provider allowlist;
- worker daily raw-response egress budget.

The permit is held until the HTTP response stream is consumed or closed.

## 6. Transport

HTTP long-polling is the baseline protocol because it requires no broker.
A future NATS/JetStream adapter may publish outbox events and wake workers, but
workers must still claim/fence work in PostgreSQL and commit ResultBatch through
Authority. Broker delivery semantics cannot override database ownership.

## 7. Creeper domain boundary

Fabric workers execute deterministic expensive I/O. They do not decide:

- baseline novelty;
- direct annual-evidence authority;
- source admission;
- adapter authority;
- submission acceptance.

Those decisions remain central and reuse the current Creeper registries and
evidence contracts.

Automatic LLM work remains restricted to local
`UNKNOWN_FORMAT -> COMPILE_ADAPTER`; no distributed LLM source-search lane
exists.

## 8. Deployment topology

Recommended production topology:

```
                         +-------------------------+
                         | PostgreSQL Authority DB |
                         +------------+------------+
                                      |
                     +----------------+----------------+
                     |                                 |
             Authority API A                   Authority API B
             (stateless)                       (stateless)
                     |                                 |
       +-------------+-------------+       +-----------+-----------+
       |                           |       |                       |
  Worker SG                    Worker US  Worker EU             Worker local
  outbound only                outbound   outbound              outbound
```

Object storage is separate from the control database. Authority API should be
behind TLS; HMAC worker authentication is supported now and may be replaced by
mTLS without changing task ownership semantics.
