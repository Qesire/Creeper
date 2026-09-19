"""Database schemas for Fabric v2.

SQLite is a local/development compatibility backend. PostgreSQL is the
production distributed authority.
"""

FABRIC_SCHEMA_VERSION = "fabric-v2-schema-1"


SQLITE_DDL = r"""
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS fabric_workers_v2 (
    worker_id TEXT PRIMARY KEY,
    descriptor_json TEXT NOT NULL,
    protocol_version TEXT NOT NULL,
    edition TEXT NOT NULL,
    last_heartbeat REAL NOT NULL,
    revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0, 1))
);

CREATE TABLE IF NOT EXISTS fabric_tasks_v2 (
    task_id TEXT PRIMARY KEY,
    work_key TEXT NOT NULL UNIQUE,
    work_class TEXT NOT NULL,
    producer TEXT NOT NULL,
    algorithm_version TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    input_identity TEXT NOT NULL,
    coverage_json TEXT NOT NULL,
    required_capabilities_json TEXT NOT NULL,
    priority REAL NOT NULL,
    queue_name TEXT NOT NULL,
    max_attempts INTEGER NOT NULL CHECK(max_attempts >= 1),
    provider TEXT,
    min_memory_bytes INTEGER NOT NULL DEFAULT 0 CHECK(min_memory_bytes >= 0),
    network_class TEXT,
    state TEXT NOT NULL,
    available_at REAL NOT NULL,
    lease_owner TEXT,
    lease_epoch INTEGER NOT NULL DEFAULT 0 CHECK(lease_epoch >= 0),
    lease_deadline REAL,
    attempt INTEGER NOT NULL DEFAULT 0 CHECK(attempt >= 0),
    cursor_json TEXT,
    next_sequence_no INTEGER NOT NULL DEFAULT 0 CHECK(next_sequence_no >= 0),
    last_error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fabric_tasks_claim_v2
    ON fabric_tasks_v2(state, queue_name, priority DESC, available_at, created_at, task_id);
CREATE INDEX IF NOT EXISTS idx_fabric_tasks_lease_v2
    ON fabric_tasks_v2(state, lease_deadline);

CREATE TABLE IF NOT EXISTS fabric_result_batches_v2 (
    task_id TEXT NOT NULL,
    sequence_no INTEGER NOT NULL CHECK(sequence_no >= 0),
    lease_epoch INTEGER NOT NULL CHECK(lease_epoch >= 1),
    payload_json TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    cursor_after_json TEXT,
    committed_at REAL NOT NULL,
    PRIMARY KEY(task_id, sequence_no),
    FOREIGN KEY(task_id) REFERENCES fabric_tasks_v2(task_id)
);

CREATE TABLE IF NOT EXISTS fabric_outbox_v2 (
    event_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_sequence INTEGER NOT NULL CHECK(aggregate_sequence >= 0),
    payload_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    published_at REAL
);
CREATE INDEX IF NOT EXISTS idx_fabric_outbox_unpublished_v2
    ON fabric_outbox_v2(published_at, created_at, event_id);

CREATE TABLE IF NOT EXISTS fabric_inbox_v2 (
    consumer_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    consumed_at REAL NOT NULL,
    PRIMARY KEY(consumer_id, event_id)
);

CREATE TABLE IF NOT EXISTS fabric_provider_budgets_v2 (
    provider TEXT PRIMARY KEY,
    requests_per_second REAL NOT NULL CHECK(requests_per_second > 0),
    max_global_inflight INTEGER NOT NULL CHECK(max_global_inflight >= 1),
    require_qualified_region INTEGER NOT NULL DEFAULT 1 CHECK(require_qualified_region IN (0,1)),
    next_request_at REAL NOT NULL DEFAULT 0,
    cooldown_until REAL NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS fabric_provider_regions_v2 (
    provider TEXT NOT NULL,
    region TEXT NOT NULL,
    qualified INTEGER NOT NULL DEFAULT 0 CHECK(qualified IN (0,1)),
    samples INTEGER NOT NULL DEFAULT 0 CHECK(samples >= 0),
    successes INTEGER NOT NULL DEFAULT 0 CHECK(successes >= 0),
    throttles INTEGER NOT NULL DEFAULT 0 CHECK(throttles >= 0),
    failures INTEGER NOT NULL DEFAULT 0 CHECK(failures >= 0),
    updated_at REAL NOT NULL,
    PRIMARY KEY(provider, region)
);

CREATE TABLE IF NOT EXISTS fabric_provider_regions_v2 (
    provider TEXT NOT NULL REFERENCES fabric_provider_budgets_v2(provider),
    region TEXT NOT NULL,
    qualified BOOLEAN NOT NULL DEFAULT FALSE,
    samples INTEGER NOT NULL DEFAULT 0 CHECK(samples >= 0),
    successes INTEGER NOT NULL DEFAULT 0 CHECK(successes >= 0),
    throttles INTEGER NOT NULL DEFAULT 0 CHECK(throttles >= 0),
    failures INTEGER NOT NULL DEFAULT 0 CHECK(failures >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(provider, region)
);

CREATE TABLE IF NOT EXISTS fabric_provider_permits_v2 (
    permit_id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    lease_epoch INTEGER NOT NULL CHECK(lease_epoch >= 1),
    allowed_requests INTEGER NOT NULL CHECK(allowed_requests >= 1),
    expires_at REAL NOT NULL,
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    issued_at REAL NOT NULL,
    status_code INTEGER,
    response_bytes INTEGER NOT NULL DEFAULT 0 CHECK(response_bytes >= 0),
    FOREIGN KEY(provider) REFERENCES fabric_provider_budgets_v2(provider),
    FOREIGN KEY(worker_id) REFERENCES fabric_workers_v2(worker_id),
    FOREIGN KEY(task_id) REFERENCES fabric_tasks_v2(task_id)
);
CREATE INDEX IF NOT EXISTS idx_fabric_provider_permits_active_v2
    ON fabric_provider_permits_v2(provider, active, expires_at);
"""


POSTGRES_DDL = r"""
CREATE TABLE IF NOT EXISTS fabric_workers_v2 (
    worker_id TEXT PRIMARY KEY,
    descriptor JSONB NOT NULL,
    protocol_version TEXT NOT NULL,
    edition TEXT NOT NULL,
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    revoked BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS fabric_tasks_v2 (
    task_id UUID PRIMARY KEY,
    work_key TEXT NOT NULL UNIQUE,
    work_class TEXT NOT NULL,
    producer TEXT NOT NULL,
    algorithm_version TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    input_identity TEXT NOT NULL,
    coverage JSONB NOT NULL,
    required_capabilities TEXT[] NOT NULL,
    priority DOUBLE PRECISION NOT NULL DEFAULT 0,
    queue_name TEXT NOT NULL DEFAULT 'default',
    max_attempts INTEGER NOT NULL CHECK(max_attempts >= 1),
    provider TEXT,
    min_memory_bytes BIGINT NOT NULL DEFAULT 0 CHECK(min_memory_bytes >= 0),
    network_class TEXT,
    state TEXT NOT NULL,
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    lease_owner TEXT,
    lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK(lease_epoch >= 0),
    lease_deadline TIMESTAMPTZ,
    attempt INTEGER NOT NULL DEFAULT 0 CHECK(attempt >= 0),
    cursor JSONB,
    next_sequence_no BIGINT NOT NULL DEFAULT 0 CHECK(next_sequence_no >= 0),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS idx_fabric_tasks_claim_v2
    ON fabric_tasks_v2(
        state,
        queue_name,
        priority DESC,
        available_at,
        created_at,
        task_id
    );
CREATE INDEX IF NOT EXISTS idx_fabric_tasks_lease_v2
    ON fabric_tasks_v2(state, lease_deadline);

CREATE TABLE IF NOT EXISTS fabric_result_batches_v2 (
    task_id UUID NOT NULL REFERENCES fabric_tasks_v2(task_id),
    sequence_no BIGINT NOT NULL CHECK(sequence_no >= 0),
    lease_epoch BIGINT NOT NULL CHECK(lease_epoch >= 1),
    payload JSONB NOT NULL,
    payload_digest TEXT NOT NULL,
    cursor_after JSONB,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(task_id, sequence_no)
);

CREATE TABLE IF NOT EXISTS fabric_outbox_v2 (
    event_id UUID PRIMARY KEY,
    topic TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_sequence BIGINT NOT NULL CHECK(aggregate_sequence >= 0),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_fabric_outbox_unpublished_v2
    ON fabric_outbox_v2(published_at, created_at, event_id)
    WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS fabric_inbox_v2 (
    consumer_id TEXT NOT NULL,
    event_id UUID NOT NULL,
    consumed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(consumer_id, event_id)
);

CREATE TABLE IF NOT EXISTS fabric_provider_budgets_v2 (
    provider TEXT PRIMARY KEY,
    requests_per_second DOUBLE PRECISION NOT NULL CHECK(requests_per_second > 0),
    max_global_inflight INTEGER NOT NULL CHECK(max_global_inflight >= 1),
    require_qualified_region BOOLEAN NOT NULL DEFAULT TRUE,
    next_request_at TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
    cooldown_until TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS fabric_provider_permits_v2 (
    permit_id UUID PRIMARY KEY,
    provider TEXT NOT NULL REFERENCES fabric_provider_budgets_v2(provider),
    worker_id TEXT NOT NULL REFERENCES fabric_workers_v2(worker_id),
    task_id UUID NOT NULL REFERENCES fabric_tasks_v2(task_id),
    lease_epoch BIGINT NOT NULL CHECK(lease_epoch >= 1),
    allowed_requests INTEGER NOT NULL CHECK(allowed_requests >= 1),
    expires_at TIMESTAMPTZ NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    issued_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    status_code INTEGER,
    response_bytes BIGINT NOT NULL DEFAULT 0 CHECK(response_bytes >= 0)
);
CREATE INDEX IF NOT EXISTS idx_fabric_provider_permits_active_v2
    ON fabric_provider_permits_v2(provider, active, expires_at)
    WHERE active = TRUE;
"""


POSTGRES_CLAIM_SQL = r"""
WITH candidate AS (
    SELECT task_id
    FROM fabric_tasks_v2
    WHERE state = 'READY'
      AND queue_name = %(queue)s
      AND available_at <= clock_timestamp()
      AND attempt < max_attempts
      AND required_capabilities <@ %(capabilities)s::text[]
      AND min_memory_bytes <= %(memory_bytes)s
      AND (network_class IS NULL OR network_class = %(network_class)s)
      AND (
            provider IS NULL
            OR provider = ANY(%(allowed_providers)s::text[])
          )
    ORDER BY priority DESC, available_at, created_at, task_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE fabric_tasks_v2 AS task
SET state = 'LEASED',
    lease_owner = %(worker_id)s,
    lease_epoch = task.lease_epoch + 1,
    lease_deadline = clock_timestamp() + (%(lease_seconds)s * interval '1 second'),
    attempt = task.attempt + 1,
    updated_at = clock_timestamp()
FROM candidate
WHERE task.task_id = candidate.task_id
RETURNING task.*;
"""
