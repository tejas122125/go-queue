-- Consolidated schema for Queue Forge

CREATE TABLE IF NOT EXISTS queues (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL UNIQUE,
    state VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    version INTEGER NOT NULL DEFAULT 0,
    state_reason VARCHAR(500),
    paused_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS queues_state_idx ON queues (state);

CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id UUID REFERENCES queues(id),
    name VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(50) NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    version INTEGER NOT NULL DEFAULT 0,
    run_at TIMESTAMP,
    cancellation_requested_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS jobs_queue_status_created_idx ON jobs (queue_id, status, created_at);
CREATE INDEX IF NOT EXISTS jobs_status_run_at_idx ON jobs (status, run_at);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key VARCHAR(255) NOT NULL UNIQUE,
    job_id UUID NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(100) NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL DEFAULT 'job',
    aggregate_id UUID,
    deduplication_key VARCHAR(255) NOT NULL UNIQUE,
    payload JSONB NOT NULL,
    processed_at TIMESTAMP,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMP NOT NULL DEFAULT now(),
    last_error VARCHAR(1000),
    claimed_by VARCHAR(255),
    claim_expires_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox_events (processed_at, next_attempt_at, created_at);
CREATE INDEX IF NOT EXISTS outbox_claim_idx ON outbox_events (claimed_by, claim_expires_at);

CREATE TABLE IF NOT EXISTS job_leases (
    job_id UUID PRIMARY KEY REFERENCES jobs(id),
    worker_id UUID NOT NULL,
    lease_token UUID NOT NULL DEFAULT gen_random_uuid(),
    fencing_token INTEGER NOT NULL,
    acquired_at TIMESTAMP NOT NULL DEFAULT now(),
    expires_at TIMESTAMP NOT NULL,
    heartbeat_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS job_leases_worker_id_idx ON job_leases (worker_id);
CREATE INDEX IF NOT EXISTS job_leases_expires_at_idx ON job_leases (expires_at);

CREATE TABLE IF NOT EXISTS workers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hostname VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    last_heartbeat TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dead_letter_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL,
    reason VARCHAR(1000),
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS job_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    status VARCHAR(50),
    error_message VARCHAR(1000)
);

CREATE TABLE IF NOT EXISTS job_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL REFERENCES jobs(id),
    event_type VARCHAR(100) NOT NULL,
    from_status VARCHAR(50),
    to_status VARCHAR(50),
    version INTEGER NOT NULL,
    actor_type VARCHAR(50) NOT NULL,
    actor_id VARCHAR(255),
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS job_events_job_created_idx ON job_events (job_id, created_at);

CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_id VARCHAR(255) NOT NULL,
    action VARCHAR(100) NOT NULL,
    target_type VARCHAR(100) NOT NULL,
    target_id VARCHAR(255) NOT NULL,
    reason VARCHAR(500) NOT NULL,
    outcome VARCHAR(50) NOT NULL,
    request_id VARCHAR(255),
    change_summary JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS audit_logs_target_created_idx ON audit_logs (target_type, target_id, created_at);

CREATE TABLE IF NOT EXISTS workflow_definitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description VARCHAR(1000),
    version INTEGER NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'DRAFT',
    input_schema JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    published_at TIMESTAMP,
    CONSTRAINT workflow_definitions_name_version_unique UNIQUE(name, version)
);

CREATE TABLE IF NOT EXISTS workflow_task_definitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_definition_id UUID NOT NULL REFERENCES workflow_definitions(id),
    task_key VARCHAR(100) NOT NULL,
    job_name VARCHAR(255) NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    input JSONB NOT NULL DEFAULT '{}',
    CONSTRAINT workflow_tasks_definition_key_unique UNIQUE(workflow_definition_id, task_key)
);

CREATE INDEX IF NOT EXISTS workflow_tasks_definition_idx ON workflow_task_definitions (workflow_definition_id);

CREATE TABLE IF NOT EXISTS workflow_dependencies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_definition_id UUID NOT NULL REFERENCES workflow_definitions(id),
    upstream_task_key VARCHAR(100) NOT NULL,
    downstream_task_key VARCHAR(100) NOT NULL,
    CONSTRAINT workflow_dependencies_edge_unique UNIQUE(workflow_definition_id, upstream_task_key, downstream_task_key)
);

CREATE INDEX IF NOT EXISTS workflow_dependencies_downstream_idx ON workflow_dependencies (workflow_definition_id, downstream_task_key);

