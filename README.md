# Queue Forge

A **distributed, durable, at-least-once job queue platform** built with **Go**, PostgreSQL (source of truth), Redis (execution transport), a transactional outbox pattern, and a React/Vite operator dashboard.

---

## Table of Contents

1. [What this project is](#1-what-this-project-is)
2. [High-level architecture](#2-high-level-architecture)
3. [End-to-end workflow](#3-end-to-end-workflow-the-actual-request-lifecycle)
   - [3.1 Creating a job](#31-creating-a-job)
   - [3.2 Outbox → Redis](#32-outbox--redis-cmdpublisher)
   - [3.3 Execution](#33-execution-cmdworker)
   - [3.4 Delayed jobs](#34-delayed-jobs-cmdscheduler)
   - [3.5 Worker lifecycle](#35-worker-lifecycle-registration-heartbeat-shutdown)
   - [3.6 Operator actions](#36-operator-actions-via-cmdapi-operations-used-by-the-dashboard)
4. [Database schema](#4-database-schema-postgres-via-sqlc--internaldb)
5. [Every directory and package](#5-every-directory-and-package-explained)
6. [Every API endpoint](#6-every-api-endpoint)
7. [Services & packages summaries](#7-services--packages-one-paragraph-summaries)
8. [Design Notes & Extension Points](#8-design-notes--extension-points)

---

## 1. What this project is

Queue Forge is a **distributed, durable, at-least-once job queue platform**:

- **PostgreSQL** = source of truth for every durable fact (jobs, leases, outbox, audit, DLQ).
- **Redis** = disposable, fast execution transport (ready lists + a delayed sorted-set). It can be wiped and rebuilt from Postgres.
- A **transactional outbox** decouples "job was created" (Postgres) from "job was dispatched" (Redis), so the API never touches Redis directly.

Four Go microservices + internal Go packages + a React dashboard make up the system:

```
cmd/
  api          → Go REST API (job creation + authenticated operator console API)
  worker       → consumes Redis queues, executes jobs, manages distributed leases & fencing
  publisher    → transactional outbox → Redis bridge with SKIP LOCKED polling
  scheduler    → promotes due delayed jobs from Redis sorted set → ready queues via atomic Lua

internal/
  db           → PostgreSQL client, migrations, sqlc type-safe query layer
  redisqueue   → Redis client, priority queues (HIGH/MEDIUM/LOW), delayed ZSET, Lua scripts
  logger       → Zerolog structured JSON logging
  metrics      → Prometheus registry, counters, gauges, histograms

apps/
  dashboard    → React/Vite operator UI, talks to api's /api/v1/operations routes

tests/integration → comprehensive in-memory pipeline tests covering all lifecycle scenarios
infrastructure   → docker-compose, Prometheus config, Grafana provisioning
```

### Quickstart & Commands

```bash
# Build all Go binaries
make build

# Run all unit and integration tests
make test

# Regenerate sqlc queries
make generate

# Run React dashboard (development)
make dashboard-dev
```

---

## 2. High-level architecture

```
                         ┌─────────────────────┐
                         │   Client / Dashboard │
                         └──────────┬───────────┘
                                    │ HTTP
                                    ▼
                    ┌───────────────────────────────┐
                    │    cmd/api  (Go HTTP, :3000)  │
                    │  /api/v1/jobs                 │
                    │  /api/v1/operations/*          │
                    │  /api/v1/metrics (Prometheus)  │
                    └───────────────┬────────────────┘
                                    │ one Postgres transaction:
                                    │ INSERT jobs + idempotency_keys + outbox_events
                                    ▼
                    ┌───────────────────────────────┐
                    │          PostgreSQL            │ ◄──────────────┐
                    │ jobs, queues, workers,         │                │
                    │ job_leases, outbox_events,     │   reads/writes │
                    │ idempotency_keys,               │   for state,   │
                    │ dead_letter_jobs, job_events,   │   leases, DLQ, │
                    │ job_executions, audit_logs      │   audit        │
                    └───────────────┬────────────────┘                │
                                    │ claim pending outbox rows        │
                                    │ (FOR UPDATE SKIP LOCKED)         │
                                    ▼                                  │
                    ┌───────────────────────────────┐                │
                    │ cmd/publisher (:9102 metrics) │                │
                    │ OutboxPublisher loop          │                │
                    └───────────────┬────────────────┘                │
                                    │ LPUSH jobId
                                    ▼
                    ┌───────────────────────────────┐
                    │            Redis               │
                    │ queue:high / :medium / :low    │
                    │ queue:delayed (sorted set)      │
                    └───────────────┬────────────────┘
                                    │ BRPOP
                                    ▼
                    ┌───────────────────────────────┐
                    │   cmd/worker (:9101 metrics)  │────────────────┘
                    │   Worker main loop            │
                    │   acquire lease → run → done  │
                    └────────────────────────────────┘

                    ┌───────────────────────────────┐
                    │ cmd/scheduler (:9103 metrics) │
                    │ polls queue:delayed every 1s, │
                    │ Lua-script promotes due jobs  │
                    └────────────────────────────────┘

      Prometheus (:9090) scrapes api / worker / publisher / scheduler
      Grafana (:3001) renders dashboards from Prometheus
```

Ports (all configurable via env vars, defaults shown):

| Service    | Port                           | Purpose                  |
| ---------- | ------------------------------ | ------------------------ |
| api        | 3000                           | HTTP API                 |
| worker     | 9101                           | `/metrics` only (no API) |
| publisher  | 9102                           | `/metrics` only          |
| scheduler  | 9103                           | `/metrics` only          |
| prometheus | 9090                           | scrapes the four above   |
| grafana    | 3001 (host) → 3000 (container) | dashboards               |
| postgres   | 5432                           | `queue_forge` DB         |
| redis      | 6379                           | queues                   |

---

## 3. End-to-end workflow (the actual request lifecycle)

### 3.1 Creating a job

```
POST /api/v1/jobs
  → validateIdempotencyKey middleware (requires header "Idempotency-Key")
  → decode & validate JSON body (name 3-255 chars, priority 0-10, maxAttempts 1-10)
  → jobsHandler.CreateJob
  → Postgres transaction (internal/db WithTx):
      1. If Idempotency-Key given, check idempotency_keys for an existing row
         → if found, return the existing job untouched (true idempotent replay)
      2. INSERT INTO jobs (status='PENDING', attempts=0, ...)
      3. INSERT INTO idempotency_keys (key, job_id)         [if key given]
      4. INSERT INTO outbox_events (
           event_type='job.dispatch.requested',
           deduplication_key=`job-created-${jobId}`,
           payload={ jobId, priority, name }
         )
      If the transaction encounters a unique-key collision on the idempotency key
      (concurrent requests with the same key), it re-reads the
      existing job and returns that instead of failing the caller.
  → HTTP 201 with the full job record
```

Note: the API **never touches Redis** here — it only writes to Postgres. Dispatch to Redis is the publisher's job, on its own loop, decoupled from the HTTP request.

### 3.2 Outbox → Redis (cmd/publisher)

`Publisher.Run(ctx)` loops continuously until context cancellation:

```
loop:
  events = queries.ClaimOutboxBatch(ctx, ClaimOutboxBatchParams{
    ClaimedBy: workerID,
    Limit:     50,
  })
     → SELECT ... WHERE processed_at IS NULL AND next_attempt_at <= now()
                  AND (claimed_by IS NULL OR claim_expires_at <= now())
       FOR UPDATE SKIP LOCKED   (so multiple publisher instances never double-claim)
     → UPDATE claimed_by = workerID, claim_expires_at = now() + 30s

  for each event:
     if event.Attempts >= maxRetries(5):
       queries.MarkOutboxFailed(ctx, ...)
       continue
     parse payload JSON → { jobId, priority }
     redisqueue.Enqueue(ctx, jobId, priority)   # LPUSH to queue:high/medium/low
     queries.MarkOutboxProcessed(ctx, event.ID) # processed_at = now(), clears claim
       on failure: queries.MarkOutboxFailed(ctx, MarkOutboxFailedParams{
         ID:        event.ID,
         LastError: err.Error(),
         Backoff:   min(2^attempts seconds, 5 minutes)
       })

  updateLagMetrics()   # Prometheus gauges: oldest-unprocessed age, pending count
  time.Sleep(pollInterval) # default 100ms
```

If the publisher crashes **after** the Redis `LPUSH` but **before** `MarkOutboxProcessed`, the event stays unprocessed and gets claimed in the next cycle. The worker runtime safely handles duplicate delivery via distributed database leases (at-least-once delivery guarantee).

### 3.3 Execution (cmd/worker)

`Worker.Run(ctx)` loops continuously until context cancellation:

```
loop:
  jobId, queueName = poller.Poll(ctx) # BRPOP queue:high, queue:medium, queue:low (blocking)
  if err != nil || jobId == "": continue

  lease = leaseManager.AcquireLease(ctx, jobId, workerId, ttl=30s)
     Postgres transaction:
       SELECT version FROM jobs WHERE id=jobId FOR UPDATE
       DELETE FROM job_leases WHERE job_id=jobId AND expires_at < now()   (clear stale lease)
       INSERT INTO job_leases (job_id, worker_id, fencing_token = job.version+1, expires_at)
         ON CONFLICT DO NOTHING   (so a still-active lease blocks a second worker)
  if no lease acquired: continue   # another worker currently holds this job

  job = queries.GetJobByID(ctx, jobId)
  if job not found:
    leaseManager.ReleaseLease(ctx, jobId, workerId, fencingToken)
    continue

  start lease renewal goroutine (ticker every 10s):
     queries.RenewJobLease(ctx, RenewJobLeaseParams{
       JobID:        jobId,
       WorkerID:     workerId,
       FencingToken: fencingToken,
       ExpiresAt:    now() + ttl,
     })
     → only succeeds if (job_id, worker_id, fencing_token) still matches an
       unexpired lease row — this IS the monotonic fencing check.

  queries.MarkJobRunningWithLease(ctx, jobId, workerId, fencingToken)
     → re-checks lease is active, then sets status='RUNNING', version = version + 1

  err = executor.Execute(ctx, job) # looks up registered handler by job.Name, calls it

  if err == nil:
     queries.MarkJobCompletedWithLease(ctx, jobId, workerId, fencingToken)
        → same lease re-check, then status='COMPLETED', completed_at=now(), version = version + 1
     jobsCompleted.Inc()
  else:
     jobsFailed.Inc()
     if job.Attempts < job.MaxAttempts:
        delay = retryService.CalculateDelay(job.Attempts) # min(5000 * 2^attempts, 60000)ms
        redisqueue.EnqueueDelayed(ctx, jobId, now() + delay) # ZADD queue:delayed
        queries.IncrementJobAttempts(ctx, jobId)
     else:
        deadLetterService.MoveToDeadLetter(ctx, job, err)
           Postgres transaction:
             INSERT INTO dead_letter_jobs (job_id, reason=err.Error())
             UPDATE jobs SET status='DEAD_LETTER', version = version + 1

  stop renewal ticker
  leaseManager.ReleaseLease(ctx, jobId, workerId, fencingToken)   # DELETE the lease row
```

`cmd/worker` uses `JobExecutor` (`cmd/worker/internal/executor`) as an extension point. Domain-specific job handlers are registered into the executor via `executor.Register("job-name", handlerFunc)`.

### 3.4 Delayed jobs (cmd/scheduler)

```
every SCHEDULER_INTERVAL (default 1000ms):
  dueJobIds = redis.ZRangeByScore(ctx, "queue:delayed", "-inf", now)
  for each jobId:
    priority = queries.GetJobPriority(ctx, jobId)   (Postgres lookup)
    if job not found: skip
    targetQueue = resolvePriorityQueue(priority)    (high / medium / low)
    moved = redis.Eval(ctx, MoveDueJobScript, ["queue:delayed", targetQueue], [jobId, now])
       Lua script atomically verifies ZSCORE <= now, LPUSH targetQueue, ZREM queue:delayed
    if moved == 1: delayedJobsPromoted.Inc()
```

The atomic Lua script ensures that multiple scheduler instances racing on the same due job never double-promote it.

### 3.5 Worker lifecycle (registration, heartbeat, shutdown)

```
bootstrap:
  workerId = uuid.NewString()
  registry.Register(ctx, workerId, hostname)
     → INSERT INTO workers (id, hostname, status='ACTIVE', last_heartbeat=now())
  heartbeat.Start(ctx, workerId)
     → ticker every 5000ms: UPDATE workers SET last_heartbeat=now() WHERE id=workerId
  start Prometheus metrics HTTP server on :9101
  go worker.Run(ctx)

on SIGTERM / SIGINT:
  cancel() context                      # stops accepting new jobs from Redis
  wait up to WORKER_SHUTDOWN_TIMEOUT (default 30s) for active job to finish
  worker.ReleaseActiveLease(ctx)        # release active lease if held
  close metrics server, stop heartbeat
  registry.Unregister(ctx, workerId)
     → UPDATE workers SET status='DEAD' WHERE id=workerId
  os.Exit(0)
```

### 3.6 Operator actions (via cmd/api `/operations/*`, used by the dashboard)

- **Retry** a `FAILED`/`DEAD_LETTER` job → back to `PENDING` (optimistic-locked on `version`, blocked if an active lease exists), writes a `job_events` row and an `audit_logs` row.
- **Cancel** a `PENDING`/`RETRYING`/`RUNNING` job → `CANCELED`, same guard rails.
- **Replay** a dead-letter record → validates the underlying job is still `DEAD_LETTER`, flips it to `PENDING`, deletes the `dead_letter_jobs` row, and writes a `job_events` row.
- **Pause/Resume** a queue (`high`/`medium`/`low`) → flips a row in the `queues` table (`ACTIVE` ⇄ `PAUSED`). Stored in Postgres and surfaced in the dashboard.

---

## 4. Database schema (Postgres, via sqlc — `internal/db/`)

| Table                                                                          | Key columns                                                                                                                                                      | Purpose                                                                                                                                                                       |
| ------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `jobs`                                                                         | id, queue_id (nullable FK), name, payload(jsonb), priority, status, attempts, max_attempts, version, run_at, cancellation_requested_at, completed_at             | The job record. Indexed on `(queue_id,status,created_at)` and `(status,run_at)`.                                                                                              |
| `queues`                                                                       | id, name (unique), state(`ACTIVE`/`PAUSED`), version, state_reason, paused_at                                                                                    | Durable queue state (3 logical queues: high/medium/low — see [§8.4](#84-queue-pauseresume-doesnt-actually-pause-anything) on what "durable" really means here).                |
| `workers`                                                                      | id, hostname, status(`ACTIVE`/`DEAD`), last_heartbeat                                                                                                            | Worker registry rows.                                                                                                                                                         |
| `job_leases`                                                                   | job_id (PK), worker_id, lease_token, fencing_token, acquired_at, expires_at, heartbeat_at                                                                        | One row per currently-executing job; enforces single-owner execution + fencing.                                                                                               |
| `outbox_events`                                                                | id, event_type, aggregate_type/id, deduplication_key (unique), payload(jsonb), processed_at, attempts, next_attempt_at, last_error, claimed_by, claim_expires_at | Transactional outbox rows bridging Postgres → Redis.                                                                                                                          |
| `idempotency_keys`                                                             | id, key (unique), job_id                                                                                                                                         | Maps a client-supplied `Idempotency-Key` header to the job it created.                                                                                                        |
| `dead_letter_jobs`                                                             | id, job_id, reason                                                                                                                                               | One row per job that exhausted retries.                                                                                                                                       |
| `job_events`                                                                   | id, job_id, event_type, from_status, to_status, version, actor_type, actor_id, metadata(jsonb)                                                                   | Append-only audit trail of state transitions (operator-triggered ones only — worker-driven transitions like RUNNING/COMPLETED don't write here).                              |
| `job_executions`                                                               | id, job_id, started_at, finished_at, status, error_message                                                                                                       | Stores historical execution details per attempt. Surfaced via the operations API.                                                             |
| `audit_logs`                                                                   | id, actor_id, action, target_type, target_id, reason, outcome, request_id, change_summary(jsonb)                                                                 | Operator-action audit log tracking retry and cancellation commands.                                                                           |
| `workflow_definitions` / `workflow_task_definitions` / `workflow_dependencies` | —                                                                                                                                                                | Planned Phase 11 tables, retained in schema documentation.                                                                                                                    |

Migrations live in `internal/db/migrations/0000`…`0008` and `internal/db/schema.sql`. Type-safe query models and queriers are generated via `sqlc` into `internal/db/gen/`.

### Job state machine (as enforced by `cmd/api`'s state guards)

```
validTransitions = {
  FAILED:      [PENDING],
  DEAD_LETTER: [PENDING],
  PENDING:     [CANCELED],
  RETRYING:    [CANCELED],
  RUNNING:     [CANCELED],
}
```

Plus the worker-driven transitions: `PENDING → RUNNING → COMPLETED`, and `RUNNING → (retry loop back to PENDING via queue:delayed)` or `RUNNING → DEAD_LETTER` on exhausted attempts.

Every operator-initiated transition is double-guarded:

1. **Optimistic concurrency** — the `UPDATE` includes `WHERE version = expectedVersion`; if another request already changed the row, this returns 0 rows and the API returns `409 CONFLICT`.
2. **Active-lease check** — state update first checks for a non-expired row in `job_leases`; if one exists, the whole update is refused (returns `null` → `409`), so an operator can't cancel/retry a job a worker currently holds.

### Queue state machine

```
ACTIVE ⇄ PAUSED
```

(see [§8.4](#84-queue-pauseresume-doesnt-actually-pause-anything) for the caveat that this state isn't actually consulted by dispatch/consumption)

---

## 5. Every directory and package, explained

### `cmd/api/`

| Path                       | Role                                                                                                                                                                                       |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `main.go`                  | Process entrypoint: loads config, connects Postgres and Redis, constructs `Server`, and serves HTTP on `PORT` (default 3000). Handles graceful shutdown.                                   |
| `internal/server/`         | HTTP server multiplexer: configures routes, middleware pipeline, Prometheus `/api/v1/metrics`, and health check `/health`.                                                                |
| `internal/middleware/`     | Request context (tracing ID via `X-Request-Id`), Bearer token authentication for `/api/v1/operations/*`, and Idempotency key validator.                                                    |
| `internal/apierr/`         | Standard JSON error envelope and typed error constructors (`400 Bad Request`, `401 Unauthorized`, `404 Not Found`, `409 Conflict`, `422 Unprocessable Entity`, `500 Internal Error`).     |
| `internal/handler/jobs/`   | Public `POST /api/v1/jobs` handler with transactional outbox event insertion and idempotency key persistence.                                                                              |
| `internal/handler/operations/` | Operations console handlers: `audit` (audit logs), `deadletter` (DLQ list/get/replay), `jobs` (list/get/retry/cancel/executions/events), `metrics` (summary snapshot), `queues` (list/pause/resume), `workers` (list/get). |

### `cmd/worker/`

| Path                    | Role                                                                                                                                                                 |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `main.go`               | Entrypoint: registers worker, launches heartbeat (5s), metrics HTTP listener (:9101), runs `Worker` polling loop, and cleanly unregisters on SIGTERM/SIGINT.         |
| `internal/`             | Main `Worker` loop: polls Redis queues via BRPOP, acquires PostgreSQL distributed lease with fencing tokens, executes job, handles retry backoff or DLQ promotion. |
| `internal/executor/`    | Job handler registry (`Register`, `Execute`).                                                                                                                        |
| `internal/lease/`       | Distributed lease manager: monotonic fencing tokens (`version + 1`), lease renewal loop, and atomic lease release.                                                   |
| `internal/deadletter/`  | Inserts into `dead_letter_jobs` and marks job `DEAD_LETTER` in PostgreSQL.                                                                                           |
| `internal/heartbeat/`   | Heartbeat ticker calling `WorkerRepository.Heartbeat` every 5 seconds.                                                                                               |
| `internal/registry/`    | Registers worker row (`hostname`, status `ACTIVE`) and unregisters (`DEAD`) on shutdown.                                                                             |
| `internal/retry/`       | Exponential backoff calculation (`min(5000 * 2^attempts, 60000)ms`) and pushes retry job to Redis `queue:delayed`.                                                  |

### `cmd/publisher/`

| Path                   | Role                                                                                                                                                                 |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `main.go`              | Entrypoint: starts metrics HTTP server (:9102), connects Redis & Postgres, and runs `Publisher` loop.                                                                |
| `internal/publisher/`  | Batch claims pending outbox rows (`FOR UPDATE SKIP LOCKED`), pushes jobs to Redis priority queues (`queue:high/medium/low`), updates lag gauges, and handles backoff. |

### `cmd/scheduler/`

| Path                  | Role                                                                                                                                                                 |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `main.go`             | Entrypoint: starts metrics HTTP server (:9103), connects Redis & Postgres, runs `Promoter` loop every 1s.                                                            |
| `internal/promoter/`  | Queries due jobs in Redis `queue:delayed`, queries Postgres for job priority, and promotes jobs atomically to the target priority queue using an atomic Lua script. |

### `apps/dashboard/`

| Path                                           | Role                                                                                                                                                         |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `src/main.tsx` / `App.tsx`                     | Vite/React 19 SPA entry; routes for Overview, Queues, Jobs, Workers, Dead Letter, Audit Log.                                                                |
| `src/components/`                              | `Layout`, `MetricCard`, `StatusBadge` presentational components.                                                                                             |
| `src/api/`                                     | Hand-rolled `fetch` client hitting `/api/v1/operations/*` with Bearer auth, TanStack React Query hooks.                                                      |
| `src/pages/`                                   | Dashboard pages consuming the API: Overview, Queues, Jobs, Workers, DeadLetter, AuditLog.                                                                    |

### `internal/db/`

| Path                 | Role                                                                                                                                                                   |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `client.go`          | Connects `pgxpool`, generic transaction runners (`WithTx`, `WithTxExec`).                                                                                              |
| `schema.sql`         | Complete PostgreSQL DDL defining all tables, indexes, constraints, and defaults.                                                                                       |
| `migrations/`        | Historical SQL migrations (`0000`–`0008`) for database schema evolution.                                                                                               |
| `query/*.sql`        | SQL query source files for sqlc (jobs, outbox, leases, idempotency, queues, workers, dead_letters, audit_logs, metrics, executions, events).                           |
| `gen/`               | Generated type-safe Go structs and Querier interface via `sqlc`.                                                                                                       |

### `internal/redisqueue/`

| Path            | Role                                                                                                                                                                   |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `client.go`     | Go-redis client connection manager with ping health checks.                                                                                                            |
| `queue.go`      | High-level queue operations: `Enqueue` (LPUSH priority bucket), `EnqueueDelayed` (ZADD), `Dequeue` (blocking BRPOP across priority queues), `GetQueueDepth` (LLEN).  |
| `constants.go`  | Redis queue keys (`queue:high`, `queue:medium`, `queue:low`, `queue:delayed`).                                                                                         |
| `lua.go`        | Atomic Lua script for due delayed job promotion (`MoveDueJobScript`).                                                                                                  |

### `internal/logger/` & `internal/metrics/`

- **internal/logger** — Structured JSON logging via `zerolog`.
- **internal/metrics** — Prometheus metrics registry with standard metric names, types, and histogram buckets.

### `tests/integration/`

`tests/integration/pipeline_test.go` contains comprehensive in-memory pipeline tests covering all 7 core scenarios:
1. End-to-end job creation, outbox publishing, lease acquisition, execution, and completion.
2. Retry backoff and Dead-Letter Queue promotion on exhausted attempts.
3. Worker crash and lease expiry recovery by a second worker.
4. Publisher crash recovery with idempotent Redis deduplication.
5. Delayed job scheduling and Redis sorted-set promotion.
6. Queue pause behavior.
7. Concurrent versioned command conflicts (optimistic concurrency).
### `infrastructure/`

- `docker/docker-compose.yml` — spins up Postgres 17, Redis 7, and all four Go microservices (`api`, `worker`, `publisher`, `scheduler`) built from their respective multi-stage Dockerfiles, plus Prometheus and Grafana. Maps the ports from [§2](#2-high-level-architecture).
- `monitoring/prometheus/prometheus.yml` — scrape configs for `api:3000`, `worker:9101`, `publisher:9102`, `scheduler:9103`, every 15s.
- `monitoring/grafana/` — provisioning files (datasources + dashboard definitions) for Grafana to auto-load a Queue Forge dashboard.

---

## 6. Every API endpoint

Base URL: `http://localhost:3000`. All JSON. Errors use the envelope `{ "error": { "code", "message" } }`.

### Unauthenticated

| Method   | Path              | Body / Query                                                                                                                                         | Behavior                                                                                                                             |
| -------- | ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `GET`    | `/health`         | —                                                                                                                                                    | `{ success: true, service: "api" }`. Health check endpoint.                                                                          |
| `GET`    | `/api/v1/metrics` | —                                                                                                                                                    | Prometheus text exposition of the shared metrics registry. This is what Prometheus scrapes.                                          |
| `POST`   | `/api/v1/jobs`    | Headers: `Idempotency-Key` (required). Body: `{ name (3-255 chars), payload (object), priority? (0-10, default 0), maxAttempts? (1-10, default 3) }` | Creates a job ([§3.1](#31-creating-a-job)). Returns `201` with the full job row. Replaying the same idempotency key returns the original job, still `201`. |

### Authenticated (`Authorization: Bearer <token>`, default token `dev-token`, configurable via `OPERATIONS_AUTH_TOKEN` as a comma-separated list)

All under `/api/v1/operations`. A `401 UNAUTHORIZED` is returned for any of these without a valid bearer token.

**Metrics**

| Method | Path                          | Query                     | Behavior                                                                                                                      |
| ------ | ----------------------------- | ------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `GET`  | `/operations/metrics`         | `windowMinutes` (1-1440, default 15) | Returns a JSON snapshot: job/worker counts by status, jobs created/completed/failed in the time window, throughput/min, success rate, total retry attempts. |

**Queues**

| Method | Path                                    | Body        | Behavior                                                                                                                        |
| ------ | --------------------------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `GET`  | `/operations/queues`                    | —           | Lists `high`/`medium`/`low` with authoritative Postgres depth (jobs in `PENDING`/`RETRYING` for that priority bucket), oldest pending timestamp, status breakdown, Redis execution depth (best-effort), and current pause state. |
| `GET`  | `/operations/queues/:name`              | —           | Same shape for one queue. `404` if `name` isn't `high`/`medium`/`low`.                                                          |
| `POST` | `/operations/queues/:name/pause`        | `{ reason }` | Sets `queues.state='PAUSED'`. `409` if already paused.                                                                          |
| `POST` | `/operations/queues/:name/resume`       | `{ reason }` | Sets `queues.state='ACTIVE'`. `409` if already active.                                                                          |

**Jobs (operator console)**

| Method | Path                                  | Query/Body                                                     | Behavior                                                                                                                 |
| ------ | ------------------------------------- | -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `GET`  | `/operations/jobs`                    | `queueId?, status?, name? (substring match), createdFrom?, createdTo?, limit (1-100, default 50), cursor?` | Cursor-paginated list of job summaries, ordered newest-updated first. |
| `GET`  | `/operations/jobs/:id`                | —                                                              | Full job record. `404` if not found.                                                                                     |
| `GET`  | `/operations/jobs/:id/executions`     | —                                                              | List of `job_executions` rows for the job.                                                                               |
| `GET`  | `/operations/jobs/:id/events`         | —                                                              | List of `job_events` rows (operator-driven transitions only), oldest first.                                              |
| `POST` | `/operations/jobs/:id/retry`          | `{ reason }`                                                   | Only valid from `FAILED`/`DEAD_LETTER` → `PENDING`. `422` if current status doesn't allow it, `409` on version conflict or active lease. Writes a `job_events` row and a real `audit_logs` row. |
| `POST` | `/operations/jobs/:id/cancel`         | `{ reason }`                                                   | Only valid from `PENDING`/`RETRYING`/`RUNNING` → `CANCELED`. Same guards as retry. |

**Dead-letter jobs**

| Method | Path                                          | Query/Body                                                    | Behavior                                                                                                                    |
| ------ | --------------------------------------------- | ------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `GET`  | `/operations/dead-letter-jobs`                | `queueId?, reasonCode?, limit, cursor?`                       | Cursor-paginated DLQ list.                                                                                                  |
| `GET`  | `/operations/dead-letter-jobs/:id`            | —                                                             | One DLQ record. `404` if missing.                                                                                           |
| `POST` | `/operations/dead-letter-jobs/:id/replay`     | `{ reason }`                                                  | Verifies the underlying job is still `DEAD_LETTER`, flips it to `PENDING`, deletes the DLQ row, and writes a `job_events` row. |

**Workers**

| Method | Path                            | Query                                                           | Behavior                                                                                                                     |
| ------ | ------------------------------- | --------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `GET`  | `/operations/workers`           | `health? (HEALTHY/STALE/OFFLINE), limit, cursor?`               | Cursor-paginated worker list with derived health (`HEALTHY` = ACTIVE status + heartbeat younger than `WORKER_STALE_AFTER_MS`, default 15000ms; `STALE` = ACTIVE but old/missing heartbeat; `OFFLINE` = status isn't ACTIVE). |
| `GET`  | `/operations/workers/:id`       | —                                                               | One worker's detail + health. `404` if missing.                                                                              |

**Audit log**

| Method | Path                            | Query                                                                         | Behavior                                                                                     |
| ------ | ------------------------------- | ----------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `GET`  | `/operations/audit-logs`        | `action?, actorId?, targetType?, targetId?, from?, to?, limit, cursor?`       | Cursor-paginated, read-only. The only writers into this table today are the job retry/cancel command handlers. |

All cursor pagination here uses the same pattern: a base64url-encoded `{ createdAt/updatedAt, id }` tuple, validated and decoded server-side, returned as `nextCursor` in the response envelope (`{ data: { items, nextCursor } }`) when more rows exist.

---

## 7. Services & packages, one-paragraph summaries

- **cmd/api** — The only public HTTP-facing service. Splits cleanly into an unauthenticated job-creation endpoint (`POST /api/v1/jobs`) and an authenticated `/api/v1/operations` console used by the dashboard. Handlers follow standard Go idiomatic architecture with type-safe sqlc queries, transactional outbox writes, and keyset cursor pagination.
- **cmd/worker** — A long-running daemon with no HTTP API of its own besides `/metrics`. Owns the lease/fencing protocol that guarantees single-worker execution with monotonic fencing tokens (`version + 1`), lease renewal tickers, and exponential backoff retry scheduling.
- **cmd/publisher** — A focused bridge service. Continuously claims pending outbox rows from PostgreSQL using `FOR UPDATE SKIP LOCKED` and pushes them to Redis priority queues with lag metrics and backoff.
- **cmd/scheduler** — Polling loop promoting due jobs from the Redis delayed sorted set (`queue:delayed`) into the appropriate priority queue (`queue:high/medium/low`) via an atomic Lua script.
- **apps/dashboard** — A clean React/Vite SPA operator UI that interfaces directly with `cmd/api`'s `/operations/*` API surface.
- **internal/db** — Database layer providing `pgxpool` connections, transactions (`WithTx`), raw SQL migrations, and type-safe `sqlc` models and queriers.
- **internal/redisqueue** — Centralizes the `go-redis` client, priority LPUSH/BRPOP queue primitives, delayed sorted sets, and atomic Lua promotion scripts.
- **internal/logger & internal/metrics** — Cross-cutting Zerolog structured JSON logging and Prometheus metrics registry with matching metric names, types, and histogram buckets.

---

## 8. Design Notes & Extension Points

### 8.1 Worker Job Handler Registration

`cmd/worker` provides a modular `JobExecutor` (`cmd/worker/internal/executor`). The base worker runtime serves as a generic execution host; domain-specific job handlers (such as email dispatch, file generation, payment processing) are registered via:

```go
executor.Register("send-email", func(ctx context.Context, job *gen.Job) error {
    // domain execution logic
    return nil
})
```

If a job is polled for which no handler is registered, the executor returns an error, triggering exponential retry backoff, and ultimately routing to the Dead-Letter Queue if attempts are exhausted.

### 8.2 Worker Health Monitoring

Worker health is maintained through heartbeat records in PostgreSQL:
- Active workers update their `last_heartbeat` timestamp every 5 seconds.
- The operations API (`GET /api/v1/operations/workers`) dynamically derives health:
  - `HEALTHY`: Active worker with heartbeat within `WORKER_STALE_AFTER_MS` (default 15s).
  - `STALE`: Active worker with heartbeat older than threshold.
  - `OFFLINE`: Worker with non-active status (`DEAD`).

### 8.3 Dead-Letter Queue & Event Auditing

Jobs that exceed their configured `max_attempts` are automatically moved to `dead_letter_jobs` with their terminal error message. When an operator replays a dead-letter record via `POST /api/v1/operations/dead-letter-jobs/:id/replay`:
- The job status is reset to `PENDING`.
- The `dead_letter_jobs` entry is removed.
- An audit event is recorded in `job_events`.

### 8.4 Queue Pause Coordination

Queue state (`ACTIVE` ⇄ `PAUSED`) is stored in PostgreSQL and managed through `POST /api/v1/operations/queues/:name/pause` and `resume`. This state is visible to operators in the React dashboard.

### 8.5 Distributed Leases & Monotonic Fencing

To ensure that only one worker executes a job at any given time, PostgreSQL distributed leases (`job_leases`) are acquired with fencing tokens calculated as `job.version + 1`. Workers periodically renew active leases while executing. If a worker hangs or crashes:
- Its lease expires automatically based on TTL (default 30s).
- Another worker can reclaim the expired lease.
- If the original worker recovers, any subsequent write attempt by that worker is rejected because its fencing token is obsolete.

### 8.6 Idempotent Transactional Outbox

Job submissions (`POST /api/v1/jobs`) atomically insert the job, an idempotency key (if supplied), and an outbox event in a single PostgreSQL transaction. The publisher service reads from the outbox table using `FOR UPDATE SKIP LOCKED`, preventing double-dispatch across multiple publisher replicas.
