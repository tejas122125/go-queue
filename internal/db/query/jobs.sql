-- name: CreateJob :one
INSERT INTO jobs (
    queue_id,
    name,
    payload,
    priority,
    status,
    attempts,
    max_attempts,
    version,
    run_at,
    created_at,
    updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, 0, $8, now(), now()
)
RETURNING *;

-- name: FindJobByID :one
SELECT * FROM jobs
WHERE id = $1
LIMIT 1;

-- name: FindJobForUpdate :one
SELECT id, version FROM jobs
WHERE id = $1
FOR UPDATE;

-- name: FindJobPriority :one
SELECT priority FROM jobs
WHERE id = $1
LIMIT 1;

-- name: MarkJobRunningWithLease :one
UPDATE jobs
SET status = 'RUNNING',
    version = version + 1,
    updated_at = now()
WHERE id = $1
  AND EXISTS (
    SELECT 1 FROM job_leases
    WHERE job_id = $1
      AND worker_id = $2
      AND fencing_token = $3
      AND expires_at > now()
  )
RETURNING id;

-- name: MarkJobCompletedWithLease :one
UPDATE jobs
SET status = 'COMPLETED',
    version = version + 1,
    completed_at = now(),
    updated_at = now()
WHERE id = $1
  AND EXISTS (
    SELECT 1 FROM job_leases
    WHERE job_id = $1
      AND worker_id = $2
      AND fencing_token = $3
      AND expires_at > now()
  )
RETURNING id;

-- name: IncrementJobAttempts :exec
UPDATE jobs
SET attempts = attempts + 1,
    updated_at = now()
WHERE id = $1;

-- name: UpdateJobStatusWithVersion :one
UPDATE jobs
SET status = $2,
    version = version + 1,
    updated_at = now()
WHERE id = $1
  AND version = $3
  AND NOT EXISTS (
    SELECT 1 FROM job_leases
    WHERE job_id = $1 AND expires_at > now()
  )
RETURNING *;

-- name: SetJobStatus :exec
UPDATE jobs
SET status = $2,
    updated_at = now()
WHERE id = $1;

-- name: GetJobGroups :many
SELECT priority, status, count(*) AS count, min(created_at)::timestamp AS oldest_created_at
FROM jobs
GROUP BY priority, status;

