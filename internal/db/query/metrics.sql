-- name: GetJobsByStatus :many
SELECT status, count(*)::bigint AS count
FROM jobs
GROUP BY status;

-- name: GetWorkersByStatus :many
SELECT status, count(*)::bigint AS count
FROM workers
GROUP BY status;

-- name: GetTotalRetryAttempts :one
SELECT coalesce(sum(attempts), 0)::bigint AS value
FROM jobs;

-- name: GetJobsCreatedSince :one
SELECT count(*)::bigint AS value
FROM jobs
WHERE created_at >= $1;

-- name: GetJobsCompletedSince :one
SELECT count(*)::bigint AS value
FROM jobs
WHERE status = 'COMPLETED' AND updated_at >= $1;

-- name: GetJobsFailedSince :one
SELECT count(*)::bigint AS value
FROM jobs
WHERE status IN ('FAILED', 'DEAD_LETTER') AND updated_at >= $1;

