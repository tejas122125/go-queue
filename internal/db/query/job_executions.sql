-- name: CreateJobExecution :one
INSERT INTO job_executions (
    job_id,
    started_at,
    finished_at,
    status,
    error_message
) VALUES (
    $1, $2, $3, $4, $5
)
RETURNING *;

-- name: GetJobExecutionsByJobID :many
SELECT * FROM job_executions
WHERE job_id = $1
ORDER BY started_at DESC;

