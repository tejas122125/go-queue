-- name: CreateJobEvent :one
INSERT INTO job_events (
    job_id,
    event_type,
    from_status,
    to_status,
    version,
    actor_type,
    actor_id,
    metadata,
    created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, now()
)
RETURNING *;

-- name: GetJobEventsByJobID :many
SELECT * FROM job_events
WHERE job_id = $1
ORDER BY created_at ASC;

