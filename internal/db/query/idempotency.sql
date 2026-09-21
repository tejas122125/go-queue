-- name: FindIdempotencyKey :one
SELECT * FROM idempotency_keys
WHERE key = $1
LIMIT 1;

-- name: CreateIdempotencyKey :one
INSERT INTO idempotency_keys (
    key,
    job_id,
    created_at
) VALUES (
    $1, $2, now()
)
RETURNING *;

