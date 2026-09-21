-- name: GetQueueState :one
SELECT state, paused_at, state_reason
FROM queues
WHERE name = $1
LIMIT 1;

-- name: SetQueueState :one
UPDATE queues
SET state = $2,
    paused_at = $3,
    state_reason = $4,
    updated_at = now()
WHERE name = $1
RETURNING state, paused_at;

-- name: ListQueues :many
SELECT * FROM queues
ORDER BY name ASC;

-- name: CreateQueue :one
INSERT INTO queues (
    name,
    state,
    version,
    state_reason,
    paused_at,
    created_at,
    updated_at
) VALUES (
    $1, $2, 0, $3, $4, now(), now()
)
RETURNING *;

