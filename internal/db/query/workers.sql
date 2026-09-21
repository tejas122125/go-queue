-- name: RegisterWorker :one
INSERT INTO workers (
    id,
    hostname,
    status,
    last_heartbeat,
    created_at
) VALUES (
    $1, $2, $3, now(), now()
)
ON CONFLICT (id) DO UPDATE
SET hostname = $2,
    status = $3,
    last_heartbeat = now()
RETURNING *;

-- name: UpdateWorkerHeartbeat :exec
UPDATE workers
SET last_heartbeat = now()
WHERE id = $1;

-- name: MarkWorkerDead :exec
UPDATE workers
SET status = 'DEAD'
WHERE id = $1;

-- name: FindWorkerByID :one
SELECT * FROM workers
WHERE id = $1
LIMIT 1;

-- name: ListWorkers :many
SELECT * FROM workers
ORDER BY created_at DESC;

