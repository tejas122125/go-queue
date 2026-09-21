-- name: CreateOutboxEvent :one
INSERT INTO outbox_events (
    event_type,
    aggregate_type,
    aggregate_id,
    deduplication_key,
    payload,
    next_attempt_at,
    created_at
) VALUES (
    $1, $2, $3, $4, $5, now(), now()
)
RETURNING *;

-- name: ClaimOutboxBatch :many
WITH candidates AS (
    SELECT id
    FROM outbox_events
    WHERE processed_at IS NULL
      AND next_attempt_at <= now()
      AND (claimed_by IS NULL OR claim_expires_at <= now())
    ORDER BY created_at ASC
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE outbox_events
SET claimed_by = $1,
    claim_expires_at = $3
WHERE id IN (SELECT id FROM candidates)
RETURNING *;

-- name: MarkOutboxProcessed :exec
UPDATE outbox_events
SET processed_at = now(),
    claimed_by = NULL,
    claim_expires_at = NULL
WHERE id = $1;

-- name: MarkOutboxFailed :exec
UPDATE outbox_events
SET attempts = attempts + 1,
    last_error = $2,
    next_attempt_at = now() + least((power(2, attempts) * interval '1 second'), interval '5 minutes'),
    claimed_by = NULL,
    claim_expires_at = NULL
WHERE id = $1;

-- name: GetOutboxLag :one
SELECT count(*)::bigint AS count, min(created_at)::timestamp AS oldest_created_at
FROM outbox_events
WHERE processed_at IS NULL;

