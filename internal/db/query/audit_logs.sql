-- name: CreateAuditLog :one
INSERT INTO audit_logs (
    actor_id,
    action,
    target_type,
    target_id,
    reason,
    outcome,
    request_id,
    change_summary,
    created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, now()
)
RETURNING *;

-- name: ListAuditLogs :many
SELECT * FROM audit_logs
ORDER BY created_at DESC, id DESC;

