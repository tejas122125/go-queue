-- name: DeleteExpiredLease :exec
DELETE FROM job_leases
WHERE job_id = $1
  AND expires_at < now();

-- name: AcquireLease :one
INSERT INTO job_leases (
    job_id,
    worker_id,
    fencing_token,
    acquired_at,
    expires_at,
    heartbeat_at
) VALUES (
    $1, $2, $3, now(), $4, now()
)
ON CONFLICT (job_id) DO NOTHING
RETURNING lease_token, fencing_token;

-- name: RenewLease :one
UPDATE job_leases
SET expires_at = $4,
    heartbeat_at = now()
WHERE job_id = $1
  AND worker_id = $2
  AND fencing_token = $3
  AND expires_at > now()
RETURNING job_id;

-- name: ReleaseLease :exec
DELETE FROM job_leases
WHERE job_id = $1
  AND worker_id = $2
  AND fencing_token = $3;

-- name: HasActiveLease :one
SELECT job_id
FROM job_leases
WHERE job_id = $1
  AND worker_id = $2
  AND fencing_token = $3
  AND expires_at > now()
LIMIT 1;

-- name: FindActiveLeaseByJobID :one
SELECT job_id
FROM job_leases
WHERE job_id = $1
  AND expires_at > now()
LIMIT 1;

