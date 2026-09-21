-- name: InsertDeadLetterJob :one
INSERT INTO dead_letter_jobs (
    job_id,
    reason,
    created_at
) VALUES (
    $1, $2, now()
)
RETURNING *;

-- name: FindDeadLetterByID :one
SELECT * FROM dead_letter_jobs
WHERE id = $1
LIMIT 1;

-- name: FindDeadLetterByJobID :one
SELECT * FROM dead_letter_jobs
WHERE job_id = $1
LIMIT 1;

-- name: DeleteDeadLetter :exec
DELETE FROM dead_letter_jobs
WHERE id = $1;

-- name: ListDeadLetters :many
SELECT * FROM dead_letter_jobs
ORDER BY created_at DESC, id DESC;

