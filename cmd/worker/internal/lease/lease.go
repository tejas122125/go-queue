package lease

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	internaldb "queue-forge/internal/db"
	dbgen "queue-forge/internal/db/gen"
)

// Handle holds the lease credentials for an actively leased job.
type Handle struct {
	LeaseToken   uuid.UUID
	FencingToken int32
}

// Repository encapsulates atomic distributed lease operations with fencing tokens.
type Repository struct {
	pool    *pgxpool.Pool
	queries *dbgen.Queries
}

// New constructs a lease Repository.
func New(pool *pgxpool.Pool) *Repository {
	return &Repository{
		pool:    pool,
		queries: dbgen.New(pool),
	}
}

// AcquireLease attempts to acquire an exclusive execution lease for a job.
// Uses SELECT FOR UPDATE on the job row, clears expired leases, and creates a lease with fencingToken = job.version + 1.
// Returns nil if another active lease exists or the job doesn't exist.
func (r *Repository) AcquireLease(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID, ttl time.Duration) (*Handle, error) {
	now := time.Now()
	expiresAt := now.Add(ttl)

	jID := pgtype.UUID{Bytes: jobID, Valid: true}
	wID := pgtype.UUID{Bytes: workerID, Valid: true}

	return internaldb.WithTx(ctx, r.pool, func(tx pgx.Tx) (*Handle, error) {
		q := dbgen.New(tx)

		// 1. Lock job row and get version
		job, err := q.FindJobForUpdate(ctx, jID)
		if err != nil {
			if err == pgx.ErrNoRows {
				return nil, nil
			}
			return nil, fmt.Errorf("locking job row: %w", err)
		}

		// 2. Delete any expired lease
		if err := q.DeleteExpiredLease(ctx, jID); err != nil {
			return nil, fmt.Errorf("deleting expired lease: %w", err)
		}

		// 3. Insert new lease (ON CONFLICT DO NOTHING ensures single winner)
		row, err := q.AcquireLease(ctx, dbgen.AcquireLeaseParams{
			JobID:        jID,
			WorkerID:     wID,
			FencingToken: job.Version + 1,
			ExpiresAt:    pgtype.Timestamp{Time: expiresAt, Valid: true},
		})
		if err != nil {
			if err == pgx.ErrNoRows {
				// Another worker holds active lease
				return nil, nil
			}
			return nil, fmt.Errorf("inserting job lease: %w", err)
		}

		return &Handle{
			LeaseToken:   row.LeaseToken.Bytes,
			FencingToken: row.FencingToken,
		}, nil
	})
}

// RenewLease extends the expiration of an active lease if the fencing token matches.
func (r *Repository) RenewLease(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID, fencingToken int32, ttl time.Duration) (bool, error) {
	newExpiry := time.Now().Add(ttl)
	_, err := r.queries.RenewLease(ctx, dbgen.RenewLeaseParams{
		JobID:        pgtype.UUID{Bytes: jobID, Valid: true},
		WorkerID:     pgtype.UUID{Bytes: workerID, Valid: true},
		FencingToken: fencingToken,
		ExpiresAt:    pgtype.Timestamp{Time: newExpiry, Valid: true},
	})
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ReleaseLease removes the lease record upon job completion or termination.
func (r *Repository) ReleaseLease(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID, fencingToken int32) error {
	return r.queries.ReleaseLease(ctx, dbgen.ReleaseLeaseParams{
		JobID:        pgtype.UUID{Bytes: jobID, Valid: true},
		WorkerID:     pgtype.UUID{Bytes: workerID, Valid: true},
		FencingToken: fencingToken,
	})
}

// MarkRunningWithLease transitions job to RUNNING and increments version if the lease is still valid.
func (r *Repository) MarkRunningWithLease(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID, fencingToken int32) (bool, error) {
	_, err := r.queries.MarkJobRunningWithLease(ctx, dbgen.MarkJobRunningWithLeaseParams{
		ID:           pgtype.UUID{Bytes: jobID, Valid: true},
		WorkerID:     pgtype.UUID{Bytes: workerID, Valid: true},
		FencingToken: fencingToken,
	})
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// MarkCompletedWithLease transitions job to COMPLETED and sets completed_at if the lease is still valid.
func (r *Repository) MarkCompletedWithLease(ctx context.Context, jobID uuid.UUID, workerID uuid.UUID, fencingToken int32) (bool, error) {
	_, err := r.queries.MarkJobCompletedWithLease(ctx, dbgen.MarkJobCompletedWithLeaseParams{
		ID:           pgtype.UUID{Bytes: jobID, Valid: true},
		WorkerID:     pgtype.UUID{Bytes: workerID, Valid: true},
		FencingToken: fencingToken,
	})
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// IncrementAttempts increments the attempt counter of a job.
func (r *Repository) IncrementAttempts(ctx context.Context, jobID uuid.UUID) error {
	return r.queries.IncrementJobAttempts(ctx, pgtype.UUID{Bytes: jobID, Valid: true})
}

