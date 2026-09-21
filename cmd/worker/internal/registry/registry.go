package registry

import (
	"context"
	"os"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	dbgen "queue-forge/internal/db/gen"
)

// WorkerRegistry manages worker registration and deregistration in PostgreSQL.
type WorkerRegistry struct {
	queries *dbgen.Queries
}

// New constructs a WorkerRegistry.
func New(pool *pgxpool.Pool) *WorkerRegistry {
	return &WorkerRegistry{queries: dbgen.New(pool)}
}

// Register generates a worker ID and registers the worker with its hostname.
func (r *WorkerRegistry) Register(ctx context.Context) (uuid.UUID, error) {
	workerID := uuid.New()
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	_, err = r.queries.RegisterWorker(ctx, dbgen.RegisterWorkerParams{
		ID:       pgtype.UUID{Bytes: workerID, Valid: true},
		Hostname: hostname,
		Status:   "ACTIVE",
	})
	return workerID, err
}

// Unregister marks the worker status as DEAD.
func (r *WorkerRegistry) Unregister(ctx context.Context, workerID uuid.UUID) error {
	return r.queries.MarkWorkerDead(ctx, pgtype.UUID{Bytes: workerID, Valid: true})
}

