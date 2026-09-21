package deadletter

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	internaldb "queue-forge/internal/db"
	dbgen "queue-forge/internal/db/gen"
)

// DeadLetterService records dead letter jobs and transitions their status to DEAD_LETTER.
type DeadLetterService struct {
	pool *pgxpool.Pool
}

// New constructs a DeadLetterService.
func New(pool *pgxpool.Pool) *DeadLetterService {
	return &DeadLetterService{pool: pool}
}

// MoveToDeadLetter inserts a record into dead_letter_jobs and sets jobs.status = 'DEAD_LETTER' inside a transaction.
func (d *DeadLetterService) MoveToDeadLetter(ctx context.Context, job *dbgen.Job, execErr error) error {
	reason := "Unknown Error"
	if execErr != nil {
		reason = execErr.Error()
	}

	return internaldb.WithTxExec(ctx, d.pool, func(tx pgx.Tx) error {
		q := dbgen.New(tx)

		_, err := q.InsertDeadLetterJob(ctx, dbgen.InsertDeadLetterJobParams{
			JobID:  job.ID,
			Reason: pgtype.Text{String: reason, Valid: true},
		})
		if err != nil {
			return fmt.Errorf("inserting dead letter job: %w", err)
		}

		if err := q.SetJobStatus(ctx, dbgen.SetJobStatusParams{
			ID:     job.ID,
			Status: "DEAD_LETTER",
		}); err != nil {
			return fmt.Errorf("updating job status to DEAD_LETTER: %w", err)
		}

		return nil
	})
}

