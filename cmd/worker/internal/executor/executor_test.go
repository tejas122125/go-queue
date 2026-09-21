package executor

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog"

	dbgen "queue-forge/internal/db/gen"
)

func TestJobExecutor(t *testing.T) {
	log := zerolog.Nop()
	exec := New(log)

	executed := false
	exec.Register("send-email", func(ctx context.Context, job *dbgen.Job) error {
		executed = true
		return nil
	})

	jobID := uuid.New()
	job := &dbgen.Job{
		ID:   pgtype.UUID{Bytes: jobID, Valid: true},
		Name: "send-email",
	}

	ctx := context.Background()
	if err := exec.Execute(ctx, job); err != nil {
		t.Fatalf("unexpected error executing registered handler: %v", err)
	}
	if !executed {
		t.Fatal("expected handler to be invoked")
	}

	// Test unregistered job name returns error matching TS behavior
	unregisteredJob := &dbgen.Job{
		ID:   pgtype.UUID{Bytes: jobID, Valid: true},
		Name: "unknown-job",
	}

	err := exec.Execute(ctx, unregisteredJob)
	if err == nil {
		t.Fatal("expected error for unregistered job, got nil")
	}
	if !strings.Contains(err.Error(), "No handler registered for job name: unknown-job") {
		t.Errorf("expected specific error message, got: %v", err)
	}
}

