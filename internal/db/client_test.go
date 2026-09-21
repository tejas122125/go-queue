package db

import (
	"context"
	"testing"
	"time"

	dbgen "queue-forge/internal/db/gen"
)

func TestClientInvalidDSN(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := New(ctx, "invalid-dsn://bad-url")
	if err == nil {
		t.Fatal("expected error on invalid DSN, got nil")
	}
}

func TestGeneratedQuerierInterface(t *testing.T) {
	// Verify that dbgen.Queries implements dbgen.Querier
	var _ dbgen.Querier = (*dbgen.Queries)(nil)
}

