package internal

import (
	"context"
	"testing"
	"time"
)

func TestWorkerConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.LeaseTTL != 30*time.Second {
		t.Errorf("expected 30s lease TTL, got %v", cfg.LeaseTTL)
	}
	if cfg.LeaseRenewalInterval != 10*time.Second {
		t.Errorf("expected 10s lease renewal interval, got %v", cfg.LeaseRenewalInterval)
	}
}

func TestWorkerShutdown(t *testing.T) {
	worker := &WorkerService{}
	worker.RequestShutdown()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := worker.Start(ctx)
	if err != nil {
		t.Fatalf("expected nil error on immediate shutdown, got %v", err)
	}
}

