package promoter

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"queue-forge/internal/redisqueue"
)

func TestResolvePriorityQueue(t *testing.T) {
	tests := []struct {
		priority int
		expected string
	}{
		{10, redisqueue.QueueHigh},
		{15, redisqueue.QueueHigh},
		{5, redisqueue.QueueMedium},
		{9, redisqueue.QueueMedium},
		{4, redisqueue.QueueLow},
		{0, redisqueue.QueueLow},
	}

	for _, tt := range tests {
		got := redisqueue.ResolveQueue(tt.priority)
		if got != tt.expected {
			t.Errorf("ResolveQueue(%d) = %s, expected %s", tt.priority, got, tt.expected)
		}
	}
}

func TestPromoterWithMiniredis(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close()

	ctx := context.Background()
	log := zerolog.Nop()

	p := &DelayedJobPromoter{
		rdb:   rdb,
		queue: redisqueue.NewQueueService(rdb),
		log:   log,
	}

	// When no jobs are delayed, PromoteDueJobs returns 0
	promoted, err := p.PromoteDueJobs(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if promoted != 0 {
		t.Fatalf("expected 0 promoted jobs, got %d", promoted)
	}

	// Add a job due in the future
	now := time.Now().UnixMilli()
	if err := rdb.ZAdd(ctx, redisqueue.QueueDelayed, redis.Z{
		Score:  float64(now + 10000),
		Member: "job-future",
	}).Err(); err != nil {
		t.Fatalf("failed to add future job: %v", err)
	}

	promoted, err = p.PromoteDueJobs(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if promoted != 0 {
		t.Fatalf("expected 0 promoted for future job, got %d", promoted)
	}
}

