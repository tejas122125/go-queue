package redisqueue

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestResolveQueue(t *testing.T) {
	tests := []struct {
		priority int
		expected string
	}{
		{10, QueueHigh},
		{15, QueueHigh},
		{5, QueueMedium},
		{8, QueueMedium},
		{0, QueueLow},
		{1, QueueLow},
		{4, QueueLow},
	}

	for _, tt := range tests {
		got := ResolveQueue(tt.priority)
		if got != tt.expected {
			t.Errorf("ResolveQueue(%d) = %s, expected %s", tt.priority, got, tt.expected)
		}
	}
}

func TestQueueServiceOperations(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close()

	qs := NewQueueService(rdb)
	ctx := context.Background()

	// Test Enqueue
	if err := qs.Enqueue(ctx, "job-low", 1); err != nil {
		t.Fatalf("failed to enqueue low: %v", err)
	}
	if err := qs.Enqueue(ctx, "job-med", 5); err != nil {
		t.Fatalf("failed to enqueue med: %v", err)
	}
	if err := qs.Enqueue(ctx, "job-high", 10); err != nil {
		t.Fatalf("failed to enqueue high: %v", err)
	}

	// Test GetQueueDepth
	depth, err := qs.GetQueueDepth(ctx, QueueHigh)
	if err != nil || depth != 1 {
		t.Fatalf("expected high queue depth 1, got %d (err: %v)", depth, err)
	}

	// Test Dequeue strict priority order: HIGH first, then MED, then LOW
	job1, err := qs.Dequeue(ctx, 1)
	if err != nil || job1 != "job-high" {
		t.Fatalf("expected job-high, got %s (err: %v)", job1, err)
	}

	job2, err := qs.Dequeue(ctx, 1)
	if err != nil || job2 != "job-med" {
		t.Fatalf("expected job-med, got %s (err: %v)", job2, err)
	}

	job3, err := qs.Dequeue(ctx, 1)
	if err != nil || job3 != "job-low" {
		t.Fatalf("expected job-low, got %s (err: %v)", job3, err)
	}

	// Queue is now empty, next dequeue should return "" within timeout
	emptyJob, err := qs.Dequeue(ctx, 1)
	if err != nil || emptyJob != "" {
		t.Fatalf("expected empty dequeue, got %s (err: %v)", emptyJob, err)
	}
}

func TestDelayedJobPromotion(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close()

	qs := NewQueueService(rdb)
	ctx := context.Background()

	now := time.Now().UnixMilli()

	// Enqueue a job delayed by 1000ms
	if err := qs.EnqueueDelayed(ctx, "delayed-1", 1000); err != nil {
		t.Fatalf("failed to enqueue delayed: %v", err)
	}

	// Attempt promotion before due -> should return false
	promoted, err := qs.PromoteDueJob(ctx, "delayed-1", 10, now+500)
	if err != nil {
		t.Fatalf("unexpected error promoting: %v", err)
	}
	if promoted {
		t.Fatal("expected job not to be promoted yet")
	}

	// Attempt promotion after due -> should return true and be in high queue
	promoted, err = qs.PromoteDueJob(ctx, "delayed-1", 10, now+1500)
	if err != nil {
		t.Fatalf("unexpected error promoting due job: %v", err)
	}
	if !promoted {
		t.Fatal("expected job to be promoted")
	}

	// Verify job is now in QueueHigh
	dequeued, err := qs.Dequeue(ctx, 1)
	if err != nil || dequeued != "delayed-1" {
		t.Fatalf("expected to dequeue delayed-1 from high queue, got %s", dequeued)
	}
}

