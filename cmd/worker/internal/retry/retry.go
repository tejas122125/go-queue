package retry

import (
	"context"
	"math"
	"time"

	"queue-forge/internal/redisqueue"
)

// CalculateDelay calculates exponential backoff delay matching TS BackoffStrategy:
// min(5000 * 2^attempts, 60000) ms
func CalculateDelay(attempts int) time.Duration {
	delayMs := math.Min(5000.0*math.Pow(2, float64(attempts)), 60000.0)
	return time.Duration(delayMs) * time.Millisecond
}

// RetryService enqueues failed jobs into the delayed queue with exponential backoff.
type RetryService struct {
	queue *redisqueue.QueueService
}

// New constructs a RetryService.
func New(queue *redisqueue.QueueService) *RetryService {
	return &RetryService{queue: queue}
}

// Retry schedules a job for retry after calculated backoff delay.
func (r *RetryService) Retry(ctx context.Context, jobID string, attempts int) error {
	delay := CalculateDelay(attempts)
	return r.queue.EnqueueDelayed(ctx, jobID, delay.Milliseconds())
}

