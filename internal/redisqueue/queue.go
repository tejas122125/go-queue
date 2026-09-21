package redisqueue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// MoveDueJobScript atomically verifies a job's delayed score and moves it to the target queue.
var MoveDueJobScript = redis.NewScript(`
local score = redis.call("ZSCORE", KEYS[1], ARGV[1])
if not score then
  return 0
end
if tonumber(score) > tonumber(ARGV[2]) then
  return 0
end
redis.call("LPUSH", KEYS[2], ARGV[1])
redis.call("ZREM", KEYS[1], ARGV[1])
return 1
`)

// QueueService provides Redis queue primitives matching TS QueueService.
type QueueService struct {
	client *redis.Client
}

// NewQueueService constructs a QueueService.
func NewQueueService(client *redis.Client) *QueueService {
	return &QueueService{client: client}
}

// GetQueueDepth returns the length of the specified list.
func (s *QueueService) GetQueueDepth(ctx context.Context, queueName string) (int64, error) {
	return s.client.LLen(ctx, queueName).Result()
}

// Enqueue pushes a job ID to the head of the priority list corresponding to the given priority.
func (s *QueueService) Enqueue(ctx context.Context, jobID string, priority int) error {
	queue := ResolveQueue(priority)
	return s.client.LPush(ctx, queue, jobID).Err()
}

// EnqueueDelayed adds a job ID to the delayed sorted set with score = now + delayMs.
func (s *QueueService) EnqueueDelayed(ctx context.Context, jobID string, delayMs int64) error {
	score := float64(time.Now().UnixMilli() + delayMs)
	return s.client.ZAdd(ctx, QueueDelayed, redis.Z{
		Score:  score,
		Member: jobID,
	}).Err()
}

// Dequeue pops the next job ID in priority order (high -> medium -> low) with a blocking timeout.
// A timeoutSec of 0 blocks indefinitely (matching redis brpop semantics), or positive timeout.
func (s *QueueService) Dequeue(ctx context.Context, timeoutSec int) (string, error) {
	timeout := time.Duration(timeoutSec) * time.Second

	result, err := s.client.BRPop(ctx, timeout, QueueHigh, QueueMedium, QueueLow).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		return "", err
	}

	// BRPop returns a slice of [queueName, element]
	if len(result) >= 2 {
		return result[1], nil
	}

	return "", nil
}

// PromoteDueJob executes the atomic Lua script to promote a delayed job if due.
func (s *QueueService) PromoteDueJob(ctx context.Context, jobID string, priority int, nowEpochMs int64) (bool, error) {
	targetQueue := ResolveQueue(priority)
	res, err := MoveDueJobScript.Run(ctx, s.client, []string{QueueDelayed, targetQueue}, jobID, strconv.FormatInt(nowEpochMs, 10)).Int()
	if err != nil {
		return false, fmt.Errorf("running delayed promotion script: %w", err)
	}
	return res == 1, nil
}

