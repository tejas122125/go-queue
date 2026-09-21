package promoter

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	dbgen "queue-forge/internal/db/gen"
	"queue-forge/internal/metrics"
	"queue-forge/internal/redisqueue"
)

// DelayedJobPromoter polls the Redis delayed sorted set and promotes due jobs
// to the appropriate priority queue using an atomic Lua script.
type DelayedJobPromoter struct {
	rdb     *redis.Client
	queries *dbgen.Queries
	queue   *redisqueue.QueueService
	log     zerolog.Logger
}

// New creates a new DelayedJobPromoter.
func New(rdb *redis.Client, pool *pgxpool.Pool, log zerolog.Logger) *DelayedJobPromoter {
	return &DelayedJobPromoter{
		rdb:     rdb,
		queries: dbgen.New(pool),
		queue:   redisqueue.NewQueueService(rdb),
		log:     log.With().Str("component", "promoter").Logger(),
	}
}

// PromoteDueJobs finds all jobs whose delay has elapsed and atomically moves them
// from the delayed sorted set into their priority queue.
func (p *DelayedJobPromoter) PromoteDueJobs(ctx context.Context) (int, error) {
	now := time.Now().UnixMilli()
	nowStr := strconv.FormatInt(now, 10)

	dueJobIDs, err := p.rdb.ZRangeByScore(ctx, redisqueue.QueueDelayed, &redis.ZRangeBy{
		Min: "-inf",
		Max: nowStr,
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("querying delayed jobs: %w", err)
	}

	promoted := 0
	for _, jobID := range dueJobIDs {
		priority, err := p.findJobPriority(ctx, jobID)
		if err != nil {
			p.log.Warn().Err(err).Str("jobId", jobID).Msg("Failed to find job priority, skipping")
			continue
		}
		if priority == nil {
			continue
		}

		moved, err := p.queue.PromoteDueJob(ctx, jobID, *priority, now)
		if err != nil {
			p.log.Warn().Err(err).Str("jobId", jobID).Msg("Failed to promote delayed job")
			continue
		}

		if moved {
			promoted++
		}
	}

	if promoted > 0 {
		metrics.DelayedJobsPromoted.Add(float64(promoted))
	}

	return promoted, nil
}

// findJobPriority looks up a job's priority from Postgres.
func (p *DelayedJobPromoter) findJobPriority(ctx context.Context, jobID string) (*int, error) {
	parsedUUID, err := uuid.Parse(jobID)
	if err != nil {
		return nil, fmt.Errorf("parsing job id %q: %w", jobID, err)
	}

	uid := pgtype.UUID{Bytes: parsedUUID, Valid: true}
	row, err := p.queries.FindJobPriority(ctx, uid)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	priority := int(row)
	return &priority, nil
}
