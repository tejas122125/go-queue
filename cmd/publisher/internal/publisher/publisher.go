package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	dbgen "queue-forge/internal/db/gen"
	"queue-forge/internal/metrics"
	"queue-forge/internal/redisqueue"
)

// DispatchPayload is the expected structure inside an outbox event's payload.
type DispatchPayload struct {
	JobID    string `json:"jobId"`
	Priority int    `json:"priority"`
	Name     string `json:"name,omitempty"`
}

// Config holds publisher configuration.
type Config struct {
	WorkerID       string
	BatchSize      int
	PollIntervalMs int
	MaxRetries     int
	ClaimTTLMs     int
}

// DefaultConfig returns a Config with default values matching the TS defaults.
func DefaultConfig(workerID string) Config {
	return Config{
		WorkerID:       workerID,
		BatchSize:      50,
		PollIntervalMs: 100,
		MaxRetries:     5,
		ClaimTTLMs:     30_000,
	}
}

// OutboxPublisher polls the outbox_events table and publishes events to Redis queues.
type OutboxPublisher struct {
	pool    *pgxpool.Pool
	queries *dbgen.Queries
	queue   *redisqueue.QueueService
	config  Config
	log     zerolog.Logger
}

// New creates a new OutboxPublisher.
func New(pool *pgxpool.Pool, queue *redisqueue.QueueService, config Config, log zerolog.Logger) *OutboxPublisher {
	return &OutboxPublisher{
		pool:    pool,
		queries: dbgen.New(pool),
		queue:   queue,
		config:  config,
		log:     log.With().Str("component", "publisher").Logger(),
	}
}

// Start runs the publisher loop until ctx is cancelled.
func (p *OutboxPublisher) Start(ctx context.Context) error {
	p.log.Info().Str("workerId", p.config.WorkerID).Msg("Outbox publisher started")

	pollInterval := time.Duration(p.config.PollIntervalMs) * time.Millisecond

	for {
		select {
		case <-ctx.Done():
			p.log.Info().Str("workerId", p.config.WorkerID).Msg("Outbox publisher stopped")
			return nil
		default:
		}

		if err := p.processBatch(ctx); err != nil {
			p.log.Error().Err(err).Msg("batch processing failed")
		}

		p.updateLagMetrics(ctx)

		select {
		case <-time.After(pollInterval):
		case <-ctx.Done():
			p.log.Info().Str("workerId", p.config.WorkerID).Msg("Outbox publisher stopped")
			return nil
		}
	}
}

// processBatch claims and processes a batch of outbox events.
func (p *OutboxPublisher) processBatch(ctx context.Context) error {
	claimExpiry := time.Now().Add(time.Duration(p.config.ClaimTTLMs) * time.Millisecond)

	events, err := p.queries.ClaimOutboxBatch(ctx, dbgen.ClaimOutboxBatchParams{
		ClaimedBy: pgtype.Text{String: p.config.WorkerID, Valid: true},
		Limit:     int32(p.config.BatchSize),
		ClaimExpiresAt: pgtype.Timestamp{
			Time:  claimExpiry,
			Valid: true,
		},
	})
	if err != nil {
		return fmt.Errorf("claiming outbox batch: %w", err)
	}

	for _, event := range events {
		p.processEvent(ctx, event)
	}

	return nil
}

// processEvent publishes a single outbox event to Redis.
func (p *OutboxPublisher) processEvent(ctx context.Context, event dbgen.OutboxEvent) {
	timer := prometheus.NewTimer(metrics.OutboxPublishDuration)
	defer timer.ObserveDuration()

	if event.Attempts >= int32(p.config.MaxRetries) {
		p.markFailed(ctx, event, fmt.Sprintf("Outbox event exceeded max retries (%d)", p.config.MaxRetries))
		return
	}

	if err := p.publishEvent(ctx, event); err != nil {
		p.markFailed(ctx, event, err.Error())
		return
	}

	if err := p.queries.MarkOutboxProcessed(ctx, event.ID); err != nil {
		p.log.Error().Err(err).Str("eventId", uuidToString(event.ID)).Msg("Failed to mark outbox event processed")
		return
	}

	metrics.OutboxEventsPublished.Inc()
	p.log.Info().
		Str("eventId", uuidToString(event.ID)).
		Str("eventType", event.EventType).
		Msg("Outbox event published")
}

// publishEvent validates the event type and enqueues the job into Redis.
func (p *OutboxPublisher) publishEvent(ctx context.Context, event dbgen.OutboxEvent) error {
	if event.EventType != "job.dispatch.requested" {
		return fmt.Errorf("unsupported outbox event type: %s", event.EventType)
	}

	payload, err := parseDispatchPayload(event.Payload)
	if err != nil {
		return fmt.Errorf("parsing outbox payload: %w", err)
	}

	if err := p.queue.Enqueue(ctx, payload.JobID, payload.Priority); err != nil {
		return fmt.Errorf("enqueuing job to redis: %w", err)
	}

	return nil
}

// markFailed marks an outbox event as failed with an error message.
func (p *OutboxPublisher) markFailed(ctx context.Context, event dbgen.OutboxEvent, errMsg string) {
	metrics.OutboxEventsFailed.Inc()

	truncated := errMsg
	if len(truncated) > 1000 {
		truncated = truncated[:1000]
	}

	if err := p.queries.MarkOutboxFailed(ctx, dbgen.MarkOutboxFailedParams{
		ID:        event.ID,
		LastError: pgtype.Text{String: truncated, Valid: true},
	}); err != nil {
		p.log.Error().Err(err).Str("eventId", uuidToString(event.ID)).Msg("Failed to mark outbox event as failed")
	}

	p.log.Error().
		Str("eventId", uuidToString(event.ID)).
		Str("error", errMsg).
		Msg("Outbox event publish failed")
}

// updateLagMetrics queries the outbox lag and updates Prometheus gauges.
func (p *OutboxPublisher) updateLagMetrics(ctx context.Context) {
	lag, err := p.queries.GetOutboxLag(ctx)
	if err != nil {
		p.log.Warn().Err(err).Msg("Failed to query outbox lag metrics")
		return
	}

	var lagSeconds float64
	if lag.OldestCreatedAt.Valid {
		lagSeconds = float64(time.Since(lag.OldestCreatedAt.Time).Milliseconds()) / 1000.0
		if lagSeconds < 0 {
			lagSeconds = 0
		}
	}

	metrics.OutboxLagSeconds.Set(lagSeconds)
	metrics.OutboxPendingEvents.Set(float64(lag.Count))
}

// parseDispatchPayload parses the JSON payload of an outbox event.
func parseDispatchPayload(raw []byte) (*DispatchPayload, error) {
	var payload DispatchPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("outbox event payload must be valid JSON: %w", err)
	}
	if payload.JobID == "" {
		return nil, fmt.Errorf("outbox event payload is missing jobId")
	}
	return &payload, nil
}

// uuidToString converts a pgtype.UUID to its string representation.
func uuidToString(u pgtype.UUID) string {
	if !u.Valid {
		return ""
	}
	b := u.Bytes
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
