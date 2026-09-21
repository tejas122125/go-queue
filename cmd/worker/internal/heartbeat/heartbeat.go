package heartbeat

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	dbgen "queue-forge/internal/db/gen"
)

// HeartbeatService sends periodic heartbeats to update worker last_heartbeat.
type HeartbeatService struct {
	queries *dbgen.Queries
	log     zerolog.Logger
	cancel  context.CancelFunc
}

// New constructs a HeartbeatService.
func New(pool *pgxpool.Pool, log zerolog.Logger) *HeartbeatService {
	return &HeartbeatService{
		queries: dbgen.New(pool),
		log:     log.With().Str("component", "heartbeat").Logger(),
	}
}

// Start launches a background goroutine ticking every 5 seconds.
func (h *HeartbeatService) Start(parentCtx context.Context, workerID uuid.UUID) {
	ctx, cancel := context.WithCancel(parentCtx)
	h.cancel = cancel

	ticker := time.NewTicker(5 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := h.queries.UpdateWorkerHeartbeat(ctx, pgtype.UUID{Bytes: workerID, Valid: true}); err != nil {
					h.log.Warn().Err(err).Str("workerId", workerID.String()).Msg("Failed to update worker heartbeat")
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop terminates the background heartbeat ticker.
func (h *HeartbeatService) Stop() {
	if h.cancel != nil {
		h.cancel()
	}
}

