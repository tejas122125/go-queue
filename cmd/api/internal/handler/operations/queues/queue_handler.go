package queues

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"queue-forge/cmd/api/internal/apierr"
	"queue-forge/cmd/api/internal/handler"
	dbgen "queue-forge/internal/db/gen"
	"queue-forge/internal/redisqueue"
)

var validQueueNames = []string{"high", "medium", "low"}

type QueueSummary struct {
	Name                    string         `json:"name"`
	AuthoritativeDepth      int64          `json:"authoritativeDepth"`
	ExecutionDepth          *int64         `json:"executionDepth"`
	OldestPendingAt         *string        `json:"oldestPendingAt"`
	StatusCounts            map[string]int `json:"statusCounts"`
	ExecutionLayerAvailable bool           `json:"executionLayerAvailable"`
	State                   string         `json:"state"`
	PausedAt                *string        `json:"pausedAt"`
	StateReason             *string        `json:"stateReason"`
}

type QueueStateChangeRequest struct {
	Reason string `json:"reason"`
}

type QueueStateChangeResponse struct {
	Name     string  `json:"name"`
	State    string  `json:"state"`
	Reason   string  `json:"reason"`
	PausedAt *string `json:"pausedAt"`
}

type QueueHandler struct {
	queries *dbgen.Queries
	queue   *redisqueue.QueueService
	log     zerolog.Logger
}

func New(pool *pgxpool.Pool, queue *redisqueue.QueueService, log zerolog.Logger) *QueueHandler {
	return &QueueHandler{
		queries: dbgen.New(pool),
		queue:   queue,
		log:     log.With().Str("component", "queue_handler").Logger(),
	}
}

func (h *QueueHandler) List(w http.ResponseWriter, r *http.Request) {
	summaries, err := h.getSummaries(r.Context())
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	handler.WriteJSON(w, http.StatusOK, summaries)
}

func (h *QueueHandler) Get(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if !isValidQueue(name) {
		handler.WriteError(w, apierr.ResourceNotFound("Queue", name))
		return
	}

	summaries, err := h.getSummaries(r.Context())
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	for _, s := range summaries {
		if s.Name == name {
			handler.WriteJSON(w, http.StatusOK, s)
			return
		}
	}

	handler.WriteError(w, apierr.ResourceNotFound("Queue", name))
}

func (h *QueueHandler) Pause(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if !isValidQueue(name) {
		handler.WriteError(w, apierr.ResourceNotFound("Queue", name))
		return
	}

	var req QueueStateChangeRequest
	_ = json.NewDecoder(r.Body).Decode(&req)

	currentState, err := h.queries.GetQueueState(r.Context(), name)
	if err == nil && currentState.State == "PAUSED" {
		handler.WriteError(w, apierr.Conflict(fmt.Sprintf("Queue '%s' is already paused", name)))
		return
	}

	now := time.Now()
	res, err := h.queries.SetQueueState(r.Context(), dbgen.SetQueueStateParams{
		Name:        name,
		State:       "PAUSED",
		PausedAt:    pgtype.Timestamp{Time: now, Valid: true},
		StateReason: pgtype.Text{String: req.Reason, Valid: req.Reason != ""},
	})
	if err != nil {
		handler.WriteError(w, apierr.ResourceNotFound("Queue", name))
		return
	}

	pausedAtStr := now.Format(time.RFC3339Nano)
	if res.PausedAt.Valid {
		pausedAtStr = res.PausedAt.Time.Format(time.RFC3339Nano)
	}

	handler.WriteJSON(w, http.StatusOK, QueueStateChangeResponse{
		Name:     name,
		State:    "PAUSED",
		Reason:   req.Reason,
		PausedAt: &pausedAtStr,
	})
}

func (h *QueueHandler) Resume(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if !isValidQueue(name) {
		handler.WriteError(w, apierr.ResourceNotFound("Queue", name))
		return
	}

	var req QueueStateChangeRequest
	_ = json.NewDecoder(r.Body).Decode(&req)

	currentState, err := h.queries.GetQueueState(r.Context(), name)
	if err == nil && currentState.State == "ACTIVE" {
		handler.WriteError(w, apierr.Conflict(fmt.Sprintf("Queue '%s' is already active", name)))
		return
	}

	_, err = h.queries.SetQueueState(r.Context(), dbgen.SetQueueStateParams{
		Name:        name,
		State:       "ACTIVE",
		PausedAt:    pgtype.Timestamp{Valid: false},
		StateReason: pgtype.Text{String: req.Reason, Valid: req.Reason != ""},
	})
	if err != nil {
		handler.WriteError(w, apierr.ResourceNotFound("Queue", name))
		return
	}

	handler.WriteJSON(w, http.StatusOK, QueueStateChangeResponse{
		Name:     name,
		State:    "ACTIVE",
		Reason:   req.Reason,
		PausedAt: nil,
	})
}

func (h *QueueHandler) getSummaries(ctx context.Context) ([]QueueSummary, error) {
	summaries := make(map[string]*QueueSummary)
	for _, name := range validQueueNames {
		summaries[name] = &QueueSummary{
			Name:                    name,
			AuthoritativeDepth:      0,
			ExecutionDepth:          nil,
			OldestPendingAt:         nil,
			StatusCounts:            make(map[string]int),
			ExecutionLayerAvailable: false,
			State:                   "ACTIVE",
			PausedAt:                nil,
			StateReason:             nil,
		}
	}

	// 1. Group counts
	groups, err := h.queries.GetJobGroups(ctx)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}

	for _, g := range groups {
		qName := resolveQueueName(int(g.Priority))
		s := summaries[qName]
		s.StatusCounts[g.Status] = s.StatusCounts[g.Status] + int(g.Count)

		if g.Status == "PENDING" || g.Status == "RETRYING" {
			s.AuthoritativeDepth += g.Count
			if g.OldestCreatedAt.Valid {
				tStr := g.OldestCreatedAt.Time.Format(time.RFC3339Nano)
				if s.OldestPendingAt == nil || g.OldestCreatedAt.Time.Before(parseTimeSafe(*s.OldestPendingAt)) {
					s.OldestPendingAt = &tStr
				}
			}
		}
	}

	// 2. Queue states and Redis depths
	result := make([]QueueSummary, 0, len(validQueueNames))
	for _, name := range validQueueNames {
		s := summaries[name]

		state, err := h.queries.GetQueueState(ctx, name)
		if err == nil {
			s.State = state.State
			if state.PausedAt.Valid {
				tStr := state.PausedAt.Time.Format(time.RFC3339Nano)
				s.PausedAt = &tStr
			}
			if state.StateReason.Valid {
				s.StateReason = &state.StateReason.String
			}
		}

		// Read Redis depth
		redisQueueName := "queue:" + name
		depth, err := h.queue.GetQueueDepth(ctx, redisQueueName)
		if err == nil {
			s.ExecutionDepth = &depth
			s.ExecutionLayerAvailable = true
		} else {
			h.log.Warn().Err(err).Str("queueName", name).Msg("Unable to read Redis queue depth")
			s.ExecutionDepth = nil
			s.ExecutionLayerAvailable = false
		}

		result = append(result, *s)
	}

	return result, nil
}

func isValidQueue(name string) bool {
	for _, v := range validQueueNames {
		if v == name {
			return true
		}
	}
	return false
}

func resolveQueueName(priority int) string {
	if priority >= 10 {
		return "high"
	}
	if priority >= 5 {
		return "medium"
	}
	return "low"
}

func parseTimeSafe(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}

