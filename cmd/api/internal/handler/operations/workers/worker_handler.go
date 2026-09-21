package workers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"queue-forge/cmd/api/internal/apierr"
	"queue-forge/cmd/api/internal/handler"
	dbgen "queue-forge/internal/db/gen"
)

type WorkerSummary struct {
	ID             string  `json:"id"`
	Hostname       string  `json:"hostname"`
	Status         string  `json:"status"`
	Health         string  `json:"health"`
	LastHeartbeat  *string `json:"lastHeartbeat"`
	HeartbeatAgeMs *int64  `json:"heartbeatAgeMs"`
	CreatedAt      string  `json:"createdAt"`
}

type WorkerCursor struct {
	CreatedAt string `json:"createdAt"`
	ID        string `json:"id"`
}

type WorkerPage struct {
	Items      []WorkerSummary `json:"items"`
	NextCursor *string         `json:"nextCursor"`
}

type WorkerHandler struct {
	queries        *dbgen.Queries
	staleAfterTime time.Duration
}

func New(pool *pgxpool.Pool, staleAfterMs int) *WorkerHandler {
	if staleAfterMs <= 0 {
		staleAfterMs = 15_000
	}
	return &WorkerHandler{
		queries:        dbgen.New(pool),
		staleAfterTime: time.Duration(staleAfterMs) * time.Millisecond,
	}
}

func (h *WorkerHandler) List(w http.ResponseWriter, r *http.Request) {
	healthFilter := r.URL.Query().Get("health")
	limit := 20
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}

	cursorStr := r.URL.Query().Get("cursor")
	var cursor *WorkerCursor
	if cursorStr != "" {
		var c WorkerCursor
		if err := handler.DecodeCursor(cursorStr, &c); err != nil {
			handler.WriteError(w, apierr.InvalidCursor())
			return
		}
		cursor = &c
	}

	workers, err := h.queries.ListWorkers(r.Context())
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	var filtered []WorkerSummary
	now := time.Now()

	for _, worker := range workers {
		summary := h.toSummary(worker, now)

		if healthFilter != "" && summary.Health != healthFilter {
			continue
		}

		// Cursor evaluation: created_at < cursor.createdAt OR (created_at == cursor.createdAt AND id < cursor.id)
		if cursor != nil {
			cursorTime, err := time.Parse(time.RFC3339Nano, cursor.CreatedAt)
			if err != nil {
				cursorTime, _ = time.Parse(time.RFC3339, cursor.CreatedAt)
			}
			workerTime := worker.CreatedAt.Time
			workerID := uuidToString(worker.ID)

			if workerTime.After(cursorTime) {
				continue
			}
			if workerTime.Equal(cursorTime) && workerID >= cursor.ID {
				continue
			}
		}

		filtered = append(filtered, summary)
	}

	hasMore := len(filtered) > limit
	items := filtered
	if hasMore {
		items = filtered[:limit]
	}

	var nextCursor *string
	if hasMore && len(items) > 0 {
		last := items[len(items)-1]
		encoded, err := handler.EncodeCursor(WorkerCursor{
			CreatedAt: last.CreatedAt,
			ID:        last.ID,
		})
		if err == nil {
			nextCursor = &encoded
		}
	}

	handler.WriteJSON(w, http.StatusOK, WorkerPage{
		Items:      items,
		NextCursor: nextCursor,
	})
}

func (h *WorkerHandler) Get(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	parsedUUID, err := uuid.Parse(idStr)
	if err != nil {
		handler.WriteError(w, apierr.ResourceNotFound("Worker", idStr))
		return
	}

	worker, err := h.queries.FindWorkerByID(r.Context(), pgtype.UUID{Bytes: parsedUUID, Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			handler.WriteError(w, apierr.ResourceNotFound("Worker", idStr))
			return
		}
		handler.WriteError(w, err)
		return
	}

	handler.WriteJSON(w, http.StatusOK, h.toSummary(worker, time.Now()))
}

func (h *WorkerHandler) toSummary(w dbgen.Worker, now time.Time) WorkerSummary {
	var lastHeartbeatStr *string
	var heartbeatAgeMs *int64

	if w.LastHeartbeat.Valid {
		s := w.LastHeartbeat.Time.Format(time.RFC3339Nano)
		lastHeartbeatStr = &s
		age := now.Sub(w.LastHeartbeat.Time).Milliseconds()
		if age < 0 {
			age = 0
		}
		heartbeatAgeMs = &age
	}

	health := "HEALTHY"
	if w.Status != "ACTIVE" {
		health = "OFFLINE"
	} else if heartbeatAgeMs == nil || time.Duration(*heartbeatAgeMs)*time.Millisecond > h.staleAfterTime {
		health = "STALE"
	}

	return WorkerSummary{
		ID:             uuidToString(w.ID),
		Hostname:       w.Hostname,
		Status:         w.Status,
		Health:         health,
		LastHeartbeat:  lastHeartbeatStr,
		HeartbeatAgeMs: heartbeatAgeMs,
		CreatedAt:      w.CreatedAt.Time.Format(time.RFC3339Nano),
	}
}

func uuidToString(u pgtype.UUID) string {
	if !u.Valid {
		return ""
	}
	b := u.Bytes
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func init() {
	_ = json.Marshal
}

