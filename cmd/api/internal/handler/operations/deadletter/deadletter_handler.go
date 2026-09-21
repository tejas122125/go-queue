package deadletter

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
	"queue-forge/cmd/api/internal/middleware"
	dbgen "queue-forge/internal/db/gen"
)

type DeadLetterRecord struct {
	ID        string  `json:"id"`
	JobID     string  `json:"jobId"`
	Reason    *string `json:"reason"`
	CreatedAt string  `json:"createdAt"`
}

type DeadLetterCursor struct {
	CreatedAt string `json:"createdAt"`
	ID        string `json:"id"`
}

type DeadLetterPage struct {
	Items      []DeadLetterRecord `json:"items"`
	NextCursor *string            `json:"nextCursor"`
}

type ReplayRequest struct {
	Reason string `json:"reason"`
}

type ReplayResponse struct {
	JobID  string `json:"jobId"`
	Status string `json:"status"`
}

type DeadLetterHandler struct {
	pool    *pgxpool.Pool
	queries *dbgen.Queries
}

func New(pool *pgxpool.Pool) *DeadLetterHandler {
	return &DeadLetterHandler{
		pool:    pool,
		queries: dbgen.New(pool),
	}
}

func (h *DeadLetterHandler) List(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}

	reasonCode := r.URL.Query().Get("reasonCode")
	// Note Gap 8.10: queueId filter is silently ignored per TS implementation

	cursorStr := r.URL.Query().Get("cursor")
	var cursor *DeadLetterCursor
	if cursorStr != "" {
		var c DeadLetterCursor
		if err := handler.DecodeCursor(cursorStr, &c); err != nil {
			handler.WriteError(w, apierr.InvalidCursor())
			return
		}
		cursor = &c
	}

	query := `SELECT id, job_id, reason, created_at FROM dead_letter_jobs WHERE 1=1`
	var args []interface{}
	argIdx := 1

	if reasonCode != "" {
		query += fmt.Sprintf(" AND reason = $%d", argIdx)
		args = append(args, reasonCode)
		argIdx++
	}

	if cursor != nil {
		cursorTime, err := time.Parse(time.RFC3339Nano, cursor.CreatedAt)
		if err != nil {
			cursorTime, _ = time.Parse(time.RFC3339, cursor.CreatedAt)
		}
		if cursorUUID, err := uuid.Parse(cursor.ID); err == nil {
			query += fmt.Sprintf(" AND (created_at < $%d OR (created_at = $%d AND id < $%d))", argIdx, argIdx, argIdx+1)
			args = append(args, cursorTime, cursorUUID)
			argIdx += 2
		}
	}

	query += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", argIdx)
	args = append(args, limit+1)

	rows, err := h.pool.Query(r.Context(), query, args...)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	defer rows.Close()

	var items []DeadLetterRecord
	for rows.Next() {
		var id pgtype.UUID
		var jobID pgtype.UUID
		var reason pgtype.Text
		var createdAt pgtype.Timestamp

		if err := rows.Scan(&id, &jobID, &reason, &createdAt); err != nil {
			handler.WriteError(w, err)
			return
		}

		var rStr *string
		if reason.Valid {
			rStr = &reason.String
		}

		items = append(items, DeadLetterRecord{
			ID:        uuidToString(id),
			JobID:     uuidToString(jobID),
			Reason:    rStr,
			CreatedAt: createdAt.Time.Format(time.RFC3339Nano),
		})
	}

	hasMore := len(items) > limit
	page := items
	if hasMore {
		page = items[:limit]
	}

	var nextCursor *string
	if hasMore && len(page) > 0 {
		last := page[len(page)-1]
		encoded, err := handler.EncodeCursor(DeadLetterCursor{
			CreatedAt: last.CreatedAt,
			ID:        last.ID,
		})
		if err == nil {
			nextCursor = &encoded
		}
	}

	handler.WriteJSON(w, http.StatusOK, DeadLetterPage{
		Items:      page,
		NextCursor: nextCursor,
	})
}

func (h *DeadLetterHandler) Get(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	parsedUUID, err := uuid.Parse(idStr)
	if err != nil {
		handler.WriteError(w, apierr.ResourceNotFound("DeadLetterJob", idStr))
		return
	}

	row, err := h.queries.FindDeadLetterByID(r.Context(), pgtype.UUID{Bytes: parsedUUID, Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			handler.WriteError(w, apierr.ResourceNotFound("DeadLetterJob", idStr))
			return
		}
		handler.WriteError(w, err)
		return
	}

	var rStr *string
	if row.Reason.Valid {
		rStr = &row.Reason.String
	}

	handler.WriteJSON(w, http.StatusOK, DeadLetterRecord{
		ID:        uuidToString(row.ID),
		JobID:     uuidToString(row.JobID),
		Reason:    rStr,
		CreatedAt: row.CreatedAt.Time.Format(time.RFC3339Nano),
	})
}

func (h *DeadLetterHandler) Replay(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	parsedUUID, err := uuid.Parse(idStr)
	if err != nil {
		handler.WriteError(w, apierr.ResourceNotFound("DeadLetterJob", idStr))
		return
	}

	var req ReplayRequest
	_ = json.NewDecoder(r.Body).Decode(&req)

	dlqID := pgtype.UUID{Bytes: parsedUUID, Valid: true}
	dlq, err := h.queries.FindDeadLetterByID(r.Context(), dlqID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			handler.WriteError(w, apierr.ResourceNotFound("DeadLetterJob", idStr))
			return
		}
		handler.WriteError(w, err)
		return
	}

	job, err := h.queries.FindJobByID(r.Context(), dlq.JobID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			handler.WriteError(w, apierr.ResourceNotFound("Job", uuidToString(dlq.JobID)))
			return
		}
		handler.WriteError(w, err)
		return
	}

	if job.Status != "DEAD_LETTER" {
		handler.WriteError(w, apierr.InvalidStateTransition(job.Status, "PENDING"))
		return
	}

	updated, err := h.queries.UpdateJobStatusWithVersion(r.Context(), dbgen.UpdateJobStatusWithVersionParams{
		ID:      job.ID,
		Status:  "PENDING",
		Version: job.Version,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			handler.WriteError(w, apierr.Conflict("Job was modified concurrently; retry the request"))
			return
		}
		handler.WriteError(w, err)
		return
	}

	_ = h.queries.DeleteDeadLetter(r.Context(), dlqID)

	actorID := middleware.GetActorID(r.Context())
	metaJSON, _ := json.Marshal(map[string]interface{}{
		"reason": req.Reason,
		"dlqId":  idStr,
	})
	_, _ = h.queries.CreateJobEvent(r.Context(), dbgen.CreateJobEventParams{
		JobID:      job.ID,
		EventType:  "job.dlq.replay",
		FromStatus: pgtype.Text{String: "DEAD_LETTER", Valid: true},
		ToStatus:   pgtype.Text{String: "PENDING", Valid: true},
		Version:    updated.Version,
		ActorType:  "operator",
		ActorID:    pgtype.Text{String: actorID, Valid: true},
		Metadata:   metaJSON,
	})

	// Preserves Gap 8.3: writeAuditLog is a no-op default in DeadLetterService

	handler.WriteJSON(w, http.StatusOK, ReplayResponse{
		JobID:  uuidToString(updated.ID),
		Status: updated.Status,
	})
}

func uuidToString(u pgtype.UUID) string {
	if !u.Valid {
		return ""
	}
	b := u.Bytes
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

