package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
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

var validTransitions = map[string][]string{
	"FAILED":      {"PENDING"},
	"DEAD_LETTER": {"PENDING"},
	"PENDING":     {"CANCELED"},
	"RETRYING":    {"CANCELED"},
	"RUNNING":     {"CANCELED"},
}

type JobSummary struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Priority    int32  `json:"priority"`
	Status      string `json:"status"`
	Attempts    int32  `json:"attempts"`
	MaxAttempts int32  `json:"maxAttempts"`
	CreatedAt   string `json:"createdAt"`
	UpdatedAt   string `json:"updatedAt"`
}

type JobRecord struct {
	ID                      string          `json:"id"`
	QueueID                 *string         `json:"queueId"`
	Name                    string          `json:"name"`
	Payload                 json.RawMessage `json:"payload"`
	Priority                int32           `json:"priority"`
	Status                  string          `json:"status"`
	Attempts                int32           `json:"attempts"`
	MaxAttempts             int32           `json:"maxAttempts"`
	Version                 int32           `json:"version"`
	RunAt                   *string         `json:"runAt"`
	CancellationRequestedAt *string         `json:"cancellationRequestedAt"`
	CompletedAt             *string         `json:"completedAt"`
	CreatedAt               string          `json:"createdAt"`
	UpdatedAt               string          `json:"updatedAt"`
}

type JobCursor struct {
	CreatedAt string `json:"createdAt"`
	ID        string `json:"id"`
}

type JobPage struct {
	Items      []JobSummary `json:"items"`
	NextCursor *string      `json:"nextCursor"`
}

type JobActionRequest struct {
	Reason string `json:"reason"`
}

type JobActionResponse struct {
	ID      string `json:"id"`
	Status  string `json:"status"`
	Version int32  `json:"version"`
}

type JobExecutionRecord struct {
	ID           string  `json:"id"`
	JobID        string  `json:"jobId"`
	StartedAt    string  `json:"startedAt"`
	FinishedAt   *string `json:"finishedAt"`
	Status       *string `json:"status"`
	ErrorMessage *string `json:"errorMessage"`
}

type JobEventRecord struct {
	ID         string                 `json:"id"`
	JobID      string                 `json:"jobId"`
	EventType  string                 `json:"eventType"`
	FromStatus *string                `json:"fromStatus"`
	ToStatus   *string                `json:"toStatus"`
	Version    int32                  `json:"version"`
	ActorType  string                 `json:"actorType"`
	ActorID    *string                `json:"actorId"`
	Metadata   map[string]interface{} `json:"metadata"`
	CreatedAt  string                 `json:"createdAt"`
}

type JobHandler struct {
	pool    *pgxpool.Pool
	queries *dbgen.Queries
}

func New(pool *pgxpool.Pool) *JobHandler {
	return &JobHandler{
		pool:    pool,
		queries: dbgen.New(pool),
	}
}

func (h *JobHandler) List(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}

	queueIDFilter := r.URL.Query().Get("queueId")
	statusFilter := r.URL.Query().Get("status")
	nameFilter := r.URL.Query().Get("name")
	createdFromStr := r.URL.Query().Get("createdFrom")
	createdToStr := r.URL.Query().Get("createdTo")

	cursorStr := r.URL.Query().Get("cursor")
	var cursor *JobCursor
	if cursorStr != "" {
		var c JobCursor
		if err := handler.DecodeCursor(cursorStr, &c); err != nil {
			handler.WriteError(w, apierr.InvalidCursor())
			return
		}
		cursor = &c
	}

	query := `SELECT id, name, priority, status, attempts, max_attempts, created_at, updated_at FROM jobs WHERE 1=1`
	var args []interface{}
	argIdx := 1

	if queueIDFilter != "" {
		if qUUID, err := uuid.Parse(queueIDFilter); err == nil {
			query += fmt.Sprintf(" AND queue_id = $%d", argIdx)
			args = append(args, qUUID)
			argIdx++
		}
	}
	if statusFilter != "" {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, statusFilter)
		argIdx++
	}
	if nameFilter != "" {
		query += fmt.Sprintf(" AND name ILIKE $%d", argIdx)
		args = append(args, "%"+nameFilter+"%")
		argIdx++
	}
	if createdFromStr != "" {
		if t, err := time.Parse(time.RFC3339, createdFromStr); err == nil {
			query += fmt.Sprintf(" AND created_at >= $%d", argIdx)
			args = append(args, t)
			argIdx++
		}
	}
	if createdToStr != "" {
		if t, err := time.Parse(time.RFC3339, createdToStr); err == nil {
			query += fmt.Sprintf(" AND created_at <= $%d", argIdx)
			args = append(args, t)
			argIdx++
		}
	}

	if cursor != nil {
		cursorTime, err := time.Parse(time.RFC3339Nano, cursor.CreatedAt)
		if err != nil {
			cursorTime, _ = time.Parse(time.RFC3339, cursor.CreatedAt)
		}
		if cursorUUID, err := uuid.Parse(cursor.ID); err == nil {
			query += fmt.Sprintf(" AND (updated_at < $%d OR (updated_at = $%d AND id < $%d))", argIdx, argIdx, argIdx+1)
			args = append(args, cursorTime, cursorUUID)
			argIdx += 2
		}
	}

	query += fmt.Sprintf(" ORDER BY updated_at DESC, id DESC LIMIT $%d", argIdx)
	args = append(args, limit+1)

	rows, err := h.pool.Query(r.Context(), query, args...)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	defer rows.Close()

	var items []JobSummary
	for rows.Next() {
		var id pgtype.UUID
		var name string
		var priority int32
		var status string
		var attempts int32
		var maxAttempts int32
		var createdAt pgtype.Timestamp
		var updatedAt pgtype.Timestamp

		if err := rows.Scan(&id, &name, &priority, &status, &attempts, &maxAttempts, &createdAt, &updatedAt); err != nil {
			handler.WriteError(w, err)
			return
		}

		items = append(items, JobSummary{
			ID:          uuidToString(id),
			Name:        name,
			Priority:    priority,
			Status:      status,
			Attempts:    attempts,
			MaxAttempts: maxAttempts,
			CreatedAt:   createdAt.Time.Format(time.RFC3339Nano),
			UpdatedAt:   updatedAt.Time.Format(time.RFC3339Nano),
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
		encoded, err := handler.EncodeCursor(JobCursor{
			CreatedAt: last.UpdatedAt,
			ID:        last.ID,
		})
		if err == nil {
			nextCursor = &encoded
		}
	}

	handler.WriteJSON(w, http.StatusOK, JobPage{
		Items:      page,
		NextCursor: nextCursor,
	})
}

func (h *JobHandler) Get(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	parsedUUID, err := uuid.Parse(idStr)
	if err != nil {
		handler.WriteError(w, apierr.ResourceNotFound("Job", idStr))
		return
	}

	row, err := h.queries.FindJobByID(r.Context(), pgtype.UUID{Bytes: parsedUUID, Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			handler.WriteError(w, apierr.ResourceNotFound("Job", idStr))
			return
		}
		handler.WriteError(w, err)
		return
	}

	handler.WriteJSON(w, http.StatusOK, toJobRecord(row))
}

func (h *JobHandler) GetExecutions(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	parsedUUID, err := uuid.Parse(idStr)
	if err != nil {
		handler.WriteError(w, apierr.ResourceNotFound("Job", idStr))
		return
	}

	// Preserves Gap 8.5: job_executions are never written, always return empty array []
	rows, err := h.queries.GetJobExecutionsByJobID(r.Context(), pgtype.UUID{Bytes: parsedUUID, Valid: true})
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		handler.WriteError(w, err)
		return
	}

	records := make([]JobExecutionRecord, 0)
	for _, row := range rows {
		records = append(records, JobExecutionRecord{
			ID:           uuidToString(row.ID),
			JobID:        uuidToString(row.JobID),
			StartedAt:    row.StartedAt.Time.Format(time.RFC3339Nano),
			FinishedAt:   formatTimestampPtr(row.FinishedAt),
			Status:       formatTextPtr(row.Status),
			ErrorMessage: formatTextPtr(row.ErrorMessage),
		})
	}

	handler.WriteJSON(w, http.StatusOK, records)
}

func (h *JobHandler) GetEvents(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	parsedUUID, err := uuid.Parse(idStr)
	if err != nil {
		handler.WriteError(w, apierr.ResourceNotFound("Job", idStr))
		return
	}

	rows, err := h.queries.GetJobEventsByJobID(r.Context(), pgtype.UUID{Bytes: parsedUUID, Valid: true})
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		handler.WriteError(w, err)
		return
	}

	records := make([]JobEventRecord, 0)
	for _, row := range rows {
		var meta map[string]interface{}
		_ = json.Unmarshal(row.Metadata, &meta)

		records = append(records, JobEventRecord{
			ID:         uuidToString(row.ID),
			JobID:      uuidToString(row.JobID),
			EventType:  row.EventType,
			FromStatus: formatTextPtr(row.FromStatus),
			ToStatus:   formatTextPtr(row.ToStatus),
			Version:    row.Version,
			ActorType:  row.ActorType,
			ActorID:    formatTextPtr(row.ActorID),
			Metadata:   meta,
			CreatedAt:  row.CreatedAt.Time.Format(time.RFC3339Nano),
		})
	}

	handler.WriteJSON(w, http.StatusOK, records)
}

func (h *JobHandler) Retry(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	parsedUUID, err := uuid.Parse(idStr)
	if err != nil {
		handler.WriteError(w, apierr.ResourceNotFound("Job", idStr))
		return
	}

	var req JobActionRequest
	_ = json.NewDecoder(r.Body).Decode(&req)

	actorID := middleware.GetActorID(r.Context())
	requestID := middleware.GetRequestID(r.Context())

	res, err := h.transitionJob(r.Context(), parsedUUID, "PENDING", req.Reason, "job.retry.requested", "job.retry", actorID, requestID)
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	handler.WriteJSON(w, http.StatusOK, res)
}

func (h *JobHandler) Cancel(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	parsedUUID, err := uuid.Parse(idStr)
	if err != nil {
		handler.WriteError(w, apierr.ResourceNotFound("Job", idStr))
		return
	}

	var req JobActionRequest
	_ = json.NewDecoder(r.Body).Decode(&req)

	actorID := middleware.GetActorID(r.Context())
	requestID := middleware.GetRequestID(r.Context())

	res, err := h.transitionJob(r.Context(), parsedUUID, "CANCELED", req.Reason, "job.canceled", "job.cancel", actorID, requestID)
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	handler.WriteJSON(w, http.StatusOK, res)
}

func (h *JobHandler) transitionJob(
	ctx context.Context,
	jobID uuid.UUID,
	targetStatus string,
	reason string,
	eventType string,
	auditAction string,
	actorID string,
	requestID string,
) (*JobActionResponse, error) {
	uid := pgtype.UUID{Bytes: jobID, Valid: true}

	job, err := h.queries.FindJobByID(ctx, uid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, apierr.ResourceNotFound("Job", jobID.String())
		}
		return nil, err
	}

	allowed, ok := validTransitions[job.Status]
	if !ok || !containsString(allowed, targetStatus) {
		return nil, apierr.InvalidStateTransition(job.Status, targetStatus)
	}

	updated, err := h.queries.UpdateJobStatusWithVersion(ctx, dbgen.UpdateJobStatusWithVersionParams{
		ID:      uid,
		Status:  targetStatus,
		Version: job.Version,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, apierr.Conflict("Job was modified concurrently; retry the request")
		}
		return nil, err
	}

	metaJSON, _ := json.Marshal(map[string]interface{}{"reason": reason})
	_, _ = h.queries.CreateJobEvent(ctx, dbgen.CreateJobEventParams{
		JobID:      uid,
		EventType:  eventType,
		FromStatus: pgtype.Text{String: job.Status, Valid: true},
		ToStatus:   pgtype.Text{String: targetStatus, Valid: true},
		Version:    updated.Version,
		ActorType:  "operator",
		ActorID:    pgtype.Text{String: actorID, Valid: true},
		Metadata:   metaJSON,
	})

	changeSummaryJSON, _ := json.Marshal(map[string]interface{}{
		"fromStatus":      job.Status,
		"toStatus":        targetStatus,
		"previousVersion": job.Version,
	})
	_, _ = h.queries.CreateAuditLog(ctx, dbgen.CreateAuditLogParams{
		ActorID:       actorID,
		Action:        auditAction,
		TargetType:    "job",
		TargetID:      jobID.String(),
		Reason:        reason,
		Outcome:       "accepted",
		RequestID:     pgtype.Text{String: requestID, Valid: requestID != ""},
		ChangeSummary: changeSummaryJSON,
	})

	return &JobActionResponse{
		ID:      uuidToString(updated.ID),
		Status:  updated.Status,
		Version: updated.Version,
	}, nil
}

func toJobRecord(row dbgen.Job) JobRecord {
	return JobRecord{
		ID:                      uuidToString(row.ID),
		QueueID:                 formatUUIDPtr(row.QueueID),
		Name:                    row.Name,
		Payload:                 row.Payload,
		Priority:                row.Priority,
		Status:                  row.Status,
		Attempts:                row.Attempts,
		MaxAttempts:             row.MaxAttempts,
		Version:                 row.Version,
		RunAt:                   formatTimestampPtr(row.RunAt),
		CancellationRequestedAt: formatTimestampPtr(row.CancellationRequestedAt),
		CompletedAt:             formatTimestampPtr(row.CompletedAt),
		CreatedAt:               row.CreatedAt.Time.Format(time.RFC3339Nano),
		UpdatedAt:               row.UpdatedAt.Time.Format(time.RFC3339Nano),
	}
}

func formatTimestampPtr(t pgtype.Timestamp) *string {
	if !t.Valid {
		return nil
	}
	s := t.Time.Format(time.RFC3339Nano)
	return &s
}

func formatUUIDPtr(u pgtype.UUID) *string {
	if !u.Valid {
		return nil
	}
	s := uuidToString(u)
	return &s
}

func formatTextPtr(t pgtype.Text) *string {
	if !t.Valid {
		return nil
	}
	return &t.String
}

func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if strings.EqualFold(item, s) {
			return true
		}
	}
	return false
}

func uuidToString(u pgtype.UUID) string {
	if !u.Valid {
		return ""
	}
	b := u.Bytes
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

