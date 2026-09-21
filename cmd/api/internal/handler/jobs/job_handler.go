package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"queue-forge/cmd/api/internal/apierr"
	"queue-forge/cmd/api/internal/handler"
	internaldb "queue-forge/internal/db"
	dbgen "queue-forge/internal/db/gen"
)

// CreateJobRequest defines the request body for job creation.
type CreateJobRequest struct {
	Name        string          `json:"name"`
	Payload     json.RawMessage `json:"payload"`
	Priority    *int32          `json:"priority"`
	MaxAttempts *int32          `json:"maxAttempts"`
}

// JobHandler handles job creation requests.
type JobHandler struct {
	pool    *pgxpool.Pool
	queries *dbgen.Queries
}

// New constructs a JobHandler.
func New(pool *pgxpool.Pool) *JobHandler {
	return &JobHandler{
		pool:    pool,
		queries: dbgen.New(pool),
	}
}

// Create handles POST /api/v1/jobs with idempotency and transactional outbox.
func (h *JobHandler) Create(w http.ResponseWriter, r *http.Request) {
	idempotencyKey := r.Header.Get("Idempotency-Key")

	var req CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		handler.WriteError(w, apierr.ValidationError("Invalid JSON body"))
		return
	}

	// Validate inputs
	if len(req.Name) < 3 || len(req.Name) > 255 {
		handler.WriteError(w, apierr.ValidationError("Job name must be between 3 and 255 characters"))
		return
	}
	if len(req.Payload) == 0 || string(req.Payload) == "null" {
		handler.WriteError(w, apierr.ValidationError("Job payload is required"))
		return
	}

	priority := int32(0)
	if req.Priority != nil {
		if *req.Priority < 0 || *req.Priority > 10 {
			handler.WriteError(w, apierr.ValidationError("Priority must be between 0 and 10"))
			return
		}
		priority = *req.Priority
	}

	maxAttempts := int32(3)
	if req.MaxAttempts != nil {
		if *req.MaxAttempts < 1 || *req.MaxAttempts > 10 {
			handler.WriteError(w, apierr.ValidationError("Max attempts must be between 1 and 10"))
			return
		}
		maxAttempts = *req.MaxAttempts
	}

	job, err := h.createJobWithIdempotencyAndOutbox(r.Context(), req.Name, req.Payload, priority, maxAttempts, idempotencyKey)
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	handler.WriteJSON(w, http.StatusCreated, job)
}

func (h *JobHandler) createJobWithIdempotencyAndOutbox(
	ctx context.Context,
	name string,
	payload json.RawMessage,
	priority int32,
	maxAttempts int32,
	idempotencyKey string,
) (*dbgen.Job, error) {
	job, err := internaldb.WithTx(ctx, h.pool, func(tx pgx.Tx) (*dbgen.Job, error) {
		q := dbgen.New(tx)

		// 1. Check existing idempotency key
		if idempotencyKey != "" {
			existingKey, err := q.FindIdempotencyKey(ctx, idempotencyKey)
			if err == nil {
				existingJob, err := q.FindJobByID(ctx, existingKey.JobID)
				if err != nil {
					return nil, fmt.Errorf("idempotency key references a missing job: %w", err)
				}
				return &existingJob, nil
			} else if !errors.Is(err, pgx.ErrNoRows) {
				return nil, err
			}
		}

		// 2. Insert Job
		insertedJob, err := q.CreateJob(ctx, dbgen.CreateJobParams{
			Name:        name,
			Payload:     payload,
			Priority:    priority,
			Status:      "PENDING",
			Attempts:    0,
			MaxAttempts: maxAttempts,
			RunAt:       pgtype.Timestamp{},
		})
		if err != nil {
			return nil, fmt.Errorf("inserting job: %w", err)
		}

		// 3. Save idempotency key if provided
		if idempotencyKey != "" {
			_, err = q.CreateIdempotencyKey(ctx, dbgen.CreateIdempotencyKeyParams{
				Key:   idempotencyKey,
				JobID: insertedJob.ID,
			})
			if err != nil {
				return nil, err
			}
		}

		// 4. Create outbox event
		outboxPayload, _ := json.Marshal(map[string]interface{}{
			"jobId":    uuidToString(insertedJob.ID),
			"priority": insertedJob.Priority,
			"name":     insertedJob.Name,
		})

		_, err = q.CreateOutboxEvent(ctx, dbgen.CreateOutboxEventParams{
			EventType:        "job.dispatch.requested",
			AggregateType:    "job",
			AggregateID:      insertedJob.ID,
			DeduplicationKey: fmt.Sprintf("job-created-%s", uuidToString(insertedJob.ID)),
			Payload:          outboxPayload,
		})
		if err != nil {
			return nil, fmt.Errorf("inserting outbox event: %w", err)
		}

		return &insertedJob, nil
	})

	if err != nil {
		// Handle concurrent insert race condition on unique constraint code 23505
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && idempotencyKey != "" {
			existingKey, findErr := h.queries.FindIdempotencyKey(ctx, idempotencyKey)
			if findErr == nil {
				existingJob, getErr := h.queries.FindJobByID(ctx, existingKey.JobID)
				if getErr == nil {
					return &existingJob, nil
				}
			}
		}
		return nil, err
	}

	return job, nil
}

func uuidToString(u pgtype.UUID) string {
	if !u.Valid {
		return ""
	}
	b := u.Bytes
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

