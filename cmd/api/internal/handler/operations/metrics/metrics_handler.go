package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"queue-forge/cmd/api/internal/handler"
	dbgen "queue-forge/internal/db/gen"
)

type WindowMetrics struct {
	Minutes             int     `json:"minutes"`
	From                string  `json:"from"`
	JobsCreated         int64   `json:"jobsCreated"`
	JobsCompleted       int64   `json:"jobsCompleted"`
	JobsFailed          int64   `json:"jobsFailed"`
	ThroughputPerMinute float64 `json:"throughputPerMinute"`
}

type JobAggregateMetrics struct {
	Total         int64            `json:"total"`
	ByStatus      map[string]int64 `json:"byStatus"`
	RetryAttempts int64            `json:"retryAttempts"`
	SuccessRate   *float64         `json:"successRate"`
}

type WorkerAggregateMetrics struct {
	Total    int64            `json:"total"`
	ByStatus map[string]int64 `json:"byStatus"`
}

type MetricsSnapshotResponse struct {
	GeneratedAt string                 `json:"generatedAt"`
	Window      WindowMetrics          `json:"window"`
	Jobs        JobAggregateMetrics    `json:"jobs"`
	Workers     WorkerAggregateMetrics `json:"workers"`
}

type MetricsHandler struct {
	queries *dbgen.Queries
}

func New(pool *pgxpool.Pool) *MetricsHandler {
	return &MetricsHandler{
		queries: dbgen.New(pool),
	}
}

func (h *MetricsHandler) GetSnapshot(w http.ResponseWriter, r *http.Request) {
	windowMinutes := 15
	if wm := r.URL.Query().Get("windowMinutes"); wm != "" {
		if parsed, err := strconv.Atoi(wm); err == nil && parsed > 0 {
			windowMinutes = parsed
		}
	}

	now := time.Now()
	since := now.Add(-time.Duration(windowMinutes) * time.Minute)
	sincePg := pgtype.Timestamp{Time: since, Valid: true}

	ctx := r.Context()

	jobsByStatusRows, err := h.queries.GetJobsByStatus(ctx)
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	workersByStatusRows, err := h.queries.GetWorkersByStatus(ctx)
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	retryAttempts, err := h.queries.GetTotalRetryAttempts(ctx)
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	jobsCreatedInWindow, err := h.queries.GetJobsCreatedSince(ctx, sincePg)
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	jobsCompletedInWindow, err := h.queries.GetJobsCompletedSince(ctx, sincePg)
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	jobsFailedInWindow, err := h.queries.GetJobsFailedSince(ctx, sincePg)
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	jobsByStatus := make(map[string]int64)
	var totalJobs int64
	for _, row := range jobsByStatusRows {
		jobsByStatus[row.Status] = row.Count
		totalJobs += row.Count
	}

	workersByStatus := make(map[string]int64)
	var totalWorkers int64
	for _, row := range workersByStatusRows {
		workersByStatus[row.Status] = row.Count
		totalWorkers += row.Count
	}

	completed := jobsByStatus["COMPLETED"]
	failed := jobsByStatus["FAILED"]
	deadLetter := jobsByStatus["DEAD_LETTER"]
	terminal := completed + failed + deadLetter

	var successRate *float64
	if terminal > 0 {
		sr := float64(completed) / float64(terminal)
		successRate = &sr
	}

	resp := MetricsSnapshotResponse{
		GeneratedAt: now.Format(time.RFC3339Nano),
		Window: WindowMetrics{
			Minutes:             windowMinutes,
			From:                since.Format(time.RFC3339Nano),
			JobsCreated:         jobsCreatedInWindow,
			JobsCompleted:       jobsCompletedInWindow,
			JobsFailed:          jobsFailedInWindow,
			ThroughputPerMinute: float64(jobsCompletedInWindow) / float64(windowMinutes),
		},
		Jobs: JobAggregateMetrics{
			Total:         totalJobs,
			ByStatus:      jobsByStatus,
			RetryAttempts: retryAttempts,
			SuccessRate:   successRate,
		},
		Workers: WorkerAggregateMetrics{
			Total:    totalWorkers,
			ByStatus: workersByStatus,
		},
	}

	handler.WriteJSON(w, http.StatusOK, resp)
}

