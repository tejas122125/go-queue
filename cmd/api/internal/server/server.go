package server

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	jobshandler "queue-forge/cmd/api/internal/handler/jobs"
	audithandler "queue-forge/cmd/api/internal/handler/operations/audit"
	dlqhandler "queue-forge/cmd/api/internal/handler/operations/deadletter"
	opjobshandler "queue-forge/cmd/api/internal/handler/operations/jobs"
	metricshandler "queue-forge/cmd/api/internal/handler/operations/metrics"
	queuehandler "queue-forge/cmd/api/internal/handler/operations/queues"
	workerhandler "queue-forge/cmd/api/internal/handler/operations/workers"
	"queue-forge/cmd/api/internal/middleware"
	"queue-forge/internal/metrics"
	"queue-forge/internal/redisqueue"
)

// NewRouter constructs and configures the complete HTTP route tree.
func NewRouter(
	pool *pgxpool.Pool,
	rdb *redis.Client,
	log zerolog.Logger,
	allowedTokens map[string]struct{},
	workerStaleAfterMs int,
) *chi.Mux {
	r := chi.NewRouter()

	// Global middlewares
	r.Use(chimiddleware.RealIP)
	r.Use(chimiddleware.Recoverer)
	r.Use(middleware.RequestContext)
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "Idempotency-Key", "X-Request-Id"},
		ExposedHeaders:   []string{"Link", "X-Request-Id"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	// Health check
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true,"service":"api"}`))
	})

	// Services & Handlers
	qService := redisqueue.NewQueueService(rdb)

	publicJobHandler := jobshandler.New(pool)
	queuesHandler := queuehandler.New(pool, qService, log)
	workersHandler := workerhandler.New(pool, workerStaleAfterMs)
	opJobHandler := opjobshandler.New(pool)
	deadLetterHandler := dlqhandler.New(pool)
	auditHandler := audithandler.New(pool)
	opsMetricsHandler := metricshandler.New(pool)

	r.Route("/api/v1", func(r chi.Router) {
		// Public Prometheus metrics scrape endpoint
		r.Handle("/metrics", metrics.Handler())

		// Public job submission
		r.Route("/jobs", func(r chi.Router) {
			r.With(middleware.ValidateIdempotencyKey).Post("/", publicJobHandler.Create)
		})

		// Operations platform (requires authentication)
		r.Route("/operations", func(r chi.Router) {
			r.Use(middleware.CreateAuthenticationMiddleware(allowedTokens))

			r.Get("/metrics", opsMetricsHandler.GetSnapshot)

			r.Route("/queues", func(r chi.Router) {
				r.Get("/", queuesHandler.List)
				r.Get("/{name}", queuesHandler.Get)
				r.Put("/{name}/pause", queuesHandler.Pause)
				r.Put("/{name}/resume", queuesHandler.Resume)
			})

			r.Route("/workers", func(r chi.Router) {
				r.Get("/", workersHandler.List)
				r.Get("/{id}", workersHandler.Get)
			})

			r.Route("/jobs", func(r chi.Router) {
				r.Get("/", opJobHandler.List)
				r.Get("/{id}", opJobHandler.Get)
				r.Get("/{id}/executions", opJobHandler.GetExecutions)
				r.Get("/{id}/events", opJobHandler.GetEvents)
				r.Post("/{id}/retry", opJobHandler.Retry)
				r.Post("/{id}/cancel", opJobHandler.Cancel)
			})

			r.Route("/dead-letter-jobs", func(r chi.Router) {
				r.Get("/", deadLetterHandler.List)
				r.Get("/{id}", deadLetterHandler.Get)
				r.Post("/{id}/replay", deadLetterHandler.Replay)
			})

			r.Route("/audit-logs", func(r chi.Router) {
				r.Get("/", auditHandler.List)
			})
		})
	})

	return r
}

