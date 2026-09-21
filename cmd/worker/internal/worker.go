package internal

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"queue-forge/cmd/worker/internal/deadletter"
	"queue-forge/cmd/worker/internal/executor"
	"queue-forge/cmd/worker/internal/lease"
	"queue-forge/cmd/worker/internal/retry"
	dbgen "queue-forge/internal/db/gen"
	"queue-forge/internal/metrics"
	"queue-forge/internal/redisqueue"
)

// ActiveLease tracks the currently acquired lease for a worker.
type ActiveLease struct {
	JobID        uuid.UUID
	FencingToken int32
}

// Config holds options for the worker.
type Config struct {
	LeaseTTL              time.Duration
	LeaseRenewalInterval time.Duration
}

// DefaultConfig provides default settings matching TS WorkerService options.
func DefaultConfig() Config {
	return Config{
		LeaseTTL:              30 * time.Second,
		LeaseRenewalInterval: 10 * time.Second,
	}
}

// WorkerService orchestrates job polling, lease acquisition, execution, renewal, and completion.
type WorkerService struct {
	workerID          uuid.UUID
	config            Config
	mu                sync.Mutex
	shutdownRequested bool
	activeLease       *ActiveLease

	pool              *pgxpool.Pool
	queries           *dbgen.Queries
	queue             *redisqueue.QueueService
	leaseRepo         *lease.Repository
	executor          *executor.JobExecutor
	retryService      *retry.RetryService
	deadLetterService *deadletter.DeadLetterService
	log               zerolog.Logger
}

// New constructs a WorkerService.
func New(
	workerID uuid.UUID,
	pool *pgxpool.Pool,
	rdb *redis.Client,
	log zerolog.Logger,
	cfg Config,
) *WorkerService {
	qService := redisqueue.NewQueueService(rdb)

	return &WorkerService{
		workerID:          workerID,
		config:            cfg,
		pool:              pool,
		queries:           dbgen.New(pool),
		queue:             qService,
		leaseRepo:         lease.New(pool),
		executor:          executor.New(log),
		retryService:      retry.New(qService),
		deadLetterService: deadletter.New(pool),
		log:               log.With().Str("component", "worker").Str("workerId", workerID.String()).Logger(),
	}
}

// Executor returns the underlying JobExecutor to allow registering handlers.
func (w *WorkerService) Executor() *executor.JobExecutor {
	return w.executor
}

// Start runs the worker polling and execution loop until shutdown is requested or ctx is cancelled.
func (w *WorkerService) Start(ctx context.Context) error {
	w.log.Info().Msg("Worker loop started")

	for {
		w.mu.Lock()
		shuttingDown := w.shutdownRequested
		w.mu.Unlock()

		if shuttingDown || ctx.Err() != nil {
			w.log.Info().Msg("Worker loop stopped")
			return nil
		}

		timer := prometheus.NewTimer(metrics.ExecutionLatency)

		// 1. Poll next job ID from Redis queues (timeout 1s)
		jobIDStr, err := w.queue.Dequeue(ctx, 1)
		if err != nil || jobIDStr == "" {
			timer.ObserveDuration()
			continue
		}

		jobUUID, err := uuid.Parse(jobIDStr)
		if err != nil {
			w.log.Warn().Str("jobId", jobIDStr).Err(err).Msg("Invalid job UUID dequeued")
			timer.ObserveDuration()
			continue
		}

		// 2. Acquire lease in PostgreSQL
		leaseHandle, err := w.leaseRepo.AcquireLease(ctx, jobUUID, w.workerID, w.config.LeaseTTL)
		if err != nil || leaseHandle == nil {
			timer.ObserveDuration()
			continue
		}

		w.mu.Lock()
		w.activeLease = &ActiveLease{
			JobID:        jobUUID,
			FencingToken: leaseHandle.FencingToken,
		}
		w.mu.Unlock()

		// 3. Find job record
		job, err := w.queries.FindJobByID(ctx, pgtype.UUID{Bytes: jobUUID, Valid: true})
		if err != nil {
			_ = w.leaseRepo.ReleaseLease(ctx, jobUUID, w.workerID, leaseHandle.FencingToken)
			w.mu.Lock()
			w.activeLease = nil
			w.mu.Unlock()
			timer.ObserveDuration()
			continue
		}

		// 4. Start lease renewal goroutine
		renewCtx, cancelRenew := context.WithCancel(ctx)
		renewalTicker := time.NewTicker(w.config.LeaseRenewalInterval)

		go func(jID uuid.UUID, fToken int32) {
			defer renewalTicker.Stop()
			for {
				select {
				case <-renewalTicker.C:
					_, _ = w.leaseRepo.RenewLease(renewCtx, jID, w.workerID, fToken, w.config.LeaseTTL)
				case <-renewCtx.Done():
					return
				}
			}
		}(jobUUID, leaseHandle.FencingToken)

		// 5. Execute job under lease
		func() {
			defer cancelRenew()
			defer func() {
				_ = w.leaseRepo.ReleaseLease(ctx, jobUUID, w.workerID, leaseHandle.FencingToken)
				w.mu.Lock()
				w.activeLease = nil
				w.mu.Unlock()
				timer.ObserveDuration()
			}()

			markedRunning, err := w.leaseRepo.MarkRunningWithLease(ctx, jobUUID, w.workerID, leaseHandle.FencingToken)
			if err != nil || !markedRunning {
				return
			}

			execErr := w.executor.Execute(ctx, &job)

			if execErr == nil {
				markedCompleted, _ := w.leaseRepo.MarkCompletedWithLease(ctx, jobUUID, w.workerID, leaseHandle.FencingToken)
				if markedCompleted {
					metrics.JobsCompleted.Inc()
				}
				return
			}

			// Execution failed
			metrics.JobsFailed.Inc()

			if job.Attempts < job.MaxAttempts {
				_ = w.retryService.Retry(ctx, jobIDStr, int(job.Attempts))
				_ = w.leaseRepo.IncrementAttempts(ctx, jobUUID)
			} else {
				_ = w.deadLetterService.MoveToDeadLetter(ctx, &job, execErr)
			}
		}()
	}
}

// RequestShutdown requests the worker to stop processing new jobs.
func (w *WorkerService) RequestShutdown() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.shutdownRequested = true
}

// ReleaseActiveLease releases any active lease held by this worker during shutdown.
func (w *WorkerService) ReleaseActiveLease(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.activeLease == nil {
		return nil
	}

	err := w.leaseRepo.ReleaseLease(ctx, w.activeLease.JobID, w.workerID, w.activeLease.FencingToken)
	w.activeLease = nil
	return err
}

