package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	workerpkg "queue-forge/cmd/worker/internal"
	"queue-forge/cmd/worker/internal/heartbeat"
	"queue-forge/cmd/worker/internal/registry"
	internaldb "queue-forge/internal/db"
	"queue-forge/internal/metrics"
	"queue-forge/internal/redisqueue"
)

func readPositiveInt(value string, fallback int) int {
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func main() {
	log := zerolog.New(os.Stdout).With().Timestamp().Str("service", "worker").Logger()

	// 1. Connect to Database
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal().Msg("DATABASE_URL is required")
	}
	ctx := context.Background()
	dbClient, err := internaldb.New(ctx, databaseURL)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to database")
	}
	defer dbClient.Close()

	// 2. Connect to Redis
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Fatal().Msg("REDIS_URL is required")
	}
	rdb, err := redisqueue.NewClient(ctx, redisURL)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to Redis")
	}
	defer rdb.Close()

	// 3. Register worker
	reg := registry.New(dbClient.Pool)
	workerID, err := reg.Register(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to register worker")
	}
	log.Info().Str("workerId", workerID.String()).Msg("Worker registered")

	// 4. Start heartbeat service
	hb := heartbeat.New(dbClient.Pool, log)
	hbCtx, cancelHB := context.WithCancel(ctx)
	hb.Start(hbCtx, workerID)

	// 5. Start metrics server
	metricsPort := readPositiveInt(os.Getenv("WORKER_METRICS_PORT"), 9101)
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	metricsServer := &http.Server{
		Addr:    ":" + strconv.Itoa(metricsPort),
		Handler: mux,
	}

	go func() {
		log.Info().Int("port", metricsPort).Msg("Worker metrics server started")
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("Metrics server error")
		}
	}()

	// 6. Worker service
	leaseTTLMs := readPositiveInt(os.Getenv("WORKER_LEASE_TTL_MS"), 30_000)
	renewalIntervalMs := readPositiveInt(os.Getenv("WORKER_LEASE_RENEWAL_INTERVAL_MS"), 10_000)
	shutdownTimeoutMs := readPositiveInt(os.Getenv("WORKER_SHUTDOWN_TIMEOUT_MS"), 30_000)

	workerCfg := workerpkg.Config{
		LeaseTTL:              time.Duration(leaseTTLMs) * time.Millisecond,
		LeaseRenewalInterval: time.Duration(renewalIntervalMs) * time.Millisecond,
	}

	worker := workerpkg.New(workerID, dbClient.Pool, rdb, log, workerCfg)

	workerCtx, cancelWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- worker.Start(workerCtx)
	}()

	// 7. Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	sig := <-sigCh
	log.Info().Str("signal", sig.String()).Str("workerId", workerID.String()).Msg("Worker shutdown requested")

	worker.RequestShutdown()
	cancelWorker()

	// Wait for worker loop or shutdown timeout
	select {
	case <-workerDone:
	case <-time.After(time.Duration(shutdownTimeoutMs) * time.Millisecond):
		log.Warn().Msg("Worker shutdown timed out, releasing active lease immediately")
	}

	// Clean up resources
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = worker.ReleaseActiveLease(releaseCtx)
	releaseCancel()

	_ = metricsServer.Close()
	cancelHB()
	hb.Stop()
	_ = rdb.Close()
	_ = reg.Unregister(context.Background(), workerID)

	log.Info().Str("workerId", workerID.String()).Msg("Worker shutdown complete")
}
