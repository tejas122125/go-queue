package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"queue-forge/cmd/publisher/internal/publisher"
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
	log := zerolog.New(os.Stdout).With().Timestamp().Str("service", "publisher").Logger()

	// Context with signal cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		log.Info().Str("signal", sig.String()).Msg("Publisher shutdown requested")
		cancel()
	}()

	// Database
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal().Msg("DATABASE_URL is required")
	}
	dbClient, err := internaldb.New(ctx, databaseURL)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to database")
	}
	defer dbClient.Close()

	// Redis
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Fatal().Msg("REDIS_URL is required")
	}
	rdb, err := redisqueue.NewClient(ctx, redisURL)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to Redis")
	}
	defer rdb.Close()

	queueService := redisqueue.NewQueueService(rdb)

	// Config
	workerID := os.Getenv("PUBLISHER_ID")
	if workerID == "" {
		workerID = uuid.New().String()
	}

	config := publisher.Config{
		WorkerID:       workerID,
		BatchSize:      readPositiveInt(os.Getenv("OUTBOX_BATCH_SIZE"), 50),
		PollIntervalMs: readPositiveInt(os.Getenv("OUTBOX_POLL_INTERVAL_MS"), 100),
		MaxRetries:     readPositiveInt(os.Getenv("OUTBOX_MAX_RETRIES"), 5),
		ClaimTTLMs:     30_000,
	}

	pub := publisher.New(dbClient.Pool, queueService, config, log)

	// Metrics server
	metricsPort := readPositiveInt(os.Getenv("PUBLISHER_METRICS_PORT"), 9102)
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	metricsServer := &http.Server{
		Addr:    ":" + strconv.Itoa(metricsPort),
		Handler: mux,
	}

	go func() {
		log.Info().Int("port", metricsPort).Msg("Publisher metrics server started")
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("Metrics server error")
		}
	}()

	// Run publisher
	if err := pub.Start(ctx); err != nil {
		log.Error().Err(err).Msg("Publisher failed")
		os.Exit(1)
	}

	// Shutdown metrics server
	metricsServer.Close()
	log.Info().Msg("Publisher shutdown complete")
}
