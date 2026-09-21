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

	"queue-forge/cmd/scheduler/internal/promoter"
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
	log := zerolog.New(os.Stdout).With().Timestamp().Str("service", "scheduler").Logger()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		log.Info().Str("signal", sig.String()).Msg("Scheduler shutdown requested")
		cancel()
	}()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal().Msg("DATABASE_URL is required")
	}
	dbClient, err := internaldb.New(ctx, databaseURL)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to database")
	}
	defer dbClient.Close()

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Fatal().Msg("REDIS_URL is required")
	}
	rdb, err := redisqueue.NewClient(ctx, redisURL)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to Redis")
	}
	defer rdb.Close()

	intervalMs := readPositiveInt(os.Getenv("SCHEDULER_INTERVAL_MS"), 1_000)
	metricsPort := readPositiveInt(os.Getenv("SCHEDULER_METRICS_PORT"), 9103)

	p := promoter.New(rdb, dbClient.Pool, log)

	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	metricsServer := &http.Server{
		Addr:    ":" + strconv.Itoa(metricsPort),
		Handler: mux,
	}

	go func() {
		log.Info().Int("port", metricsPort).Msg("Scheduler metrics server started")
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("Metrics server error")
		}
	}()

	ticker := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)
	defer ticker.Stop()

	log.Info().Int("intervalMs", intervalMs).Msg("Scheduler started")

	for {
		select {
		case <-ctx.Done():
			metricsServer.Close()
			log.Info().Msg("Scheduler shutdown complete")
			return
		case <-ticker.C:
			if _, err := p.PromoteDueJobs(ctx); err != nil {
				log.Error().Err(err).Msg("Delayed job promotion failed")
			}
		}
	}
}
