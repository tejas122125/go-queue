package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"queue-forge/cmd/api/internal/server"
	internaldb "queue-forge/internal/db"
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

func getAllowedTokens() map[string]struct{} {
	tokenStr := os.Getenv("OPERATIONS_AUTH_TOKEN")
	if tokenStr == "" {
		tokenStr = "dev-token"
	}
	tokens := make(map[string]struct{})
	for _, t := range strings.Split(tokenStr, ",") {
		trimmed := strings.TrimSpace(t)
		if trimmed != "" {
			tokens[trimmed] = struct{}{}
		}
	}
	return tokens
}

func main() {
	log := zerolog.New(os.Stdout).With().Timestamp().Str("service", "api").Logger()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

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

	port := readPositiveInt(os.Getenv("PORT"), 3000)
	workerStaleAfterMs := readPositiveInt(os.Getenv("WORKER_STALE_AFTER_MS"), 15_000)
	allowedTokens := getAllowedTokens()

	router := server.NewRouter(dbClient.Pool, rdb, log, allowedTokens, workerStaleAfterMs)

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: router,
	}

	go func() {
		log.Info().Int("port", port).Msg("API server listening")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("HTTP server error")
		}
	}()

	sig := <-sigCh
	log.Info().Str("signal", sig.String()).Msg("API shutdown requested")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("HTTP server shutdown error")
	}

	log.Info().Msg("API shutdown complete")
}
