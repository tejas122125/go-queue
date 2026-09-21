package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	dbgen "queue-forge/internal/db/gen"
)

// Client wraps a pgx connection pool and sqlc generated queries.
type Client struct {
	Pool    *pgxpool.Pool
	Queries *dbgen.Queries
}

// New creates a new database connection pool and initializes sqlc Queries.
func New(ctx context.Context, dsn string) (*Client, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing database connection string: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("creating database connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	queries := dbgen.New(pool)
	return &Client{
		Pool:    pool,
		Queries: queries,
	}, nil
}

// Close closes the underlying connection pool.
func (c *Client) Close() {
	if c.Pool != nil {
		c.Pool.Close()
	}
}

// WithTx executes a function within a database transaction, automatically rolling back on error
// and committing on success.
func WithTx[T any](ctx context.Context, pool *pgxpool.Pool, fn func(tx pgx.Tx) (T, error)) (T, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	result, err := fn(tx)
	if err != nil {
		var zero T
		return zero, err
	}

	if err := tx.Commit(ctx); err != nil {
		var zero T
		return zero, fmt.Errorf("committing transaction: %w", err)
	}

	return result, nil
}

// WithTxExec executes a statement within a transaction that does not return a value.
func WithTxExec(ctx context.Context, pool *pgxpool.Pool, fn func(tx pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := fn(tx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

