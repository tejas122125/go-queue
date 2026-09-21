package executor

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog"

	dbgen "queue-forge/internal/db/gen"
)

// HandlerFunc is the function signature for executing a job.
type HandlerFunc func(ctx context.Context, job *dbgen.Job) error

// JobExecutor executes jobs by dispatching them to registered handler functions.
type JobExecutor struct {
	mu       sync.RWMutex
	handlers map[string]HandlerFunc
	log      zerolog.Logger
}

// New constructs a JobExecutor.
func New(log zerolog.Logger) *JobExecutor {
	return &JobExecutor{
		handlers: make(map[string]HandlerFunc),
		log:      log.With().Str("component", "executor").Logger(),
	}
}

// Register registers a handler function for a job name.
func (e *JobExecutor) Register(jobName string, handler HandlerFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers[jobName] = handler
}

// Execute looks up the handler for the job name and executes it.
// If no handler is registered, returns an error matching TS: "No handler registered for job name: ...".
func (e *JobExecutor) Execute(ctx context.Context, job *dbgen.Job) error {
	e.mu.RLock()
	handler, ok := e.handlers[job.Name]
	e.mu.RUnlock()

	if !ok {
		return fmt.Errorf("No handler registered for job name: %s", job.Name)
	}

	e.log.Info().
		Str("jobId", uuidToString(job.ID)).
		Str("jobName", job.Name).
		Msg("Executing job")

	return handler(ctx, job)
}

func uuidToString(u pgtype.UUID) string {
	if !u.Valid {
		return ""
	}
	b := u.Bytes
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
