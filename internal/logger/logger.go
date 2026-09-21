package logger

import (
	"os"
	"strings"

	"github.com/rs/zerolog"
)

// L is the shared structured logger instance.
var L zerolog.Logger

func init() {
	levelStr := strings.ToLower(os.Getenv("LOG_LEVEL"))
	level := zerolog.InfoLevel
	switch levelStr {
	case "debug":
		level = zerolog.DebugLevel
	case "warn", "warning":
		level = zerolog.WarnLevel
	case "error":
		level = zerolog.ErrorLevel
	case "trace":
		level = zerolog.TraceLevel
	}

	// Output standard JSON with timestamp
	L = zerolog.New(os.Stdout).With().Timestamp().Logger().Level(level)
}

// New returns a new logger configured with the given component name.
func New(component string) zerolog.Logger {
	return L.With().Str("component", component).Logger()
}

