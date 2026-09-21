package logger

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/rs/zerolog"
)

func TestLoggerOutput(t *testing.T) {
	var buf bytes.Buffer
	testLogger := zerolog.New(&buf).With().Timestamp().Logger()

	testLogger.Info().Str("jobId", "123").Msg("Processing job")

	var output map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &output); err != nil {
		t.Fatalf("failed to parse log output as json: %v", err)
	}

	if output["level"] != "info" {
		t.Errorf("expected level info, got %v", output["level"])
	}
	if output["jobId"] != "123" {
		t.Errorf("expected jobId 123, got %v", output["jobId"])
	}
	if output["message"] != "Processing job" {
		t.Errorf("expected message 'Processing job', got %v", output["message"])
	}
}

