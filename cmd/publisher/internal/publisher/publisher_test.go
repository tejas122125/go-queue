package publisher

import (
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestParseDispatchPayload_Valid(t *testing.T) {
	raw, _ := json.Marshal(map[string]interface{}{
		"jobId":    "abc-123",
		"priority": 10,
		"name":     "send-email",
	})

	payload, err := parseDispatchPayload(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if payload.JobID != "abc-123" {
		t.Errorf("expected jobId abc-123, got %s", payload.JobID)
	}
	if payload.Priority != 10 {
		t.Errorf("expected priority 10, got %d", payload.Priority)
	}
	if payload.Name != "send-email" {
		t.Errorf("expected name send-email, got %s", payload.Name)
	}
}

func TestParseDispatchPayload_MissingJobID(t *testing.T) {
	raw, _ := json.Marshal(map[string]interface{}{
		"priority": 5,
	})

	_, err := parseDispatchPayload(raw)
	if err == nil {
		t.Fatal("expected error for missing jobId")
	}
}

func TestParseDispatchPayload_InvalidJSON(t *testing.T) {
	_, err := parseDispatchPayload([]byte("not-json"))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestParseDispatchPayload_NoPriority(t *testing.T) {
	raw, _ := json.Marshal(map[string]interface{}{
		"jobId": "abc-123",
	})

	payload, err := parseDispatchPayload(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Priority defaults to 0 (Go zero value), which maps to LOW queue — matches TS behavior
	if payload.Priority != 0 {
		t.Errorf("expected priority 0 (default), got %d", payload.Priority)
	}
}

func TestUuidToString_Valid(t *testing.T) {
	u := pgtype.UUID{
		Bytes: [16]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0},
		Valid: true,
	}

	result := uuidToString(u)
	expected := "12345678-9abc-def0-1234-56789abcdef0"
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestUuidToString_Invalid(t *testing.T) {
	u := pgtype.UUID{Valid: false}

	result := uuidToString(u)
	if result != "" {
		t.Errorf("expected empty string for invalid UUID, got %s", result)
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig("worker-1")
	if cfg.WorkerID != "worker-1" {
		t.Errorf("expected worker-1, got %s", cfg.WorkerID)
	}
	if cfg.BatchSize != 50 {
		t.Errorf("expected batch size 50, got %d", cfg.BatchSize)
	}
	if cfg.PollIntervalMs != 100 {
		t.Errorf("expected poll interval 100, got %d", cfg.PollIntervalMs)
	}
	if cfg.MaxRetries != 5 {
		t.Errorf("expected max retries 5, got %d", cfg.MaxRetries)
	}
	if cfg.ClaimTTLMs != 30_000 {
		t.Errorf("expected claim TTL 30000, got %d", cfg.ClaimTTLMs)
	}
}

