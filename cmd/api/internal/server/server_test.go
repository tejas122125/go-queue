package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"queue-forge/cmd/api/internal/handler"
	"queue-forge/cmd/api/internal/middleware"
)

func TestHealthEndpoint(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true,"service":"api"}`))
	})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if body["success"] != true {
		t.Errorf("expected success true, got %v", body["success"])
	}
	if body["service"] != "api" {
		t.Errorf("expected service 'api', got %v", body["service"])
	}
}

func TestIdempotencyMiddleware(t *testing.T) {
	r := chi.NewRouter()
	r.With(middleware.ValidateIdempotencyKey).Post("/test", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Missing header -> 400
	req := httptest.NewRequest(http.MethodPost, "/test", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	var errResp map[string]string
	_ = json.Unmarshal(rec.Body.Bytes(), &errResp)
	if errResp["error"] != "Missing Idempotency-Key" {
		t.Errorf("expected 'Missing Idempotency-Key', got %q", errResp["error"])
	}

	// Present header -> 200
	reqWithHeader := httptest.NewRequest(http.MethodPost, "/test", nil)
	reqWithHeader.Header.Set("Idempotency-Key", "test-key-123")
	recOk := httptest.NewRecorder()
	r.ServeHTTP(recOk, reqWithHeader)

	if recOk.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recOk.Code)
	}
}

func TestAuthMiddleware(t *testing.T) {
	allowed := map[string]struct{}{
		"secret-token": {},
	}

	r := chi.NewRouter()
	r.Use(middleware.CreateAuthenticationMiddleware(allowed))
	r.Get("/protected", func(w http.ResponseWriter, r *http.Request) {
		actorID := middleware.GetActorID(r.Context())
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(actorID))
	})

	// No auth header -> 401
	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}

	// Invalid token -> 401
	reqBad := httptest.NewRequest(http.MethodGet, "/protected", nil)
	reqBad.Header.Set("Authorization", "Bearer wrong-token")
	recBad := httptest.NewRecorder()
	r.ServeHTTP(recBad, reqBad)

	if recBad.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", recBad.Code)
	}

	// Valid token -> 200 with actorId "operator"
	reqGood := httptest.NewRequest(http.MethodGet, "/protected", nil)
	reqGood.Header.Set("Authorization", "Bearer secret-token")
	recGood := httptest.NewRecorder()
	r.ServeHTTP(recGood, reqGood)

	if recGood.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recGood.Code)
	}
	if recGood.Body.String() != "operator" {
		t.Errorf("expected actorId 'operator', got %q", recGood.Body.String())
	}
}

func TestCursorRoundTrip(t *testing.T) {
	type Cursor struct {
		CreatedAt string `json:"createdAt"`
		ID        string `json:"id"`
	}

	original := Cursor{
		CreatedAt: "2026-10-18T10:00:00.000Z",
		ID:        "550e8400-e29b-41d4-a716-446655440000",
	}

	encoded, err := handler.EncodeCursor(original)
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	var decoded Cursor
	if err := handler.DecodeCursor(encoded, &decoded); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	if decoded != original {
		t.Errorf("decoded %+v does not match original %+v", decoded, original)
	}
}

