package middleware

import (
	"encoding/json"
	"net/http"
)

// ValidateIdempotencyKey checks for the required Idempotency-Key header on job creation.
func ValidateIdempotencyKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		if key == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "Missing Idempotency-Key",
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}

