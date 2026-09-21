package handler

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"

	"queue-forge/cmd/api/internal/apierr"
)

type ErrorEnvelope struct {
	Error *apierr.APIError `json:"error"`
}

// WriteJSON sends a JSON response with the given status code.
func WriteJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if data != nil {
		_ = json.NewEncoder(w).Encode(data)
	}
}

// WriteError sends an error response formatted to match TS error envelope.
func WriteError(w http.ResponseWriter, err error) {
	var apiErr *apierr.APIError
	if errors.As(err, &apiErr) {
		WriteJSON(w, apiErr.StatusCode, ErrorEnvelope{Error: apiErr})
		return
	}

	WriteJSON(w, http.StatusInternalServerError, ErrorEnvelope{
		Error: apierr.New(500, "INTERNAL_SERVER_ERROR", "An unexpected error occurred"),
	})
}

// EncodeCursor encodes a cursor struct to base64url string.
func EncodeCursor(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// DecodeCursor decodes a base64url string into target cursor struct.
func DecodeCursor(s string, target any) error {
	// Support both RawURLEncoding and URLEncoding with padding
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		b, err = base64.URLEncoding.DecodeString(s)
		if err != nil {
			return apierr.InvalidCursor()
		}
	}

	if err := json.Unmarshal(b, target); err != nil {
		return apierr.InvalidCursor()
	}
	return nil
}

