package middleware

import (
	"context"
	"net/http"
	"strings"

	"queue-forge/cmd/api/internal/apierr"
	"queue-forge/cmd/api/internal/handler"
)

type contextKey string

const ActorIDKey contextKey = "actorId"

// BypassPaths contains routes exempted from authentication.
// Preserves Gap 8.9: /api/v1/operations/metrics/prometheus is included in bypass list.
var BypassPaths = map[string]struct{}{
	"/api/v1/operations/metrics/prometheus": {},
}

// CreateAuthenticationMiddleware validates Bearer tokens on operational routes.
func CreateAuthenticationMiddleware(allowedTokens map[string]struct{}) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, bypassed := BypassPaths[r.URL.Path]; bypassed {
				next.ServeHTTP(w, r)
				return
			}

			authHeader := r.Header.Get("Authorization")
			if !strings.HasPrefix(authHeader, "Bearer ") {
				handler.WriteError(w, apierr.Unauthorized(""))
				return
			}

			token := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
			if _, ok := allowedTokens[token]; !ok {
				handler.WriteError(w, apierr.Unauthorized(""))
				return
			}

			// Store actorId = "operator" in request context
			ctx := context.WithValue(r.Context(), ActorIDKey, "operator")
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// GetActorID retrieves the authenticated actorId from context.
func GetActorID(ctx context.Context) string {
	if val, ok := ctx.Value(ActorIDKey).(string); ok {
		return val
	}
	return "operator"
}

