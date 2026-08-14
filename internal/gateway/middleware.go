package gateway

import (
	"log/slog"
	"net/http"

	"github.com/efathom/yase/pkg/auth"
)

// Recover returns middleware that recovers from panics in handlers, logging
// the panic and returning a 500 JSON response instead of resetting the connection.
func Recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("panic recovered in HTTP handler", "panic", rec, "path", r.URL.Path)
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// BodyLimit caps the request body size for all downstream handlers.
func BodyLimit(limit int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, limit)
			next.ServeHTTP(w, r)
		})
	}
}

// RequireScope returns a middleware that enforces a scope, but only when
// auth enforcement is enabled. When disabled it passes through.
func (h *Handler) requireScope(scope string, next http.Handler) http.Handler {
	if !h.authEnabled {
		return next
	}
	return auth.RequireScope(scope)(next)
}
