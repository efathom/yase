package auth

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// HTTPMiddleware returns an HTTP middleware that authenticates requests.
// Extracts token from "Authorization: Bearer <token>" or "X-API-Key: <key>" headers.
// Unauthenticated requests receive 401. Public paths (health, ready) are exempt.
func HTTPMiddleware(auth Authenticator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip auth for health/readiness probes
			if r.URL.Path == "/health" || r.URL.Path == "/ready" {
				next.ServeHTTP(w, r)
				return
			}

			token := extractHTTPToken(r)
			if token == "" {
				http.Error(w, `{"error":"unauthorized: missing credentials"}`, http.StatusUnauthorized)
				return
			}

			ac, err := auth.Authenticate(r.Context(), token)
			if err != nil {
				slog.Warn("auth: authentication failed", "error", err)
				http.Error(w, `{"error":"unauthorized: invalid credentials"}`, http.StatusUnauthorized)
				return
			}

			ctx := WithContext(r.Context(), ac)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireScope returns an HTTP middleware that checks for a specific scope.
// Must be used after HTTPMiddleware.
func RequireScope(scope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ac := FromContext(r.Context())
			if ac == nil || !ac.HasScope(scope) {
				http.Error(w, `{"error":"forbidden: insufficient permissions"}`, http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func extractHTTPToken(r *http.Request) string {
	// Try Authorization: Bearer <token>
	if authHeader := r.Header.Get("Authorization"); authHeader != "" {
		if strings.HasPrefix(authHeader, "Bearer ") {
			return strings.TrimPrefix(authHeader, "Bearer ")
		}
	}
	// Try X-API-Key header
	if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
		return apiKey
	}
	// NOTE: query parameter auth removed — keys in URLs leak via access logs, proxies, browser history
	return ""
}

// UnaryServerInterceptor returns a gRPC unary interceptor for authentication.
// Extracts token from "authorization" metadata.
func UnaryServerInterceptor(auth Authenticator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		token := extractGRPCToken(ctx)
		if token == "" {
			return nil, status.Error(codes.Unauthenticated, "missing credentials")
		}
		ac, err := auth.Authenticate(ctx, token)
		if err != nil {
			// Log the reason; return a fixed message. The underlying error
			// names the expected issuer and audience, which would otherwise
			// go straight back to an unauthenticated caller.
			slog.Warn("auth: gRPC authentication failed", "method", info.FullMethod, "error", err)
			return nil, status.Error(codes.Unauthenticated, "invalid credentials")
		}
		return handler(WithContext(ctx, ac), req)
	}
}

// StreamServerInterceptor returns a gRPC stream interceptor for authentication.
func StreamServerInterceptor(auth Authenticator) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		token := extractGRPCToken(ss.Context())
		if token == "" {
			return status.Error(codes.Unauthenticated, "missing credentials")
		}
		ac, err := auth.Authenticate(ss.Context(), token)
		if err != nil {
			slog.Warn("auth: gRPC stream authentication failed", "method", info.FullMethod, "error", err)
			return status.Error(codes.Unauthenticated, "invalid credentials")
		}
		wrapped := &authServerStream{ServerStream: ss, ctx: WithContext(ss.Context(), ac)}
		return handler(srv, wrapped)
	}
}

type authServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authServerStream) Context() context.Context { return s.ctx }

func extractGRPCToken(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if vals := md.Get("authorization"); len(vals) > 0 {
		token := vals[0]
		if strings.HasPrefix(token, "Bearer ") {
			return strings.TrimPrefix(token, "Bearer ")
		}
		return token
	}
	if vals := md.Get("x-api-key"); len(vals) > 0 {
		return vals[0]
	}
	return ""
}
