package auth

import "net/http"

// LoadShedder limits concurrent requests to prevent resource exhaustion.
// Returns 503 Service Unavailable when at capacity.
type LoadShedder struct {
	semaphore chan struct{}
}

// NewLoadShedder creates a load shedder with the given concurrency limit.
func NewLoadShedder(maxConcurrent int) *LoadShedder {
	if maxConcurrent <= 0 {
		maxConcurrent = 100
	}
	return &LoadShedder{
		semaphore: make(chan struct{}, maxConcurrent),
	}
}

// HTTPMiddleware returns middleware that sheds load when at capacity.
func (l *LoadShedder) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case l.semaphore <- struct{}{}:
			defer func() { <-l.semaphore }()
			next.ServeHTTP(w, r)
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, `{"error":"service overloaded, try again later"}`, http.StatusServiceUnavailable)
		}
	})
}

// InFlight returns the current number of in-flight requests.
func (l *LoadShedder) InFlight() int {
	return len(l.semaphore)
}
