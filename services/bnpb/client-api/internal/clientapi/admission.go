package clientapi

import (
	"context"
	"net/http"
	"sync"
)

// admission caps the requests in progress and keeps a share of them for clients other than Media.
type admission struct {
	mu                       sync.Mutex
	inUse, limit, mediaLimit int
}

func newAdmission(limit, reservedPercent int) *admission {
	return &admission{limit: limit, mediaLimit: limit - limit*reservedPercent/100}
}

func (a *admission) acquire(media bool) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	limit := a.limit
	if media {
		limit = a.mediaLimit
	}

	if a.inUse >= limit {
		return false
	}

	a.inUse++

	return true
}

func (a *admission) release() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.inUse--
}

// admit takes a slot for the class of the caller, and answers 429 when none is left.
func (a *API) admit(ctx context.Context, w http.ResponseWriter, claims AccessClaims) bool {
	media := requestClass(claims) == "media"
	if a.admission.acquire(media) {
		return true
	}

	if a.OnAdmissionReject != nil {
		a.OnAdmissionReject(ctx, media)
	}

	w.Header().Set("Retry-After", "1")
	writeError(w, http.StatusTooManyRequests, "capacity_exceeded")

	return false
}

// requestClass names the admission class that the Aggregator applies to a forwarded read.
func requestClass(claims AccessClaims) string {
	if hasScope(claims.Scope, "hazards:raw") {
		return "priority"
	}

	return "media"
}
