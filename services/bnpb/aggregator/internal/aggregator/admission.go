package aggregator

import "sync"

// Admission caps the queries in progress and keeps a share of them for callers other than Media.
type Admission struct {
	mu                       sync.Mutex
	inUse, limit, mediaLimit int
}

func NewAdmission(limit, reservedPercent int) *Admission {
	return &Admission{limit: limit, mediaLimit: limit - limit*reservedPercent/100}
}

func (a *Admission) acquire(media bool) bool {
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

func (a *Admission) release() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.inUse--
}
