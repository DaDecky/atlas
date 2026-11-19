package sensor

import "time"

// Position is the last accepted observation in the contract order of observed_at, then observation_id.
type Position struct {
	ObservedAt    time.Time `json:"observed_at"`
	ObservationID string    `json:"observation_id"`
}

// Health holds the signs that the sensor is sick, judged from its data and not from /healthz.
type Health struct {
	Stale                            bool
	Repeated, OutOfOrder, Violations int
}

func (p Position) before(o Observation) bool {
	if !p.ObservedAt.Equal(o.ObservedAt) {
		return p.ObservedAt.Before(o.ObservedAt)
	}

	return p.ObservationID < o.ID
}

func (h Health) Degraded() bool {
	return h.Stale || h.Repeated+h.OutOfOrder+h.Violations > 0
}

// Assess checks a page against the last position and returns the valid items without repeats,
// and the position after them. Out-of-order items are still returned, since each is a real reading.
// Stale means the newest reading lies more than staleAfter behind generated_at.
func Assess(page Page, last Position, staleAfter time.Duration) ([]Item, Position, Health) {
	var (
		health Health
		newest time.Time
	)

	accepted := []Item{}
	seen := map[string]bool{}

	for _, item := range page.Items {
		if item.Violation != "" {
			health.Violations++
			continue
		}

		o := item.Observation
		if seen[o.ID] || (o.ID == last.ObservationID && o.ObservedAt.Equal(last.ObservedAt)) {
			health.Repeated++
			continue
		}

		seen[o.ID] = true

		if last.before(o) {
			last = Position{ObservedAt: o.ObservedAt, ObservationID: o.ID}
		} else {
			health.OutOfOrder++
		}

		if o.ObservedAt.After(newest) {
			newest = o.ObservedAt
		}

		accepted = append(accepted, item)
	}

	health.Stale = len(accepted) > 0 && page.GeneratedAt.Sub(newest) > staleAfter

	return accepted, last, health
}
