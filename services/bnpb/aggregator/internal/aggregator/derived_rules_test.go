package aggregator

import (
	"testing"
	"time"
)

func TestParkingStatusFollowsAttemptsAndOutcome(t *testing.T) {
	now := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		retryable, failed bool
		attempts          int
		status            string
		wait              time.Duration
	}{
		{true, true, 0, parkingRetrying, time.Second},
		{true, true, 4, parkingRetrying, 16 * time.Second},
		{true, true, 5, parkingParked, 0},
		{false, true, 1, parkingParked, 0},
		{true, false, 3, parkingRecovered, 0},
		{false, false, 0, parkingRecovered, 0},
	} {
		status, next := nextParkingStatus(tc.retryable, tc.failed, tc.attempts, now)
		if status != tc.status || (next == nil) != (tc.wait == 0) || (next != nil && next.Sub(now) != tc.wait) {
			t.Errorf("%+v: status %s, next %v", tc, status, next)
		}
	}
}

func TestFreshnessFromProgressAndEvents(t *testing.T) {
	at := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		lastSuccess, staleSince *time.Time
		hasEvents               bool
		want                    string
	}{
		{&at, nil, true, "healthy"},
		{&at, &at, true, "stale"},
		{nil, nil, true, "stale"},
		{nil, nil, false, "unavailable"},
		{&at, &at, false, "unavailable"},
	} {
		if got := deriveFreshness(tc.lastSuccess, tc.staleSince, tc.hasEvents); got != tc.want {
			t.Errorf("%+v: got %s, want %s", tc, got, tc.want)
		}
	}
}
