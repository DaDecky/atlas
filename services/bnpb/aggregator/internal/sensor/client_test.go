package sensor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func fakeSensor(t *testing.T, healthy *atomic.Bool, hits *atomic.Int64) *Client {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)

		if r.Header.Get("Authorization") != "Bearer token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if !healthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		_, _ = w.Write(page(validRecord))
	}))
	t.Cleanup(server.Close)

	return NewClient(server.URL, "token", server.Client(), time.Second, func(string, string) {})
}

func TestBreakerCapsLoadThenRecovers(t *testing.T) {
	var (
		healthy atomic.Bool
		hits    atomic.Int64
	)

	client := fakeSensor(t, &healthy, &hits)
	ctx := context.Background()

	for range 30 {
		_, _ = client.Observations(ctx, "", 100)
	}

	if hits.Load() != tripAfter {
		t.Fatalf("sensor received %d requests, want %d", hits.Load(), tripAfter)
	}

	if _, err := client.Observations(ctx, "", 100); !errors.Is(err, ErrOpen) {
		t.Fatalf("open breaker returned %v", err)
	}

	healthy.Store(true)
	time.Sleep(probeDelay + 100*time.Millisecond)

	if _, err := client.Observations(ctx, "", 100); err != nil {
		t.Fatalf("probe failed after recovery: %v", err)
	}

	if _, err := client.Observations(ctx, "", 100); err != nil || hits.Load() != tripAfter+2 {
		t.Fatalf("closed breaker: error %v, requests %d", err, hits.Load())
	}
}

func TestBrokenBodyCountsAsFailure(t *testing.T) {
	var hits atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)

		_, _ = w.Write([]byte(`{"items":"none"}`))
	}))
	t.Cleanup(server.Close)

	client := NewClient(server.URL, "token", server.Client(), time.Second, func(string, string) {})

	var contract *ContractError
	if _, err := client.Observations(context.Background(), "", 100); !errors.As(err, &contract) || len(contract.Raw) == 0 {
		t.Fatalf("broken body returned %v", err)
	}

	for range 10 {
		_, _ = client.Observations(context.Background(), "", 100)
	}

	if hits.Load() != tripAfter {
		t.Fatalf("sensor received %d requests, want %d", hits.Load(), tripAfter)
	}
}
