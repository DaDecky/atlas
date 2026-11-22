package notifier

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAlertOutcomesArePushedAsMetrics(t *testing.T) {
	saved := metrics
	metrics = &Metrics{}

	t.Cleanup(func() { metrics = saved })

	b, state, log := &broker{}, newState(t), slog.New(slog.NewJSONHandler(io.Discard, nil))
	now := time.Now()

	for _, d := range []struct{ rev int64 }{{1}, {1}, {0}} {
		if err := handle(state, delivery(t, b, d.rev, now.Add(-1500*time.Millisecond)), now, log); err != nil {
			t.Fatal(err)
		}
	}

	got := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		select {
		case got <- r.URL.RawQuery + "\n" + string(body):
		default:
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go metrics.push(ctx, server.URL+"/api/v1/import/prometheus", 10*time.Millisecond, log)

	var pushed string
	select {
	case pushed = <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("metrics were not pushed")
	}

	for _, want := range []string{
		"extra_label=service_name%3Dnotifier",
		`atlas_notifier_alerts_total{outcome="delivered"} 1`,
		`atlas_notifier_alerts_total{outcome="duplicate"} 1`,
		`atlas_notifier_alerts_total{outcome="rejected"} 1`,
		"atlas_notifier_delivery_latency_seconds_sum 1.500",
		"atlas_notifier_delivery_latency_seconds_count 1",
	} {
		if !strings.Contains(pushed, want) {
			t.Fatalf("push lacks %q:\n%s", want, pushed)
		}
	}
}
