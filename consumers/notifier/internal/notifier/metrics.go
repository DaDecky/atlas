package notifier

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"time"
)

// Metrics are counted in process and exposed in the Prometheus text format, both on
// /metrics and, when METRICS_PUSH_URL is set, pushed to the VictoriaMetrics import API.
// The notifier has no HTTP traffic to instrument, so the alert path itself is measured.
type Metrics struct {
	delivered, duplicate, rejected atomic.Int64
	latencyMillis, latencyCount    atomic.Int64
	brokerConnected                atomic.Bool
}

var metrics = &Metrics{}

func (m *Metrics) delivery(latency time.Duration) {
	m.delivered.Add(1)
	m.latencyMillis.Add(max(latency, 0).Milliseconds())
	m.latencyCount.Add(1)
}

func (m *Metrics) text() []byte {
	connected := 0
	if m.brokerConnected.Load() {
		connected = 1
	}

	var b bytes.Buffer

	fmt.Fprintf(&b, "atlas_notifier_alerts_total{outcome=\"delivered\"} %d\n", m.delivered.Load())
	fmt.Fprintf(&b, "atlas_notifier_alerts_total{outcome=\"duplicate\"} %d\n", m.duplicate.Load())
	fmt.Fprintf(&b, "atlas_notifier_alerts_total{outcome=\"rejected\"} %d\n", m.rejected.Load())
	fmt.Fprintf(&b, "atlas_notifier_delivery_latency_seconds_sum %.3f\n", float64(m.latencyMillis.Load())/1000)
	fmt.Fprintf(&b, "atlas_notifier_delivery_latency_seconds_count %d\n", m.latencyCount.Load())
	fmt.Fprintf(&b, "atlas_notifier_broker_connected %d\n", connected)

	return b.Bytes()
}

func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write(m.text())
	})
}

// push sends the current values every interval until ctx ends. A failed push is logged
// once per outage, because the alert path must never wait on telemetry.
func (m *Metrics) push(ctx context.Context, target string, interval time.Duration, log *slog.Logger) {
	u, err := url.Parse(target)
	if err != nil {
		log.Warn("metrics_push_disabled", "reason", "invalid METRICS_PUSH_URL")
		return
	}

	host, _ := os.Hostname()
	q := u.Query()
	q.Add("extra_label", "service_name="+name)
	q.Add("extra_label", "instance="+host)
	u.RawQuery = q.Encode()

	client := &http.Client{Timeout: 2 * time.Second}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	failing := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		err := m.pushOnce(ctx, client, u.String())
		if err != nil && !failing {
			log.Warn("metrics_push_failed", "dependency", "victoriametrics", "reason", err.Error())
		}

		failing = err != nil
	}
}

func (m *Metrics) pushOnce(ctx context.Context, client *http.Client, target string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(m.text()))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "text/plain")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}

	return nil
}
