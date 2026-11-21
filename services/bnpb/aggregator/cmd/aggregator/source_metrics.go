package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"atlas/services/bnpb/aggregator/internal/aggregator"
)

type sourceMetrics struct {
	pollDuration, fieldReportLag metric.Float64Histogram
	pollSuccess, lease, breaker  metric.Int64Gauge
	requests, staleWrites        metric.Int64Counter
	holder                       string
}

type sensorTransport struct {
	http.RoundTripper
	requests metric.Int64Counter
}

func newSourceMetrics(meter metric.Meter, log *slog.Logger) *sourceMetrics {
	if meter == nil {
		return nil
	}

	poll, pollErr := meter.Float64Histogram("atlas.source.poll.duration", metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(.01, .05, .1, .25, .5, 1, 2, 5, 10, 30))
	last, lastErr := meter.Int64Gauge("atlas.source.last.success.timestamp", metric.WithUnit("s"))
	holder, holderErr := meter.Int64Gauge("atlas.source.lease.active")
	breaker, breakerErr := meter.Int64Gauge("atlas.sensor.breaker.state")
	requests, requestsErr := meter.Int64Counter("atlas.sensor.requests")
	rejected, rejectedErr := meter.Int64Counter("atlas.store.stale.write.rejections")

	lag, lagErr := meter.Float64Histogram("atlas.consumer.delivery.lag", metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(.05, .1, .25, .5, 1, 2, 5, 10, 30, 60, 300, 3600))
	if errors.Join(pollErr, lastErr, holderErr, breakerErr, requestsErr, rejectedErr, lagErr) != nil {
		log.Warn("source_metric_init_failed")
		return nil
	}

	hostname, _ := os.Hostname()

	return &sourceMetrics{
		pollDuration: poll, pollSuccess: last, lease: holder, breaker: breaker,
		requests: requests, staleWrites: rejected, fieldReportLag: lag, holder: hostname,
	}
}

func (m *sourceMetrics) recordPoll(ctx context.Context, source string, elapsed time.Duration, err error) {
	outcome := "success"
	if err != nil {
		outcome = "failure"
	} else {
		m.pollSuccess.Record(ctx, time.Now().Unix(), metric.WithAttributes(attribute.String("source", source)))
	}

	m.pollDuration.Record(ctx, elapsed.Seconds(), metric.WithAttributes(attribute.String("source", source), attribute.String("outcome", outcome)))
}

func (m *sourceMetrics) recordLease(ctx context.Context, source string, active bool) {
	var value int64
	if active {
		value = 1
	} else if source == aggregator.SourceSensorPantai {
		m.breaker.Record(ctx, -1)
	}

	m.lease.Record(ctx, value, metric.WithAttributes(attribute.String("source", source), attribute.String("holder", m.holder)))
}

func (m *sourceMetrics) recordBreaker(ctx context.Context, state string) {
	var value int64

	switch state {
	case "half-open":
		value = 1
	case "open":
		value = 2
	}

	m.breaker.Record(ctx, value)
}

func (m *sourceMetrics) recordStaleWrite(ctx context.Context, source, node string) {
	m.staleWrites.Add(ctx, 1, metric.WithAttributes(attribute.String("source", source), attribute.String("store.node", node)))
}

func (m *sourceMetrics) recordFieldDelivery(ctx context.Context, publishedAt time.Time) {
	if !publishedAt.IsZero() {
		m.fieldReportLag.Record(ctx, max(0, time.Since(publishedAt).Seconds()), metric.WithAttributes(attribute.String("queue", aggregator.FieldReportsQueue)))
	}
}

func (t sensorTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.requests.Add(req.Context(), 1)
	return t.RoundTripper.RoundTrip(req)
}
