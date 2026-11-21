package main

import (
	"context"
	"log/slog"
	"net/http"

	"atlas/services/bnpb/aggregator/internal/aggregator"
	"atlas/services/bnpb/aggregator/internal/leadership"
	"atlas/services/bnpb/aggregator/internal/sensor"
)

// sensorWork gives the sensor its own transport with one connection, so a slow sensor
// cannot hold the connections or the deadline of another source.
func sensorWork(c aggregator.Config, store runtimeData, metrics *sourceMetrics, log *slog.Logger) leadership.Work {
	if c.SensorURL == "" {
		log.Info("sensor_disabled", "reason", "SENSOR_PANTAI_URL not configured")
		return nil
	}

	return func(ctx context.Context, epoch int64, guard leadership.Guard) {
		transport := &http.Transport{
			MaxConnsPerHost: 1, MaxIdleConnsPerHost: 1,
			IdleConnTimeout: c.SourceIdleConnTimeout, ResponseHeaderTimeout: c.SensorTimeout,
		}
		defer transport.CloseIdleConnections()

		onChange := func(from, to string) {
			log.Warn("sensor_breaker", "source", aggregator.SourceSensorPantai, "from", from, "to", to)

			if metrics != nil {
				metrics.recordBreaker(ctx, to)
			}
		}
		client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

		poller := &aggregator.SensorPoller{
			Client:   sensor.NewClient(c.SensorURL, c.SensorToken, client, c.SensorTimeout, onChange),
			Ingest:   &aggregator.EpochIngest{Data: store, Source: aggregator.SourceSensorPantai, WriterEpoch: epoch},
			Interval: c.SensorInterval, StaleAfter: c.SensorStaleAfter, Guard: guard, Log: log,
		}

		if metrics != nil {
			metrics.recordBreaker(ctx, "closed")
			client.Transport = sensorTransport{RoundTripper: transport, requests: metrics.requests}
			poller.OnPoll = metrics.recordPoll
		}

		poller.Run(ctx)
	}
}
