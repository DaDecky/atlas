package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/metric"
)

type satelliteSession struct {
	Offset     *int64 `json:"latest_offset"`
	VirtualNow string `json:"virtual_now"`
}

// sampleSatelliteSession measures the upstream head using a connection separate from the SSE stream.
func sampleSatelliteSession(ctx context.Context, origin, token string, meter metric.Meter, log *slog.Logger) {
	head, headErr := meter.Int64Gauge("atlas.satellite.latest.offset")
	virtualNow, timeErr := meter.Int64Gauge("atlas.satellite.feed.timestamp", metric.WithUnit("s"))

	available, availableErr := meter.Int64Gauge("atlas.satellite.session.available")
	if errors.Join(headErr, timeErr, availableErr) != nil {
		log.Warn("satellite_session_metric_init_failed")
		return
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()

	transport.MaxConnsPerHost, transport.MaxIdleConnsPerHost = 1, 1
	defer transport.CloseIdleConnections()

	client := &http.Client{
		Transport: transport, Timeout: 2 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	for ctx.Err() == nil {
		session, err := fetchSatelliteSession(ctx, client, origin, token)
		if ctx.Err() != nil {
			return
		}

		interval := 5 * time.Second

		if err == nil {
			available.Record(ctx, 1)
			head.Record(ctx, *session.Offset)

			if at, parseErr := time.Parse(time.RFC3339Nano, session.VirtualNow); parseErr == nil {
				virtualNow.Record(ctx, at.Unix())
			}
		} else {
			available.Record(ctx, 0)

			interval = 30 * time.Second
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func fetchSatelliteSession(ctx context.Context, client *http.Client, origin, token string) (satelliteSession, error) {
	var session satelliteSession

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/v1/session", http.NoBody)
	if err != nil {
		return session, err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("User-Agent", "Atlas-IF4031/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return session, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return session, errors.New("satellite session unavailable")
	}

	const maxSessionBytes = 8 << 10

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxSessionBytes+1))
	if err != nil || len(raw) > maxSessionBytes || json.Unmarshal(raw, &session) != nil ||
		session.Offset == nil || *session.Offset < 0 {
		return session, errors.New("invalid satellite session response")
	}

	return session, nil
}
