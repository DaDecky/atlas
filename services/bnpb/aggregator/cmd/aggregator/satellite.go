package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"atlas/services/bnpb/aggregator/internal/aggregator"
	"atlas/services/bnpb/aggregator/internal/leadership"
	"atlas/services/bnpb/aggregator/internal/satellite"
)

func satelliteWork(store runtimeData, log *slog.Logger) (leadership.Work, error) {
	token := os.Getenv("SATELLITE_TOKEN")
	if path := os.Getenv("SATELLITE_TOKEN_FILE"); path != "" {
		raw, err := os.ReadFile(path)
		if err != nil || len(raw) > 4096 {
			return nil, errors.New("satellite token file unavailable or oversized")
		}

		token = strings.TrimSpace(string(raw))
	}

	if token == "" {
		log.Info("satellite_disabled", "reason", "group token not configured")
		return nil, nil
	}

	ref, err := satellite.LoadReference(os.Getenv("PROVINCE_REFERENCE_FILE"))
	if err != nil {
		return nil, err
	}

	idle := 30 * time.Second
	if raw := os.Getenv("SATELLITE_IDLE_TIMEOUT"); raw != "" {
		idle, err = time.ParseDuration(raw)
		if err != nil || idle < time.Second || idle > 10*time.Minute {
			return nil, errors.New("invalid SATELLITE_IDLE_TIMEOUT")
		}
	}

	origin := os.Getenv("SATELLITE_BASE_URL")
	if origin == "" {
		origin = "https://satellite.naufarrel.dev"
	}

	return func(ctx context.Context, epoch int64, termGuard leadership.Guard) {
		sink := &aggregator.SatelliteSink{Data: store, Epoch: epoch, Reference: ref}
		guard := func(ctx context.Context) error {
			if termGuard != nil {
				if err := termGuard(ctx); err != nil {
					return err
				}
			}

			return sink.Guard(ctx)
		}

		log.Info("satellite_configured", "province_reference", ref.Version, "writer_epoch", epoch)

		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.MaxConnsPerHost, transport.MaxIdleConnsPerHost = 1, 1

		transport.ResponseHeaderTimeout = 5 * time.Second
		defer transport.CloseIdleConnections()

		runner := &satellite.Runner{
			Origin: origin, Token: token, Sink: sink, Guard: guard, IdleTimeout: idle,
			Client:  &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
			OnError: func(err error) { log.Warn("satellite_reconnect", "reason", err.Error()) },
			OnCommit: func(info satellite.CommitInfo) {
				latencies := make([]float64, len(info.Latencies))
				for i, value := range info.Latencies {
					latencies[i] = float64(value.Microseconds()) / 1000
				}

				log.Info("satellite_committed", "offset", info.Stats.Offset, "frames", info.Frames, "active_fires", info.Stats.Active, "singletons", info.Stats.Singletons,
					"late_detections", info.Stats.LateCount, "watermark", info.Watermark, "ingest_to_store_ms", latencies, "commit_ms", info.Duration.Milliseconds())
			},
		}
		if err := runner.Run(ctx); err != nil && ctx.Err() == nil {
			log.Error("satellite_stopped", "reason", err.Error())
		}
	}, nil
}
