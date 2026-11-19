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
	"atlas/services/bnpb/aggregator/internal/satellite"
)

func startSatellite(ctx context.Context, store runtimeData, launch func(func(context.Context)), log *slog.Logger) error {
	if os.Getenv("M2_READ_ONLY") == "1" {
		return nil
	}

	token := os.Getenv("SATELLITE_TOKEN")
	if path := os.Getenv("SATELLITE_TOKEN_FILE"); path != "" {
		raw, err := os.ReadFile(path)
		if err != nil || len(raw) > 4096 {
			return errors.New("satellite token file unavailable or oversized")
		}

		token = strings.TrimSpace(string(raw))
	}

	if token == "" {
		log.Info("satellite_disabled", "reason", "group token not configured")
		return nil
	}

	if _, ok := store.(*aggregator.Store); !ok {
		return errors.New("cluster satellite ingestion requires the source leadership owner")
	}

	ref, err := satellite.LoadReference(os.Getenv("PROVINCE_REFERENCE_FILE"))
	if err != nil {
		return err
	}

	epoch, err := store.Epoch(ctx, "SATELIT")
	if err != nil {
		return err
	}

	idle := 30 * time.Second
	if raw := os.Getenv("SATELLITE_IDLE_TIMEOUT"); raw != "" {
		idle, err = time.ParseDuration(raw)
		if err != nil || idle < time.Second || idle > 10*time.Minute {
			return errors.New("invalid SATELLITE_IDLE_TIMEOUT")
		}
	}

	origin := os.Getenv("SATELLITE_BASE_URL")
	if origin == "" {
		origin = "https://satellite.naufarrel.dev"
	}

	sink := &aggregator.SatelliteSink{Data: store, Epoch: epoch, Reference: ref}

	launch(func(ctx context.Context) {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.MaxConnsPerHost, transport.MaxIdleConnsPerHost = 1, 1

		transport.ResponseHeaderTimeout = 5 * time.Second
		defer transport.CloseIdleConnections()

		runner := &satellite.Runner{
			Origin: origin, Token: token, Sink: sink, Guard: sink.Guard, IdleTimeout: idle,
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
	})
	log.Info("satellite_configured", "province_reference", ref.Version, "writer_epoch", epoch)

	return nil
}
