package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"atlas/services/pvmbg/internal/pvmbg"
)

func run(log *slog.Logger) error {
	cfg, err := pvmbg.ConfigFromEnv()
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	startup, stop := context.WithTimeout(ctx, cfg.StartupTimeout)

	store, err := pvmbg.OpenStore(startup, cfg.DatabaseURL, int32(cfg.DBPoolSize), cfg.DBConnectTimeout)
	if err == nil {
		err = pvmbg.LoadSeed(startup, store, cfg.MinSeedRecords)
	}

	version := 1
	if err == nil {
		version, err = store.SchemaVersion(startup)
	}

	stop()

	if err != nil {
		return err
	}

	defer store.Close()

	controls := pvmbg.NewControls(version)
	go pvmbg.RunGenerator(ctx, store, controls, pvmbg.NewGenerator(cfg.Seed, time.Now()), cfg.EventInterval, cfg.StoreTimeout, log)

	api := &pvmbg.API{
		Store:    store,
		Controls: controls,
		Token:    cfg.Token,
		Delay:    cfg.RandomDelay(),
		MaxHang:  cfg.MaxHang,
		Log:      log,
	}
	server := &http.Server{
		Addr:              cfg.Address,
		Handler:           api.Handler(),
		ReadHeaderTimeout: cfg.HTTPReadHeaderTimeout,
		ReadTimeout:       cfg.HTTPReadTimeout,
		WriteTimeout:      cfg.MaxDelay + 5*time.Second,
		IdleTimeout:       cfg.HTTPIdleTimeout,
		MaxHeaderBytes:    cfg.HTTPMaxHeaderBytes,
	}

	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()

	log.Info("started",
		"address", cfg.Address,
		"min_delay_ms", cfg.MinDelay.Milliseconds(),
		"max_delay_ms", cfg.MaxDelay.Milliseconds(),
		"event_interval", cfg.EventInterval.String(),
		"schema_version", version,
	)

	select {
	case <-ctx.Done():
	case err = <-done:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	}

	shutdown, finish := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer finish()

	_ = server.Shutdown(shutdown)

	return err
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "pvmbg")
	if err := run(log); err != nil {
		log.Error("stopped", "reason", err.Error())
		os.Exit(1)
	}
}
