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

	"atlas/services/bmkg/internal/bmkg"
)

func run(log *slog.Logger) error {
	cfg, err := bmkg.ConfigFromEnv()
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	startup, stop := context.WithTimeout(ctx, cfg.StartupTimeout)

	store, err := bmkg.OpenStore(startup, cfg.DatabaseURL, int32(cfg.DBPoolSize), cfg.DBConnectTimeout)
	if err == nil {
		err = bmkg.LoadSeed(startup, store, cfg.MinSeedRecords)
	}

	stop()

	if err != nil {
		return err
	}

	defer store.Close()
	go bmkg.RunGenerator(ctx, store, bmkg.NewGenerator(cfg.Seed, time.Now()), cfg.EventInterval, cfg.StoreTimeout, log)

	api := &bmkg.API{Store: store, APIKey: cfg.APIKey, Delay: cfg.Delay, Log: log}
	server := &http.Server{
		Addr:              cfg.Address,
		Handler:           api.Handler(),
		ReadHeaderTimeout: cfg.HTTPReadHeaderTimeout,
		ReadTimeout:       cfg.HTTPReadTimeout,
		WriteTimeout:      cfg.HTTPWriteTimeout,
		IdleTimeout:       cfg.HTTPIdleTimeout,
		MaxHeaderBytes:    cfg.HTTPMaxHeaderBytes,
	}

	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()

	log.Info("started",
		"address", cfg.Address,
		"delay_ms", cfg.Delay.Milliseconds(),
		"event_interval", cfg.EventInterval.String(),
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
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "bmkg")
	if err := run(log); err != nil {
		log.Error("stopped", "reason", err.Error())
		os.Exit(1)
	}
}
