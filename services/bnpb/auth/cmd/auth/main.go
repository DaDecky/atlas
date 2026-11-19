package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"atlas/services/bnpb/auth/internal/auth"
)

func run(log *slog.Logger) error {
	cfg, err := auth.ConfigFromEnv()
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	startup, stop := context.WithTimeout(ctx, cfg.StartupTimeout)

	store, err := auth.OpenStore(startup, cfg.DatabaseURL, int32(cfg.DBPoolSize), cfg.DBConnectTimeout)

	stop()

	if err != nil {
		return err
	}

	defer store.Close()

	api := &auth.API{
		Store:         store,
		Clients:       cfg.Clients,
		PrivateKey:    cfg.PrivateKey,
		ValidateToken: cfg.ValidateToken,
		Issuer:        cfg.Issuer,
		Audience:      cfg.Audience,
		AccessTTL:     cfg.AccessTTL,
		RefreshTTL:    cfg.RefreshTTL,
		StoreTimeout:  cfg.StoreTimeout,
		MaxBodyBytes:  int64(cfg.MaxBodyBytes),
		Log:           log,
	}
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

	log.Info("started", "address", cfg.Address)

	select {
	case <-ctx.Done():
	case err = <-done:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}

		cancel()
	}

	shutdown, finish := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer finish()

	_ = server.Shutdown(shutdown)

	return err
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "auth")
	if err := run(log); err != nil {
		log.Error("stopped", "reason", err.Error())
		os.Exit(1)
	}
}
