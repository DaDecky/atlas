package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"atlas/services/bnpb/aggregator/internal/aggregator"
)

func run(log *slog.Logger) error {
	c, err := aggregator.ConfigFromEnv()
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	startup, stop := context.WithTimeout(ctx, 15*time.Second)

	store, err := openData(startup, c)

	stop()

	if err != nil {
		return errors.New("canonical store startup failed")
	}

	defer store.Close()

	pending := make(chan aggregator.Publication, c.PublishCapacity)

	var wg sync.WaitGroup

	launch := func(f func(context.Context)) {
		wg.Add(1)

		go func() {
			defer wg.Done()

			f(ctx)
		}()
	}

	if cluster, ok := store.(*aggregator.Cluster); ok {
		lease, err := cluster.PlacementLease()
		if err != nil {
			return err
		}

		launch(func(ctx context.Context) {
			cluster.RunRecovery(ctx, lease, func(err error) { log.Error("refill_failed", "reason", err.Error()) })
		})
	}

	launch(func(ctx context.Context) {
		aggregator.RunRelay(ctx, store, &aggregator.RabbitPublisher{URL: c.RabbitMQURL, Timeout: c.PublishTimeout}, 200*time.Millisecond, func(err error) { log.Error("outbox_relay_failed", "reason", err.Error()) })
	})

	sources := []string{"BMKG", "PVMBG"}
	if os.Getenv("M2_READ_ONLY") == "1" {
		sources = nil
	}

	for _, source := range sources {
		ingest, err := ingestData(store, source)
		if err != nil {
			cancel()
			wg.Wait()

			return err
		}

		base, key := c.BMKGURL, c.BMKGKey
		if source == "PVMBG" {
			base, key = c.PVMBGURL, c.PVMBGToken
		}

		transport := &http.Transport{
			MaxConnsPerHost:       1,
			MaxIdleConnsPerHost:   1,
			IdleConnTimeout:       30 * time.Second,
			ResponseHeaderTimeout: c.SourceTimeout,
		}
		defer transport.CloseIdleConnections()

		client := &http.Client{
			Transport:     transport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
		poller := &aggregator.Poller{
			Source:     source,
			BaseURL:    base,
			Credential: key,
			Interval:   c.PollInterval,
			Timeout:    c.SourceTimeout,
			Backoff:    c.Backoff,
			Retries:    c.SourceRetries,
			Client:     client,
			Store:      ingest,
			Pending:    pending,
			Log:        log,
		}
		launch(poller.Run)
	}

	api := &aggregator.API{
		Store:    store,
		Token:    c.InternalToken,
		Timeout:  c.QueryTimeout,
		Capacity: make(chan struct{}, c.QueryCapacity),
		Log:      log,
	}
	server := &http.Server{
		Addr:              c.Address,
		Handler:           dataHandler(api, store, c),
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      c.QueryTimeout + time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()

	log.Info("started", "address", c.Address)

	select {
	case <-ctx.Done():
	case err = <-done:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}

		cancel()
	}

	shutdown, finish := context.WithTimeout(context.Background(), 5*time.Second)
	defer finish()

	_ = server.Shutdown(shutdown)

	cancel()
	wg.Wait()

	return err
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "aggregator")
	if err := run(log); err != nil {
		log.Error("stopped", "reason", err.Error())
		os.Exit(1)
	}
}
