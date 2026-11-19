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

	startupTimeout := c.StartupTimeout
	if os.Getenv("M2_DATA_CONFIG") != "" {
		startupTimeout = 2 * time.Minute
	}

	startup, stop := context.WithTimeout(ctx, startupTimeout)

	store, err := openData(startup, c)

	stop()

	if err != nil {
		return errors.New("canonical store startup failed")
	}

	defer store.Close()

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
		aggregator.RunRelay(ctx, store, &aggregator.RabbitPublisher{URL: c.RabbitMQURL, Timeout: c.PublishTimeout, Heartbeat: c.BrokerHeartbeat, Log: log}, 200*time.Millisecond, func(err error) { log.Error("outbox_relay_failed", "reason", err.Error()) })
	})
	launch((&aggregator.FieldReportsConsumer{URL: c.RabbitMQURL, Store: store, Log: log}).Run)

	replay := map[string]aggregator.ReplaySource{}

	sources := []string{"BMKG", "PVMBG"}
	if os.Getenv("M2_READ_ONLY") == "1" {
		sources = nil
	}

	for _, source := range sources {
		ingest, err := ingestData(ctx, store, source)
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
			IdleConnTimeout:       c.SourceIdleConnTimeout,
			ResponseHeaderTimeout: c.SourceTimeout,
		}
		defer transport.CloseIdleConnections()

		client := &http.Client{
			Transport:     transport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}

		poller := &aggregator.Poller{
			Source:             source,
			BaseURL:            base,
			Credential:         key,
			Interval:           c.PollInterval,
			Timeout:            c.SourceTimeout,
			Backoff:            c.Backoff,
			Retries:            c.SourceRetries,
			MaxRecords:         c.MaxSourceRecords,
			MaxSourceBytes:     c.MaxSourceBytes,
			WarningLookback:    c.WarningLookback,
			FailureMarkTimeout: c.FailureMarkTimeout,
			Client:             client,
			Store:              ingest,
			Log:                log,
			PolicyFile:         os.Getenv("SOURCE_SCHEMA_POLICY_FILE"),
		}
		if writer, ok := ingest.(*aggregator.EpochIngest); ok {
			replay[source] = func(ctx context.Context, limit int, reason string) (int, error) {
				return store.ReplayReason(ctx, source, writer.WriterEpoch, limit, reason, func(message aggregator.ParkedMessage) (aggregator.HazardEvent, error) {
					return poller.DecodeParked(ctx, message)
				})
			}
			launch(func(ctx context.Context) {
				aggregator.RunParkingRetries(ctx, store, source, writer.WriterEpoch,
					func(message aggregator.ParkedMessage) (aggregator.HazardEvent, error) {
						return poller.DecodeParked(ctx, message)
					},
					func(err error) { log.Warn("parking_retry_failed", "source", source, "reason", err.Error()) })
			})
		}

		launch(poller.Run)
	}

	if err = startSatellite(ctx, store, launch, log); err != nil {
		cancel()
		wg.Wait()

		return err
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
		Handler:           dataHandler(api, store, c, replay),
		ReadHeaderTimeout: c.HTTPReadHeaderTimeout,
		ReadTimeout:       c.HTTPReadTimeout,
		WriteTimeout:      c.QueryTimeout + time.Second,
		IdleTimeout:       c.HTTPIdleTimeout,
		MaxHeaderBytes:    c.HTTPMaxHeaderBytes,
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

	shutdown, finish := context.WithTimeout(context.Background(), c.ShutdownTimeout)
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
