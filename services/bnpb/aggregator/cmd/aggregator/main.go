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
	"atlas/services/bnpb/aggregator/internal/leadership"
)

type launcher func(func(context.Context))

func run(log *slog.Logger) error {
	c, err := aggregator.ConfigFromEnv()
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	store, err := openStartupData(ctx, c)
	if err != nil {
		return errors.New("canonical store startup failed")
	}
	defer store.Close()

	var wg sync.WaitGroup

	launch := func(f func(context.Context)) {
		wg.Go(func() { f(ctx) })
	}

	replay, err := startWorkers(ctx, store, c, launch, log)
	if err == nil {
		api := &aggregator.API{
			Store: store, Token: c.InternalToken, Timeout: c.QueryTimeout,
			Admission: aggregator.NewAdmission(c.QueryCapacity, c.FieldReservedPercent), Log: log,
		}
		err = serve(ctx, cancel, newServer(c, dataHandler(api, store, c, replay)), c, log)
	}

	cancel()
	wg.Wait()

	return err
}

// openStartupData gives the replicated store two minutes, since its first start creates 48 schemas per node.
func openStartupData(ctx context.Context, c aggregator.Config) (runtimeData, error) {
	timeout := c.StartupTimeout
	if os.Getenv("CLUSTER_DATA_CONFIG") != "" {
		timeout = 2 * time.Minute
	}

	startup, stop := context.WithTimeout(ctx, timeout)
	defer stop()

	return openData(startup, c)
}

// startWorkers starts the placement planner, the outbox relay, the field report consumer, and the sources.
func startWorkers(ctx context.Context, store runtimeData, c aggregator.Config, launch launcher, log *slog.Logger) (map[string]aggregator.ReplaySource, error) {
	// The planner and the sources share one elector, so the pod holds one Kubernetes client for leases.
	var leader leadership.Leader

	if cluster, ok := store.(*aggregator.Cluster); ok {
		election, err := sourceElection(c, log)
		if err != nil {
			return nil, err
		}

		leader = leaseLeader{election}

		launch(func(ctx context.Context) {
			cluster.RunRecovery(ctx, leader, func(err error) { log.Error("refill_failed", "reason", err.Error()) })
		})
	}

	publisher := &aggregator.RabbitPublisher{URL: c.RabbitMQURL, Timeout: c.PublishTimeout, Heartbeat: c.BrokerHeartbeat, Log: log}

	launch(func(ctx context.Context) {
		aggregator.RunRelay(ctx, store, publisher, 200*time.Millisecond, func(err error) { log.Error("outbox_relay_failed", "reason", err.Error()) })
	})
	launch((&aggregator.FieldReportsConsumer{URL: c.RabbitMQURL, Heartbeat: c.BrokerHeartbeat, Store: store, Log: log}).Run)

	return startSources(ctx, store, c, leader, launch, log)
}

func newServer(c aggregator.Config, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              c.Address,
		Handler:           handler,
		ReadHeaderTimeout: c.HTTPReadHeaderTimeout,
		ReadTimeout:       c.HTTPReadTimeout,
		WriteTimeout:      c.QueryTimeout + time.Second,
		IdleTimeout:       c.HTTPIdleTimeout,
		MaxHeaderBytes:    c.HTTPMaxHeaderBytes,
	}
}

// serve runs the server until a signal arrives or the server fails, then shuts it down.
func serve(ctx context.Context, cancel context.CancelFunc, server *http.Server, c aggregator.Config, log *slog.Logger) error {
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()

	log.Info("started", "address", c.Address)

	var err error

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

	return err
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "aggregator")
	if err := run(log); err != nil {
		log.Error("stopped", "reason", err.Error())
		os.Exit(1)
	}
}
