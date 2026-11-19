package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"atlas/services/bnpb/aggregator/internal/aggregator"
	"atlas/services/bnpb/aggregator/internal/lease"
)

type sourceGuard func(context.Context) error

type sourceWork func(context.Context, int64, sourceGuard)

func sourceElection(log *slog.Logger) (*lease.Elector, error) {
	raw, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil || strings.TrimSpace(string(raw)) == "" {
		return nil, errors.New("source election requires a Kubernetes service account namespace")
	}

	timing := lease.Timing{Duration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 2 * time.Second}
	for _, setting := range []struct {
		name   string
		target *time.Duration
	}{
		{"SOURCE_LEASE_DURATION", &timing.Duration},
		{"SOURCE_LEASE_RENEW_DEADLINE", &timing.RenewDeadline},
		{"SOURCE_LEASE_RETRY_PERIOD", &timing.RetryPeriod},
	} {
		if value := os.Getenv(setting.name); value != "" {
			*setting.target, err = time.ParseDuration(value)
			if err != nil || *setting.target <= 0 {
				return nil, fmt.Errorf("invalid %s", setting.name)
			}
		}
	}

	return lease.NewInCluster(strings.TrimSpace(string(raw)), timing, log)
}

func runSource(ctx context.Context, election *lease.Elector, store *aggregator.Cluster, source string, work sourceWork, log *slog.Logger) {
	for ctx.Err() == nil {
		err := runSourceTerm(ctx, election, store, source, work, log)
		if err != nil && ctx.Err() == nil {
			log.Warn("source_term_ended", "source", source, "reason", err.Error())
		}

		if !waitSourceRetry(ctx) {
			return
		}
	}
}

func runSourceTerm(ctx context.Context, election *lease.Elector, store *aggregator.Cluster, source string, work sourceWork, log *slog.Logger) error {
	term, err := election.Lead(ctx, source)
	if err != nil {
		return err
	}
	defer term.Release()

	lifetime, cancel := context.WithCancel(ctx)
	defer cancel()

	guard := func(call context.Context) error {
		if err := lifetime.Err(); err != nil {
			return err
		}

		check, done := context.WithTimeout(call, 2*time.Second)
		defer done()

		if err := term.Check(check); err != nil {
			cancel()
			return err
		}

		return nil
	}

	bootstrap, stop := context.WithTimeout(lifetime, 8*time.Second)
	defer stop()

	if err = guard(bootstrap); err != nil {
		return err
	}

	epoch, err := store.AcquireWriterEpoch(bootstrap, source)
	if err != nil {
		return err
	}

	if err = guard(bootstrap); err != nil {
		return err
	}

	log.Info("source_writer_ready", "source", source, "epoch", epoch)

	done := make(chan struct{})
	go func() {
		defer close(done)

		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-lifetime.Done():
				return
			case <-ticker.C:
				if err := guard(lifetime); err != nil {
					return
				}
			}
		}
	}()

	work(lifetime, epoch, guard)
	cancel()
	<-done

	return nil
}

func waitSourceRetry(ctx context.Context) bool {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func sourcePoller(c aggregator.Config, source string, writer *aggregator.EpochIngest, guard sourceGuard, log *slog.Logger) (*aggregator.Poller, func()) {
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

	client := &http.Client{
		Transport:     transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	return &aggregator.Poller{
		Source: source, BaseURL: base, Credential: key, Interval: c.PollInterval,
		Timeout: c.SourceTimeout, Backoff: c.Backoff, Retries: c.SourceRetries,
		MaxRecords: c.MaxSourceRecords, MaxSourceBytes: c.MaxSourceBytes, WarningLookback: c.WarningLookback,
		FailureMarkTimeout: c.FailureMarkTimeout, Client: client, Store: writer,
		Log: log, PolicyFile: os.Getenv("SOURCE_SCHEMA_POLICY_FILE"), Guard: guard,
	}, transport.CloseIdleConnections
}

func replaySource(store runtimeData, writer *aggregator.EpochIngest, poller *aggregator.Poller, guard sourceGuard) aggregator.ReplaySource {
	return func(ctx context.Context, limit int, reason string) (int, error) {
		if guard != nil {
			if err := guard(ctx); err != nil {
				return 0, err
			}
		}

		return store.ReplayReason(ctx, writer.Source, writer.WriterEpoch, limit, reason, func(message aggregator.ParkedMessage) (aggregator.HazardEvent, error) {
			return poller.DecodeParked(ctx, message)
		})
	}
}

func retrySourceParking(ctx context.Context, store runtimeData, writer *aggregator.EpochIngest, poller *aggregator.Poller, log *slog.Logger) {
	aggregator.RunParkingRetries(ctx, store, writer.Source, writer.WriterEpoch,
		func(message aggregator.ParkedMessage) (aggregator.HazardEvent, error) {
			return poller.DecodeParked(ctx, message)
		},
		func(err error) { log.Warn("parking_retry_failed", "source", writer.Source, "reason", err.Error()) })
}

func sourcePollWork(c aggregator.Config, store runtimeData, source string, log *slog.Logger) sourceWork {
	return func(ctx context.Context, epoch int64, guard sourceGuard) {
		writer := &aggregator.EpochIngest{Data: store, Source: source, WriterEpoch: epoch}

		poller, closeIdle := sourcePoller(c, source, writer, guard, log)
		defer closeIdle()

		term, cancel := context.WithCancel(ctx)
		defer cancel()

		var workers sync.WaitGroup
		workers.Go(func() { retrySourceParking(term, store, writer, poller, log) })
		workers.Go(func() {
			queue := aggregator.ReplayRequests{URL: c.RabbitMQURL, Heartbeat: c.BrokerHeartbeat}

			apply := replaySource(store, writer, poller, guard)
			for term.Err() == nil {
				err := queue.Serve(term, source, apply)
				if errors.Is(err, aggregator.ErrStaleEpoch) {
					cancel()
					return
				}

				if err != nil && term.Err() == nil {
					log.Warn("source_replay_retry", "source", source, "reason", err.Error())
				}

				if !waitSourceRetry(term) {
					return
				}
			}
		})

		poller.Run(term)
		cancel()
		workers.Wait()
	}
}

func startSources(ctx context.Context, store runtimeData, c aggregator.Config, launch func(func(context.Context)), log *slog.Logger) (map[string]aggregator.ReplaySource, error) {
	replay := map[string]aggregator.ReplaySource{}
	if os.Getenv("INGESTION_DISABLED") == "1" {
		return replay, nil
	}

	sat, err := satelliteWork(store, log)
	if err != nil {
		return nil, err
	}

	streams := map[string]sourceWork{"SATELIT": sat, aggregator.SensorSource: sensorWork(c, store, log)}

	if cluster, ok := store.(*aggregator.Cluster); ok {
		election, err := sourceElection(log)
		if err != nil {
			return nil, err
		}

		for _, source := range []string{"BMKG", "PVMBG"} {
			work := sourcePollWork(c, store, source, log)

			launch(func(ctx context.Context) { runSource(ctx, election, cluster, source, work, log) })
		}

		for source, work := range streams {
			if work != nil {
				launch(func(ctx context.Context) { runSource(ctx, election, cluster, source, work, log) })
			}
		}

		return replay, nil
	}

	for _, source := range []string{"BMKG", "PVMBG"} {
		epoch, err := store.Epoch(ctx, source)
		if err != nil {
			return nil, err
		}

		writer := &aggregator.EpochIngest{Data: store, Source: source, WriterEpoch: epoch}
		poller, closeIdle := sourcePoller(c, source, writer, nil, log)
		replay[source] = replaySource(store, writer, poller, nil)

		launch(func(ctx context.Context) {
			defer closeIdle()

			poller.Run(ctx)
		})
		launch(func(ctx context.Context) { retrySourceParking(ctx, store, writer, poller, log) })
	}

	for source, work := range streams {
		if work == nil {
			continue
		}

		epoch, err := store.Epoch(ctx, source)
		if err != nil {
			return nil, err
		}

		launch(func(ctx context.Context) { work(ctx, epoch, nil) })
	}

	return replay, nil
}
