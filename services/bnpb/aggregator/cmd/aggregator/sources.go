package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"atlas/services/bnpb/aggregator/internal/aggregator"
	"atlas/services/bnpb/aggregator/internal/leadership"
	"atlas/services/bnpb/aggregator/internal/lease"
)

// leaseLeader adapts the client-go lease elector to the leadership package.
type leaseLeader struct{ elector *lease.Elector }

func sourceElection(c aggregator.Config, log *slog.Logger) (*lease.Elector, error) {
	raw, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil || strings.TrimSpace(string(raw)) == "" {
		return nil, errors.New("source election requires a Kubernetes service account namespace")
	}

	timing := lease.Timing{Duration: c.SourceLeaseDuration, RenewDeadline: c.SourceLeaseRenewDeadline, RetryPeriod: c.SourceLeaseRetryPeriod}

	return lease.NewInCluster(strings.TrimSpace(string(raw)), timing, log)
}

func (l leaseLeader) Lead(ctx context.Context, source string) (leadership.Term, error) {
	term, err := l.elector.Lead(ctx, source)
	if err != nil {
		return nil, err
	}

	return term, nil
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

func sourcePoller(c aggregator.Config, source string, writer *aggregator.EpochIngest, guard leadership.Guard, log *slog.Logger) (*aggregator.Poller, func()) {
	origins := map[string][2]string{
		aggregator.SourceBMKG:  {c.BMKGURL, c.BMKGKey},
		aggregator.SourcePVMBG: {c.PVMBGURL, c.PVMBGToken},
	}
	base, key := origins[source][0], origins[source][1]

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

func replaySource(store runtimeData, writer *aggregator.EpochIngest, poller *aggregator.Poller, guard leadership.Guard) aggregator.ReplaySource {
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

func sourcePollWork(c aggregator.Config, store runtimeData, source string, log *slog.Logger) leadership.Work {
	return func(ctx context.Context, epoch int64, guard leadership.Guard) {
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

func startSources(ctx context.Context, store runtimeData, c aggregator.Config, launch launcher, log *slog.Logger) (map[string]aggregator.ReplaySource, error) {
	replay := map[string]aggregator.ReplaySource{}
	if os.Getenv("INGESTION_DISABLED") == "1" {
		return replay, nil
	}

	sat, err := satelliteWork(store, log)
	if err != nil {
		return nil, err
	}

	streams := map[string]leadership.Work{aggregator.SourceSatellite: sat, aggregator.SourceSensorPantai: sensorWork(c, store, log)}

	if cluster, ok := store.(*aggregator.Cluster); ok {
		election, err := sourceElection(c, log)
		if err != nil {
			return nil, err
		}

		runner := leadership.NewRunner(leaseLeader{election}, cluster, log)

		for _, source := range aggregator.PolledSources {
			work := sourcePollWork(c, store, source, log)

			launch(func(ctx context.Context) { runner.Run(ctx, source, work) })
		}

		for source, work := range streams {
			if work != nil {
				launch(func(ctx context.Context) { runner.Run(ctx, source, work) })
			}
		}

		return replay, nil
	}

	for _, source := range aggregator.PolledSources {
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
