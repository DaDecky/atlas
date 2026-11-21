package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"atlas/services/bnpb/aggregator/internal/aggregator"
	"atlas/services/bnpb/aggregator/internal/placement"
)

type operationMetrics struct {
	outboxDepth, outboxOldest, outboxAvailable metric.Int64Gauge
	queueReady, queueConsumers, queueAvailable metric.Int64Gauge
	shardState, writerEpoch                    metric.Int64Gauge
	refill                                     metric.Float64Histogram
}

func newOperationMetrics(meter metric.Meter) (*operationMetrics, error) {
	depth, depthErr := meter.Int64Gauge("atlas.outbox.depth")
	oldest, oldestErr := meter.Int64Gauge("atlas.outbox.oldest.timestamp", metric.WithUnit("s"))
	outboxAvailable, outboxAvailableErr := meter.Int64Gauge("atlas.outbox.available")
	ready, readyErr := meter.Int64Gauge("atlas.broker.queue.ready")
	consumers, consumersErr := meter.Int64Gauge("atlas.broker.queue.consumers")
	queueAvailable, queueAvailableErr := meter.Int64Gauge("atlas.broker.queue.available")
	state, stateErr := meter.Int64Gauge("atlas.placement.copy.state")
	epoch, epochErr := meter.Int64Gauge("atlas.source.writer.epoch")

	refill, refillErr := meter.Float64Histogram("atlas.placement.refill.duration", metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(.1, .25, .5, 1, 2, 5, 10, 30, 60, 120))
	if err := errors.Join(depthErr, oldestErr, outboxAvailableErr, readyErr, consumersErr, queueAvailableErr, stateErr, epochErr, refillErr); err != nil {
		return nil, err
	}

	return &operationMetrics{
		outboxDepth: depth, outboxOldest: oldest, outboxAvailable: outboxAvailable,
		queueReady: ready, queueConsumers: consumers, queueAvailable: queueAvailable,
		shardState: state, writerEpoch: epoch, refill: refill,
	}, nil
}

func startOperationMetrics(store runtimeData, c aggregator.Config, launch launcher, meter metric.Meter, log *slog.Logger) {
	if meter == nil {
		return
	}

	m, err := newOperationMetrics(meter)
	if err != nil {
		log.Warn("operation_metric_init_failed")
		return
	}

	if cluster, ok := store.(*aggregator.Cluster); ok {
		cluster.OnRefill = m.recordRefill
	}

	launch(func(ctx context.Context) { m.sample(ctx, store, c) })
}

func (m *operationMetrics) sample(ctx context.Context, store runtimeData, c aggregator.Config) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for ctx.Err() == nil {
		switch data := store.(type) {
		case *aggregator.Cluster:
			call, cancel := context.WithTimeout(ctx, 2*time.Second)
			table, _, err := data.Placement.Load(call)

			cancel()

			if err == nil {
				m.recordPlacement(ctx, table)
			}

			for name, node := range data.Nodes {
				m.recordOutbox(ctx, name, node.Store)
			}
		case *aggregator.Store:
			m.recordOutbox(ctx, "local", data)
		}

		aggregator.ObserveQueues(ctx, c.RabbitMQURL, c.BrokerHeartbeat, func(name string, stats aggregator.QueueStats) {
			attrs := metric.WithAttributes(attribute.String("queue", name))
			if !stats.Available {
				m.queueAvailable.Record(ctx, 0, attrs)
				return
			}

			m.queueAvailable.Record(ctx, 1, attrs)
			m.queueReady.Record(ctx, int64(stats.Ready), attrs)
			m.queueConsumers.Record(ctx, int64(stats.Consumers), attrs)
		})

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *operationMetrics) recordOutbox(ctx context.Context, node string, store *aggregator.Store) {
	call, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
	defer cancel()

	count, oldest, err := store.OutboxStats(call)

	attrs := metric.WithAttributes(attribute.String("store.node", node))
	if err != nil {
		m.outboxAvailable.Record(ctx, 0, attrs)
		return
	}

	m.outboxAvailable.Record(ctx, 1, attrs)
	m.outboxDepth.Record(ctx, count, attrs)
	m.outboxOldest.Record(ctx, oldest, attrs)
}

func (m *operationMetrics) recordPlacement(ctx context.Context, table placement.Table) {
	for shard, copies := range table.Shards {
		// Publish absent (0), behind (1), refilling (2), and serving (3), including when ownership moves.
		states := make(map[string]int64, len(copies))
		for _, copy := range copies {
			switch copy.State {
			case placement.Behind:
				states[copy.Node] = 1
			case placement.Refilling:
				states[copy.Node] = 2
			case placement.Serving:
				states[copy.Node] = 3
			}
		}

		for _, node := range table.Nodes {
			m.shardState.Record(ctx, states[node], metric.WithAttributes(attribute.String("shard", shard), attribute.String("store.node", node)))
		}
	}

	for source, epoch := range table.WriterEpochs {
		m.writerEpoch.Record(ctx, epoch, metric.WithAttributes(attribute.String("source", source)))
	}
}

func (m *operationMetrics) recordRefill(ctx context.Context, shard, node string, elapsed time.Duration, err error) {
	outcome := "success"
	if err != nil {
		outcome = "failure"
	}

	m.refill.Record(ctx, elapsed.Seconds(), metric.WithAttributes(attribute.String("shard", shard),
		attribute.String("store.node", node), attribute.String("outcome", outcome)))
}
