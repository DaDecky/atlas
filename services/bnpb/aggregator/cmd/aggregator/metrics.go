package main

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/metric"

	"atlas/services/bnpb/aggregator/internal/satellite"
)

func satelliteObserver(meter metric.Meter) (func(context.Context, satellite.CommitInfo), error) {
	offset, offsetErr := meter.Int64Gauge("atlas.satellite.committed.offset")
	watermark, watermarkErr := meter.Int64Gauge("atlas.satellite.watermark.timestamp", metric.WithUnit("s"))
	late, lateErr := meter.Int64Gauge("atlas.satellite.late.detections")

	latency, latencyErr := meter.Float64Histogram("atlas.satellite.ingest_to_store.duration", metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.025, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 2, 3, 5, 10, 30, 60))
	if err := errors.Join(offsetErr, watermarkErr, lateErr, latencyErr); err != nil {
		return nil, err
	}

	return func(ctx context.Context, info satellite.CommitInfo) {
		offset.Record(ctx, info.Stats.Offset)
		late.Record(ctx, info.Stats.LateCount)

		if !info.Watermark.IsZero() {
			watermark.Record(ctx, info.Watermark.Unix())
		}

		for _, sample := range info.Latencies {
			latency.Record(ctx, sample.Seconds())
		}
	}, nil
}
