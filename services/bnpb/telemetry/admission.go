package telemetry

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// AdmissionRejections observes requests rejected by the service's own capacity limit.
func (rt *Runtime) AdmissionRejections(log *slog.Logger) func(context.Context, bool) {
	meter := rt.Meter()
	if meter == nil {
		return nil
	}

	counter, err := meter.Int64Counter("atlas.admission.rejections")
	if err != nil {
		log.Warn("admission_metric_init_failed")
		return nil
	}

	media := metric.WithAttributes(attribute.String("client.class", "media"))
	priority := metric.WithAttributes(attribute.String("client.class", "priority"))

	return func(ctx context.Context, isMedia bool) {
		class := priority
		if isMedia {
			class = media
		}

		counter.Add(ctx, 1, class)
	}
}
