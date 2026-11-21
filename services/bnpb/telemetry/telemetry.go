package telemetry

import (
	"context"
	"crypto/rand"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

type Runtime struct {
	service   string
	tracer    trace.TracerProvider
	meter     metric.MeterProvider
	traceSDK  *sdktrace.TracerProvider
	metricSDK *sdkmetric.MeterProvider
}

// Start enables each exporter when its OTLP/HTTP endpoint is configured.
func Start(ctx context.Context, service string, log *slog.Logger) *Runtime {
	rt := &Runtime{service: service, tracer: tracenoop.NewTracerProvider(), meter: metricnoop.NewMeterProvider()}
	traces := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")

	metrics := os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
	if traces == "" && metrics == "" {
		return rt
	}

	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(error) {
		log.Warn("telemetry_export_failed")
	}))

	instance := rand.Text()
	res := resource.NewSchemaless(attribute.String("service.name", service), attribute.String("service.instance.id", instance))

	if traces != "" {
		exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithTimeout(2*time.Second))
		if err != nil {
			log.Warn("telemetry_trace_init_failed")
		} else {
			rt.traceSDK = sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithBatcher(exporter,
				sdktrace.WithMaxQueueSize(512), sdktrace.WithMaxExportBatchSize(128), sdktrace.WithExportTimeout(2*time.Second)))
			rt.tracer = rt.traceSDK
		}
	}

	if metrics != "" {
		exporter, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithTimeout(2*time.Second))
		if err != nil {
			log.Warn("telemetry_metric_init_failed")
		} else {
			reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(5*time.Second), sdkmetric.WithTimeout(2*time.Second))
			rt.metricSDK = sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(reader))
			rt.meter = rt.metricSDK
		}
	}

	return rt
}

func (rt *Runtime) Close(log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	if rt.traceSDK != nil {
		if err := rt.traceSDK.Shutdown(ctx); err != nil {
			log.Warn("telemetry_trace_shutdown_failed")
		}
	}

	if rt.metricSDK != nil {
		if err := rt.metricSDK.Shutdown(ctx); err != nil {
			log.Warn("telemetry_metric_shutdown_failed")
		}
	}
}

func (rt *Runtime) Meter() metric.Meter {
	if rt.metricSDK == nil {
		return nil
	}

	return rt.meter.Meter("atlas")
}
