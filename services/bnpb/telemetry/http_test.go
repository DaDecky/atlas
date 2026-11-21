package telemetry

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func TestInternalTracePreservesRequestAndDropsQuery(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))

	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	rt := Runtime{service: "test", tracer: provider, traceSDK: provider}

	dependency := httptest.NewServer(rt.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("cursor") != "private-cursor" || r.Header.Get("Authorization") != "Bearer private-token" {
			http.Error(w, "missing query or header", http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}), "/internal/hazards"))
	defer dependency.Close()

	base := http.DefaultTransport.(*http.Transport).Clone()

	client := &http.Client{Transport: rt.Transport(base)}
	defer client.CloseIdleConnections()

	edge := rt.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, dependency.URL+"/internal/hazards?cursor=private-cursor", http.NoBody)
		if err != nil {
			http.Error(w, "request creation failed", http.StatusInternalServerError)
			return
		}

		req.Header.Set("Authorization", "Bearer private-token")

		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, "dependency failed", http.StatusBadGateway)
			return
		}

		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
	}), "/hazards")

	w := httptest.NewRecorder()
	edge.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/hazards", http.NoBody))

	if w.Code != http.StatusNoContent {
		t.Fatalf("response status %d, want 204", w.Code)
	}

	spans := exporter.GetSpans()
	if len(spans) != 3 {
		t.Fatalf("got %d spans, want edge, outbound, and dependency", len(spans))
	}

	server, outbound, entry := spans[0], spans[1], spans[2]
	if !server.Parent.IsRemote() || server.SpanContext.TraceID() != entry.SpanContext.TraceID() ||
		server.Parent.SpanID() != outbound.SpanContext.SpanID() || outbound.Parent.SpanID() != entry.SpanContext.SpanID() {
		t.Fatal("dependency span was not linked to the edge request")
	}

	for _, span := range spans {
		for _, attr := range span.Attributes {
			if value := attr.Value.String(); strings.Contains(value, "private-cursor") || strings.Contains(value, "private-token") {
				t.Fatalf("private request data in %s", attr.Key)
			}
		}
	}
}

func TestServerMetricUsesApplicationRouteWithFallbackMux(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	rt := Runtime{service: "test", tracer: tracenoop.NewTracerProvider(), meter: provider, metricSDK: provider}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := rt.Handler(mux, "/internal/hazards")
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/internal/hazards", http.NoBody))

	var result metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &result); err != nil {
		t.Fatal(err)
	}

	for _, scope := range result.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "http.server.request.duration" {
				continue
			}

			histogram := m.Data.(metricdata.Histogram[float64])

			route, _ := histogram.DataPoints[0].Attributes.Value("http.route")
			if route.AsString() != "/internal/hazards" {
				t.Fatalf("got route %q", route.AsString())
			}

			return
		}
	}

	t.Fatal("missing server histogram")
}
