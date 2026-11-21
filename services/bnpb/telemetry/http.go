package telemetry

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type transport struct {
	http.RoundTripper
	base *http.Transport
}

type internalTransport struct {
	base *http.Transport
}

// Handler instruments known application routes. Probe traffic and arbitrary request paths are excluded.
func (rt *Runtime) Handler(next http.Handler, paths ...string) http.Handler {
	if rt.traceSDK == nil && rt.metricSDK == nil {
		return next
	}

	routes := make(map[string]bool, len(paths))
	for _, path := range paths {
		routes[path] = true
	}

	return otelhttp.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route := r.URL.Path
		trace.SpanFromContext(r.Context()).SetAttributes(attribute.String("http.route", route))
		next.ServeHTTP(w, r)

		// Use the known route even when an outer ServeMux sets its dispatch pattern to "/".
		r.Pattern = route
	}), rt.service,
		otelhttp.WithFilter(func(r *http.Request) bool { return routes[r.URL.Path] }),
		otelhttp.WithServerName(rt.service),
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			switch r.Method {
			case http.MethodGet, http.MethodPost, http.MethodHead:
				return r.Method + " " + r.URL.Path
			default:
				return r.URL.Path
			}
		}),
		otelhttp.WithPropagators(propagation.TraceContext{}),
		otelhttp.WithTracerProvider(rt.tracer),
		otelhttp.WithMeterProvider(rt.meter),
	)
}

func (t transport) CloseIdleConnections() {
	t.base.CloseIdleConnections()
}

func (t internalTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	// The application still forwards the full query. The span contains only the dependency address and route.
	u := *r.URL
	u.User, u.RawQuery, u.Fragment, u.RawFragment, u.ForceQuery = nil, "", "", "", false
	trace.SpanFromContext(r.Context()).SetAttributes(attribute.String("url.full", u.String()))

	return t.base.RoundTrip(r)
}

func (rt *Runtime) Transport(base *http.Transport) http.RoundTripper {
	if rt.traceSDK == nil && rt.metricSDK == nil {
		return base
	}

	instrumented := otelhttp.NewTransport(internalTransport{base: base},
		otelhttp.WithPropagators(propagation.TraceContext{}),
		otelhttp.WithTracerProvider(rt.tracer),
		otelhttp.WithMeterProvider(rt.meter),
	)

	return transport{RoundTripper: instrumented, base: base}
}
