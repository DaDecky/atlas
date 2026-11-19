package clientapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
)

type API struct {
	Config        Config
	Client        *http.Client
	Log           *slog.Logger
	Reports       ReportPublisher
	admission     *admission
	admissionOnce sync.Once
}

type validateRequest struct {
	SessionID    string `json:"session_id"`
	TokenVersion int64  `json:"token_version"`
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

var summaryFields = map[string]bool{
	"hazard_id": true, "source": true, "hazard_type": true, "severity": true,
	"area_name": true, "occurred_at": true, "ingested_at": true,
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code}})
}

func bearerValue(header string) (string, bool) {
	token, ok := strings.CutPrefix(header, "Bearer ")
	return token, ok && token != "" && !strings.ContainsAny(token, " \t\r\n")
}

func correlationValid(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}

	for _, c := range id {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || strings.ContainsRune("-_.", c)) {
			return false
		}
	}

	return true
}

func newCorrelationID() string {
	var b [16]byte

	_, _ = rand.Read(b[:])

	return hex.EncodeToString(b[:])
}

func allowedFilters(raw string) (url.Values, string, error) {
	parsed, err := url.ParseQuery(raw)
	if err != nil {
		return nil, "", err
	}

	out := url.Values{}
	view := "summary"

	allowed := map[string]bool{
		"source":      true,
		"hazard_type": true,
		"severity":    true,
		"since":       true,
		"limit":       true,
		"view":        true,
		"cursor":      true,
	}
	for key, values := range parsed {
		if !allowed[key] || len(values) != 1 {
			return nil, "", errors.New("invalid filter")
		}

		value := values[0]

		switch key {
		case "source":
			if !validSource(value) {
				return nil, "", errors.New("invalid source")
			}
		case "hazard_type":
			if value != "SEISMIC" && value != "VOLCANIC" && value != "WILDFIRE" && value != "COASTAL" {
				return nil, "", errors.New("invalid hazard type")
			}
		case "severity":
			if value != "NORMAL" && value != "WASPADA" && value != "SIAGA" && value != "AWAS" {
				return nil, "", errors.New("invalid severity")
			}
		case "since":
			if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
				return nil, "", errors.New("invalid since")
			}
		case "limit":
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 || n > 500 {
				return nil, "", errors.New("invalid limit")
			}
		case "view":
			if value != "summary" && value != "raw" {
				return nil, "", errors.New("invalid view")
			}

			view = value

			continue
		case "cursor":
			if len(value) > 4096 {
				return nil, "", errors.New("invalid cursor")
			}
		}

		out.Set(key, value)
	}

	types := map[string]string{"BMKG": "SEISMIC", "PVMBG": "VOLCANIC", "SATELIT": "WILDFIRE", "SENSOR_PANTAI": "COASTAL"}
	if source, kind := out.Get("source"), out.Get("hazard_type"); source != "" && kind != "" && types[source] != kind {
		return nil, "", errors.New("contradictory filters")
	}

	return out, view, nil
}

func validSource(source string) bool {
	return source == "BMKG" || source == "PVMBG" || source == "SATELIT" || source == "SENSOR_PANTAI"
}

func (a *API) outbound(
	ctx context.Context,
	method, target string,
	headers map[string]string,
	body io.Reader,
	id, dependency string,
) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Correlation-ID", id)

	for key, value := range headers {
		req.Header.Set(key, value)
	}

	start := time.Now()
	resp, err := a.Client.Do(req)

	status := 0
	if resp != nil {
		status = resp.StatusCode
	}

	reason := ""
	if err != nil {
		reason = "network_error"
	}

	path := ""
	if parsed, parseErr := url.Parse(target); parseErr == nil {
		path = parsed.Path
	}

	a.Log.Info("outbound_call",
		"dependency", dependency,
		"correlation_id", id,
		"method", method,
		"path", path,
		"status", status,
		"latency_ms", time.Since(start).Milliseconds(),
		"reason", reason,
	)

	return resp, err
}

// readAggregator retries a failed read once. Dropping idle connections first makes the retry
// dial again, so the Service can route it to another instance.
func (a *API) readAggregator(ctx context.Context, target, class, id string) (*http.Response, error) {
	headers := map[string]string{"Authorization": "Bearer " + a.Config.AggregatorToken, "X-Request-Class": class}

	resp, err := a.outbound(ctx, http.MethodGet, target, headers, nil, id, "aggregator")
	if err == nil || ctx.Err() != nil {
		return resp, err
	}

	a.Client.CloseIdleConnections()

	return a.outbound(ctx, http.MethodGet, target, headers, nil, id, "aggregator")
}

func (a *API) validateCurrent(ctx context.Context, claims AccessClaims, id string) (int, string) {
	body, _ := json.Marshal(validateRequest{SessionID: claims.SessionID, TokenVersion: claims.TokenVersion})

	resp, err := a.outbound(ctx, http.MethodPost, a.Config.AuthURL+"/auth/validate", map[string]string{
		"Authorization": "Bearer " + a.Config.ValidateToken, "Content-Type": "application/json",
	}, strings.NewReader(string(body)), id, "auth")
	if err != nil {
		return http.StatusServiceUnavailable, "auth_unavailable"
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, int64(a.Config.MaxValidateBytes)+1))
	if err != nil || len(raw) > a.Config.MaxValidateBytes {
		return http.StatusServiceUnavailable, "auth_invalid_response"
	}

	if resp.StatusCode == http.StatusUnauthorized {
		var failure struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if json.Unmarshal(raw, &failure) == nil && failure.Error.Code == "inactive_session" {
			return http.StatusUnauthorized, "invalid_token"
		}

		return http.StatusServiceUnavailable, "auth_unavailable"
	}

	if resp.StatusCode != http.StatusOK {
		return http.StatusServiceUnavailable, "auth_unavailable"
	}

	var result struct {
		Active *bool `json:"active"`
	}
	if json.Unmarshal(raw, &result) != nil || result.Active == nil {
		return http.StatusServiceUnavailable, "auth_invalid_response"
	}

	if !*result.Active {
		return http.StatusUnauthorized, "invalid_token"
	}

	return http.StatusOK, ""
}

func summaryData(raw json.RawMessage) (json.RawMessage, error) {
	var events []json.RawMessage
	if err := json.Unmarshal(raw, &events); err != nil {
		return nil, err
	}

	filtered := make([]map[string]json.RawMessage, 0, len(events))
	for _, eventRaw := range events {
		var event map[string]json.RawMessage
		if err := json.Unmarshal(eventRaw, &event); err != nil {
			return nil, err
		}

		if event == nil {
			return nil, errors.New("invalid event")
		}

		out := make(map[string]json.RawMessage, len(summaryFields))
		for key, value := range event {
			if summaryFields[key] {
				var text string
				if string(value) == "null" || json.Unmarshal(value, &text) != nil {
					return nil, errors.New("invalid summary field")
				}

				out[key] = value
			}
		}

		filtered = append(filtered, out)
	}

	return json.Marshal(filtered)
}

// publicEnvelope excludes unknown fields and dependency diagnostics from public responses.
func publicEnvelope(body []byte, status int, summary bool) ([]byte, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil || envelope == nil {
		return nil, errors.New("invalid envelope")
	}

	data, hasData := envelope["data"]

	sources, hasSources := envelope["sources"]
	if !hasData && !hasSources && status == http.StatusServiceUnavailable {
		return json.Marshal(map[string]any{"error": map[string]string{"code": "aggregator_unavailable"}})
	}

	if !hasData || !hasSources || string(data) == "null" || string(sources) == "null" {
		return nil, errors.New("incomplete envelope")
	}

	var events []map[string]json.RawMessage
	if err := json.Unmarshal(data, &events); err != nil {
		return nil, err
	}

	if summary {
		filtered, err := summaryData(data)
		if err != nil {
			return nil, err
		}

		data = filtered
	}

	var freshness []map[string]json.RawMessage
	if err := json.Unmarshal(sources, &freshness); err != nil {
		return nil, err
	}

	for _, source := range freshness {
		var name, state string
		if json.Unmarshal(source["source"], &name) != nil || !validSource(name) ||
			json.Unmarshal(source["status"], &state) != nil ||
			(state != "healthy" && state != "stale" && state != "unavailable") {
			return nil, errors.New("invalid freshness")
		}

		for key, value := range source {
			switch key {
			case "source", "status":
			case "last_success_at", "stale_since":
				var timestamp *time.Time
				if json.Unmarshal(value, &timestamp) != nil {
					return nil, errors.New("invalid freshness time")
				}
			default:
				delete(source, key)
			}
		}
	}

	out := map[string]any{"data": data, "sources": freshness}

	for _, key := range []string{"partial", "unavailable_units", "next_cursor"} {
		raw, exists := envelope[key]
		if !exists {
			continue
		}

		switch key {
		case "partial":
			var value bool
			if string(raw) == "null" || json.Unmarshal(raw, &value) != nil {
				return nil, errors.New("invalid partial marker")
			}
		case "unavailable_units":
			var value []string
			if json.Unmarshal(raw, &value) != nil || len(value) > 128 {
				return nil, errors.New("invalid unavailable units")
			}
		case "next_cursor":
			var value *string
			if json.Unmarshal(raw, &value) != nil || value != nil && len(*value) > 4096 {
				return nil, errors.New("invalid next cursor")
			}
		}

		out[key] = raw
	}

	return json.Marshal(out)
}

func retryAfter(raw string, max time.Duration) string {
	if seconds, err := strconv.Atoi(raw); err == nil && seconds > 0 && time.Duration(seconds)*time.Second <= max {
		return strconv.Itoa(seconds)
	}

	now := time.Now()
	if date, err := http.ParseTime(raw); err == nil && date.After(now) && date.Before(now.Add(max)) {
		return date.UTC().Format(http.TimeFormat)
	}

	return "1"
}

func (a *API) hazards(w http.ResponseWriter, r *http.Request) {
	claims, ok := a.verifyRequest(w, r)
	if !ok {
		return
	}

	if !a.admit(w, claims) {
		return
	}
	defer a.admission.release()

	status, code := a.validateCurrent(r.Context(), claims, r.Header.Get("X-Correlation-ID"))
	if status != http.StatusOK {
		writeError(w, status, code)
		return
	}

	filters, view, err := allowedFilters(r.URL.RawQuery)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query")
		return
	}

	if view == "raw" && !hasScope(claims.Scope, "hazards:raw") {
		writeError(w, http.StatusForbidden, "insufficient_scope")
		return
	}

	target := a.Config.AggregatorURL + "/internal/hazards"
	if encoded := filters.Encode(); encoded != "" {
		target += "?" + encoded
	}

	resp, err := a.readAggregator(r.Context(), target, requestClass(claims), r.Header.Get("X-Correlation-ID"))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "aggregator_unavailable")
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		w.Header().Set("Retry-After", retryAfter(resp.Header.Get("Retry-After"), a.Config.MaxRetryAfter))
		writeError(w, http.StatusTooManyRequests, "capacity_exceeded")

		return
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(a.Config.MaxResponseBytes)+1))
	if err != nil || len(body) > a.Config.MaxResponseBytes {
		writeError(w, http.StatusBadGateway, "aggregator_invalid_response")
		return
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusServiceUnavailable {
		if resp.StatusCode >= 500 {
			writeError(w, http.StatusServiceUnavailable, "aggregator_unavailable")
		} else {
			writeError(w, http.StatusBadGateway, "aggregator_error")
		}

		return
	}

	body, err = publicEnvelope(body, resp.StatusCode, view == "summary")
	if err != nil {
		writeError(w, http.StatusBadGateway, "aggregator_invalid_response")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(append(body, '\n'))
}

func (a *API) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// The admission is shared across Handler calls, so tests that build several handlers see one limit.
func (a *API) Handler() http.Handler {
	a.admissionOnce.Do(func() { a.admission = newAdmission(a.Config.RequestCapacity, a.Config.FieldReservedPercent) })

	mux := chi.NewRouter()
	mux.Use(a.logRequest)
	mux.NotFound(func(w http.ResponseWriter, _ *http.Request) { writeError(w, http.StatusNotFound, "not_found") })
	mux.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
	})
	mux.Get("/hazards", a.hazards)
	mux.Post("/field-reports", a.fieldReports)
	mux.Get("/internal-ops/unprocessed", a.ops)
	mux.Post("/internal-ops/replay", a.ops)
	mux.Get("/health", a.health)

	return mux
}

func (a *API) logRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Correlation-ID")
		if !correlationValid(id) {
			id = newCorrelationID()
		}

		r.Header.Set("X-Correlation-ID", id)
		w.Header().Set("X-Correlation-ID", id)

		start := time.Now()
		logged := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(logged, r)

		a.Log.Info("request",
			"correlation_id", id,
			"method", r.Method,
			"path", r.URL.Path,
			"status", logged.status,
			"latency_ms", time.Since(start).Milliseconds(),
		)
	})
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
