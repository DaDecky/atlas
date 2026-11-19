package clientapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"mime"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"
)

type FieldReport struct {
	ReportID   string    `json:"report_id"`
	ObservedAt time.Time `json:"observed_at"`
	Location   struct {
		Latitude  *float64 `json:"latitude"`
		Longitude *float64 `json:"longitude"`
		AreaName  string   `json:"area_name"`
	} `json:"location"`
	Category string `json:"category"`
	Message  string `json:"message"`
	ClientID string `json:"client_id"`
}

type ReportPublisher interface {
	PublishReport(context.Context, FieldReport, string) error
}

func decodeReport(raw []byte) (FieldReport, error) {
	var report FieldReport
	if !utf8.Valid(raw) {
		return report, errors.New("invalid UTF-8")
	}

	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()

	if err := d.Decode(&report); err != nil {
		return report, err
	}

	var extra any
	if d.Decode(&extra) != io.EOF || !correlationValid(report.ReportID) || report.ObservedAt.IsZero() ||
		report.Location.Latitude == nil || report.Location.Longitude == nil ||
		math.IsNaN(*report.Location.Latitude) || math.Abs(*report.Location.Latitude) > 90 || math.Abs(*report.Location.Longitude) > 180 ||
		strings.TrimSpace(report.Location.AreaName) == "" || len(report.Location.AreaName) > 256 ||
		strings.TrimSpace(report.Message) == "" || len(report.Message) > 4096 || report.ClientID != "" {
		return report, errors.New("invalid report fields")
	}

	switch report.Category {
	case "SEISMIC", "VOLCANIC", "WILDFIRE", "COASTAL":
	default:
		return report, errors.New("invalid report category")
	}

	return report, nil
}

// verifyRequest checks the bearer token locally, so it costs no call to Auth.
func (a *API) verifyRequest(w http.ResponseWriter, r *http.Request) (AccessClaims, bool) {
	token, ok := bearerValue(r.Header.Get("Authorization"))
	if !ok {
		writeError(w, 401, "unauthorized")
		return AccessClaims{}, false
	}

	claims, err := verifyAccessToken(token, a.Config.PublicKey, a.Config.Issuer, a.Config.Audience, time.Now())
	if err != nil {
		writeError(w, 401, "invalid_token")
		return claims, false
	}

	return claims, true
}

// permit checks the scope, then asks Auth whether the session is still current.
func (a *API) permit(w http.ResponseWriter, r *http.Request, claims AccessClaims, scope string) bool {
	if !hasScope(claims.Scope, scope) {
		writeError(w, 403, "insufficient_scope")
		return false
	}

	if status, code := a.validateCurrent(r.Context(), claims, r.Header.Get("X-Correlation-ID")); status != 200 {
		writeError(w, status, code)
		return false
	}

	return true
}

func (a *API) authorize(w http.ResponseWriter, r *http.Request, scope string) (AccessClaims, bool) {
	claims, ok := a.verifyRequest(w, r)
	if !ok {
		return claims, false
	}

	return claims, a.permit(w, r, claims, scope)
}

func (a *API) fieldReports(w http.ResponseWriter, r *http.Request) {
	claims, ok := a.verifyRequest(w, r)
	if !ok {
		return
	}

	if !a.admit(w, claims) {
		return
	}
	defer a.admission.release()

	if !a.permit(w, r, claims, "reports:write") {
		return
	}

	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		writeError(w, 415, "json_required")
		return
	}

	raw, err := io.ReadAll(io.LimitReader(r.Body, (16<<10)+1))
	if err != nil || len(raw) > 16<<10 {
		writeError(w, 413, "report_too_large")
		return
	}

	report, err := decodeReport(raw)
	if err != nil {
		writeError(w, 400, "invalid_report")
		return
	}

	report.ClientID = claims.ClientID
	if a.Reports == nil || a.Reports.PublishReport(r.Context(), report, r.Header.Get("X-Correlation-ID")) != nil {
		w.Header().Set("Retry-After", "1")
		writeError(w, 503, "report_unconfirmed")

		return
	}

	writeJSON(w, 202, map[string]string{"report_id": report.ReportID, "status": "accepted"})
}

func (a *API) ops(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.authorize(w, r, "ops:read"); !ok {
		return
	}

	target := a.Config.AggregatorURL + "/internal/ops/" + strings.TrimPrefix(r.URL.Path, "/internal-ops/")
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}

	var body io.Reader

	if r.Method == http.MethodPost {
		raw, err := io.ReadAll(io.LimitReader(r.Body, 4097))
		if err != nil || len(raw) > 4096 {
			writeError(w, 413, "request_too_large")
			return
		}

		body = bytes.NewReader(raw)
	}

	resp, err := a.outbound(r.Context(), r.Method, target, map[string]string{"Authorization": "Bearer " + a.Config.AggregatorToken, "Content-Type": "application/json"}, body, r.Header.Get("X-Correlation-ID"), "aggregator")
	if err != nil {
		writeError(w, 503, "ops_unavailable")
		return
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
	if err != nil || len(raw) > 8<<20 || !json.Valid(raw) {
		writeError(w, 502, "ops_invalid_response")
		return
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusServiceUnavailable {
		writeError(w, 503, "ops_unavailable")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(raw)
}
