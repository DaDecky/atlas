package aggregator

import (
	"context"
	_ "embed"
	"encoding/json"
	"net/http"
	"sort"
	"time"
)

type AnalyticsBucket struct {
	Day   string `json:"day"`
	Type  string `json:"type"`
	Count int    `json:"count"`
}

type AnalyticsSummary struct {
	Total            int               `json:"total"`
	Timeline         []AnalyticsBucket `json:"timeline"`
	Severity         map[string]int    `json:"severity"`
	Areas            map[string]int    `json:"areas"`
	Types            map[string]int    `json:"types"`
	Partial          bool              `json:"partial"`
	UnavailableUnits []string          `json:"unavailable_units"`
}

//go:embed analytics.html
var analyticsHTML string

func Analytics(ctx context.Context, s ReadStore, since, until *time.Time) (AnalyticsSummary, error) {
	result := AnalyticsSummary{Timeline: []AnalyticsBucket{}, Severity: map[string]int{}, Areas: map[string]int{}, Types: map[string]int{}, UnavailableUnits: []string{}}
	q := Query{Limit: 1000, Audit: true}
	buckets := map[string]int{}
	missing := map[string]bool{}

	for {
		page, err := s.Read(ctx, q)
		if err != nil {
			return result, err
		}

		for _, unit := range page.UnavailableUnits {
			missing[unit] = true
		}

		for _, event := range page.Data {
			if since != nil && event.OccurredAt.Before(*since) {
				continue
			}

			if until != nil && event.OccurredAt.After(*until) {
				continue
			}

			kind := event.HazardType
			if kind == "SEISMIC" {
				var tsunami bool

				_ = json.Unmarshal(event.Attributes["potential_tsunami"], &tsunami)
				if tsunami {
					kind = "TSUNAMI"
				}
			}

			result.Total++
			result.Types[kind]++
			result.Severity[event.Severity]++
			result.Areas[event.AreaName]++
			buckets[event.OccurredAt.UTC().Format("2006-01-02")+"|"+kind]++
		}

		if page.NextCursor == nil {
			break
		}

		q.Cursor = *page.NextCursor
	}

	keys := []string{}
	for key := range buckets {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for _, key := range keys {
		result.Timeline = append(result.Timeline, AnalyticsBucket{Day: key[:10], Type: key[11:], Count: buckets[key]})
	}

	for unit := range missing {
		result.UnavailableUnits = append(result.UnavailableUnits, unit)
	}

	sort.Strings(result.UnavailableUnits)
	result.Partial = len(missing) > 0

	return result, nil
}

func (a *API) analytics(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		auditError(w, 401, "unauthorized", "token tidak valid")
		return
	}

	var since, until *time.Time

	for key, values := range r.URL.Query() {
		if (key != "from" && key != "to") || len(values) != 1 {
			auditError(w, 400, "bad_request", "invalid parameter")
			return
		}

		value, err := time.Parse(time.RFC3339, values[0])
		if err != nil {
			auditError(w, 400, "bad_request", "use RFC3339 times")
			return
		}

		if key == "from" {
			since = &value
		} else {
			until = &value
		}
	}

	if since != nil && until != nil && since.After(*until) {
		auditError(w, 400, "bad_request", "from must precede to")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), a.Timeout)
	defer cancel()

	result, err := Analytics(ctx, a.Store, since, until)
	if err != nil {
		auditError(w, 503, "unavailable", "analytics unavailable")
		return
	}

	writeJSON(w, 200, result)
}

func (a *API) authorized(r *http.Request) bool { return bearerMatches(r, a.Token) }

func analyticsPage(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(analyticsHTML))
}
