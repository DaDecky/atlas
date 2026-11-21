package aggregator

import (
	"context"
	_ "embed"
	"fmt"
	"net/http"
	"sort"
	"time"
)

// AnalyticsCount is the number of events of one day, type, severity, and area.
type AnalyticsCount struct {
	Day, Type, Severity, Area string
	Count                     int
}

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

// The tsunami split matches a JSON true only, as the stored attribute is a boolean.
const analyticsQuery = `SELECT to_char(occurred_at AT TIME ZONE 'UTC', 'YYYY-MM-DD'),
 CASE WHEN hazard_type = 'SEISMIC' AND attributes->'potential_tsunami' = 'true'::jsonb THEN 'TSUNAMI' ELSE hazard_type END,
 severity, area_name, count(*)
 FROM hazard_events
 WHERE ($1::timestamptz IS NULL OR occurred_at >= $1) AND ($2::timestamptz IS NULL OR occurred_at <= $2)
 GROUP BY 1, 2, 3, 4`

//go:embed analytics.html
var analyticsHTML string

func Analytics(ctx context.Context, s ReadStore, since, until *time.Time) (AnalyticsSummary, error) {
	result := AnalyticsSummary{Timeline: []AnalyticsBucket{}, Severity: map[string]int{}, Areas: map[string]int{}, Types: map[string]int{}, UnavailableUnits: []string{}}

	counts, missing, err := s.AnalyticsCounts(ctx, since, until)
	if err != nil {
		return result, err
	}

	buckets := map[string]int{}

	for _, count := range counts {
		result.Total += count.Count
		result.Types[count.Type] += count.Count
		result.Severity[count.Severity] += count.Count
		result.Areas[count.Area] += count.Count
		buckets[count.Day+"|"+count.Type] += count.Count
	}

	keys := []string{}
	for key := range buckets {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for _, key := range keys {
		result.Timeline = append(result.Timeline, AnalyticsBucket{Day: key[:10], Type: key[11:], Count: buckets[key]})
	}

	result.UnavailableUnits = append(result.UnavailableUnits, missing...)
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

func (s *Store) AnalyticsCounts(ctx context.Context, since, until *time.Time) ([]AnalyticsCount, []string, error) {
	counts, err := s.countEvents(ctx, since, until)

	return counts, nil, err
}

// AnalyticsCounts asks every shard for its counts, so no event leaves its store to be counted.
func (c *Cluster) AnalyticsCounts(ctx context.Context, since, until *time.Time) ([]AnalyticsCount, []string, error) {
	return fanout(ctx, c, func(ctx context.Context, s *Store) ([]AnalyticsCount, error) {
		return s.countEvents(ctx, since, until)
	})
}

func (s *Store) countEvents(ctx context.Context, since, until *time.Time) ([]AnalyticsCount, error) {
	tx, err := s.begin(ctx, true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, analyticsQuery, since, until)
	if err != nil {
		return nil, fmt.Errorf("count events: %w", err)
	}
	defer rows.Close()

	counts := []AnalyticsCount{}

	for rows.Next() {
		var count AnalyticsCount
		if err = rows.Scan(&count.Day, &count.Type, &count.Severity, &count.Area, &count.Count); err != nil {
			return nil, err
		}

		counts = append(counts, count)
	}

	return counts, rows.Err()
}
