package aggregator

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

// countRows is the count the dashboard made from rows before it moved to SQL. The test
// keeps it as the oracle, so the SQL and this rule must agree on the same data.
func countRows(t *testing.T, s ReadStore, since, until *time.Time) map[string]int {
	t.Helper()

	counts := map[string]int{}
	q := Query{Limit: 1000, Audit: true}

	for {
		page, err := s.Read(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}

		for _, event := range page.Data {
			if since != nil && event.OccurredAt.Before(*since) || until != nil && event.OccurredAt.After(*until) {
				continue
			}

			kind := event.HazardType

			var tsunami bool
			if kind == "SEISMIC" && json.Unmarshal(event.Attributes["potential_tsunami"], &tsunami) == nil && tsunami {
				kind = "TSUNAMI"
			}

			counts[event.OccurredAt.UTC().Format("2006-01-02")+"|"+kind+"|"+event.Severity+"|"+event.AreaName]++
		}

		if page.NextCursor == nil {
			return counts
		}

		q.Cursor = *page.NextCursor
	}
}

func countSQL(t *testing.T, s ReadStore, since, until *time.Time) map[string]int {
	t.Helper()

	rows, missing, err := s.AnalyticsCounts(context.Background(), since, until)
	if err != nil || len(missing) != 0 {
		t.Fatal(err, missing)
	}

	counts := map[string]int{}
	for _, row := range rows {
		counts[row.Day+"|"+row.Type+"|"+row.Severity+"|"+row.Area] += row.Count
	}

	return counts
}

func TestIntegrationAnalyticsCountsMatchEventRows(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	day := time.Date(2026, 11, 18, 23, 30, 0, 0, time.UTC)

	events := []HazardEvent{}

	for i, tsunami := range []string{`true`, `false`, `"true"`, ``} {
		h := contractEvent("BMKG", "quake-"+string(rune('a'+i)), day.Add(time.Duration(i)*time.Hour))
		h.AreaName, h.Severity = []string{"Aceh", "Aceh", "Banten", "Banten"}[i], []string{"AWAS", "WASPADA", "SIAGA", "WASPADA"}[i]

		if tsunami != "" {
			h.Attributes["potential_tsunami"] = json.RawMessage(tsunami)
		}

		events = append(events, h)
	}

	if _, err := s.Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Events: events, Now: day}); err != nil {
		t.Fatal(err)
	}

	volcanic := contractEvent("PVMBG", "merapi", day.Add(-48*time.Hour))
	if _, err := s.Upsert(ctx, WriteBatch{Source: "PVMBG", Epoch: 1, Events: []HazardEvent{volcanic}, Now: day}); err != nil {
		t.Fatal(err)
	}

	since, until := day.Add(-time.Hour), day.Add(90*time.Minute)

	for _, window := range [][2]*time.Time{{nil, nil}, {&since, &until}} {
		want, got := countRows(t, s, window[0], window[1]), countSQL(t, s, window[0], window[1])
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("window %v: rows %v, sql %v", window, want, got)
		}
	}
}
