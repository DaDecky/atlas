package aggregator

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"atlas/services/bnpb/aggregator/internal/sensor"
)

const sensorStations = `{"items":[{"station_id":"stn-1","name":"Padang","province":"Sumatera Barat","latitude":-0.9,"longitude":100.3}]}`

// fakeSensorPages answers each cursor with its page and records the cursors it was asked for.
func fakeSensorPages(t *testing.T, pages map[string]string) (*sensor.Client, func() []string) {
	t.Helper()

	var (
		mu      sync.Mutex
		cursors []string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/stations" {
			_, _ = w.Write([]byte(sensorStations))
			return
		}

		cursor := r.URL.Query().Get("cursor")

		mu.Lock()

		cursors = append(cursors, cursor)

		mu.Unlock()

		_, _ = w.Write([]byte(pages[cursor]))
	}))
	t.Cleanup(server.Close)

	client := sensor.NewClient(server.URL, "token", server.Client(), time.Second, func(string, string) {})

	return client, func() []string {
		mu.Lock()
		defer mu.Unlock()

		return append([]string(nil), cursors...)
	}
}

func sensorRecord(id, station, observed string, critical bool) string {
	flag := "false"
	if critical {
		flag = "true"
	}

	return `{"observation_id":"` + id + `","station_id":"` + station + `","observed_at":"` + observed +
		`","sea_level_anomaly_m":0.2,"wave_height_m":1.0,"quality":"ok","critical":` + flag + `}`
}

func TestIntegrationSensorStoresParksAndResumes(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	generated := time.Now().UTC().Truncate(time.Second)
	at := func(age time.Duration) string { return generated.Add(-age).Format(time.RFC3339) }

	first := `{"generated_at":"` + generated.Format(time.RFC3339) + `","items":[` + strings.Join([]string{
		sensorRecord("obs-1", "stn-1", at(3*time.Second), true),
		`{"observation_id":"obs-2","station_id":"stn-1"}`,
		sensorRecord("obs-3", "stn-unknown", at(2*time.Second), false),
	}, ",") + `],"next_cursor":"c1"}`
	second := `{"generated_at":"` + generated.Format(time.RFC3339) + `","items":[` +
		sensorRecord("obs-4", "stn-1", at(time.Second), false) + `],"next_cursor":"c2"}`

	client, cursors := fakeSensorPages(t, map[string]string{"": first, "c1": second})
	poller := func() *SensorPoller {
		return &SensorPoller{Client: client, Ingest: &EpochIngest{Data: s, Source: "SENSOR_PANTAI", WriterEpoch: 1}, StaleAfter: time.Minute, Log: slog.New(slog.DiscardHandler)}
	}

	if _, err := poller().Poll(ctx, "first"); err != nil {
		t.Fatal(err)
	}

	var critical, parked int

	var stale bool
	if err := s.ingest.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM publication_outbox WHERE critical),
		(SELECT count(*) FROM unprocessed_messages WHERE source='SENSOR_PANTAI' AND reason_code='contract_violation' AND status='parked'),
		(SELECT stale_since IS NOT NULL FROM source_progress WHERE source='SENSOR_PANTAI')`).Scan(&critical, &parked, &stale); err != nil {
		t.Fatal(err)
	}

	if critical != 1 || parked != 2 || !stale {
		t.Fatalf("critical outbox rows %d, parked %d, stale %v", critical, parked, stale)
	}

	// A new poller stands for a restart, so the cursor must come from the store.
	if _, err := poller().Poll(ctx, "second"); err != nil {
		t.Fatal(err)
	}

	var stored []string

	rows, err := s.ingest.Query(ctx, `SELECT source_ref_id||':'||severity||':'||area_name FROM hazard_events WHERE source='SENSOR_PANTAI' ORDER BY source_ref_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var row string
		if err = rows.Scan(&row); err != nil {
			t.Fatal(err)
		}

		stored = append(stored, row)
	}

	if got := strings.Join(stored, ","); got != "obs-1:AWAS:Padang,obs-4:NORMAL:Padang" {
		t.Fatalf("stored %s", got)
	}

	if got := strings.Join(cursors(), ","); got != ",c1" {
		t.Fatalf("cursors %q", got)
	}

	if err = s.ingest.QueryRow(ctx, `SELECT stale_since IS NOT NULL FROM source_progress WHERE source='SENSOR_PANTAI'`).Scan(&stale); err != nil || stale {
		t.Fatalf("clean page left the source stale: %v", err)
	}
}
