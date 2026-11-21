package sensor

import (
	"strings"
	"testing"
	"time"
)

const validRecord = `{"observation_id":"obs-1","station_id":"stn-1","observed_at":"2026-11-12T02:43:00Z",` +
	`"sea_level_anomaly_m":0.42,"wave_height_m":1.8,"quality":"ok","critical":false}`

func page(items ...string) []byte {
	return []byte(`{"generated_at":"2026-11-12T02:43:05Z","items":[` + strings.Join(items, ",") + `],"next_cursor":"c2"}`)
}

func TestValidRecordDecodes(t *testing.T) {
	got, err := DecodePage(page(validRecord))
	if err != nil || len(got.Items) != 1 || got.Items[0].Violation != "" || got.NextCursor != "c2" {
		t.Fatalf("page %+v, error %v", got, err)
	}

	o := got.Items[0].Observation
	if o.ID != "obs-1" || o.WaveHeight != 1.8 || o.Critical || !o.ObservedAt.Equal(time.Date(2026, 11, 12, 2, 43, 0, 0, time.UTC)) {
		t.Fatalf("observation %+v", o)
	}
}

func TestBrokenRecordsCarryViolation(t *testing.T) {
	for name, record := range map[string]string{
		"missing field":     strings.Replace(validRecord, `,"critical":false`, "", 1),
		"null critical":     strings.Replace(validRecord, `"critical":false`, `"critical":null`, 1),
		"wrong type":        strings.Replace(validRecord, `1.8`, `"1.8"`, 1),
		"negative wave":     strings.Replace(validRecord, `1.8`, `-0.1`, 1),
		"bad time":          strings.Replace(validRecord, `2026-11-12T02:43:00Z`, `yesterday`, 1),
		"after generation":  strings.Replace(validRecord, `02:43:00Z`, `02:44:00Z`, 1),
		"empty identifier":  strings.Replace(validRecord, `"obs-1"`, `""`, 1),
		"not an object":     `[1,2]`,
		"trailing document": validRecord + `{}`,
	} {
		got, err := DecodePage(page(validRecord, record))
		if name == "trailing document" {
			if err == nil {
				t.Errorf("%s: page accepted", name)
			}

			continue
		}

		if err != nil || got.Items[1].Violation == "" || got.Items[0].Violation != "" {
			t.Errorf("%s: page %+v, error %v", name, got.Items, err)
		}
	}
}

func TestPageWithoutCursorBreaksContract(t *testing.T) {
	for _, body := range []string{`{"generated_at":"2026-11-12T02:43:05Z","items":[]}`, `{"items":[],"next_cursor":""}`, `<html>`} {
		if _, err := DecodePage([]byte(body)); err == nil {
			t.Errorf("accepted %s", body)
		}
	}
}

func TestInvalidStationBreaksContract(t *testing.T) {
	stations, err := DecodeStations([]byte(`{"items":[{"station_id":"stn-1","name":"Padang","province":"Sumatera Barat","latitude":-0.9,"longitude":100.3}]}`))
	if err != nil || stations["stn-1"].Name != "Padang" {
		t.Fatalf("stations %+v, error %v", stations, err)
	}

	if _, err = DecodeStations([]byte(`{"items":[{"station_id":"stn-1","name":"Padang","province":"Sumbar","latitude":-91,"longitude":100}]}`)); err == nil {
		t.Fatal("latitude out of range accepted")
	}
}
