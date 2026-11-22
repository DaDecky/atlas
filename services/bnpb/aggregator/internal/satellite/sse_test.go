package satellite

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestSSECommentsMultilineAndBoundaryValidation(t *testing.T) {
	body := ": heartbeat\r\n\r\nevent: detection\r\nid: 1\r\ndata: {\r\ndata: \"satellite\":\"N20\",\"acq_date\":\"2026-11-18\",\"acq_time\":15,\"latitude\":-6,\"longitude\":106,\"confidence\":\"n\",\"frp\":4}\r\n\r\nevent: update\nid: 2\ndata: {\"unfinished\":"

	var frames []Frame

	_ = ReadSSE(context.Background(), strings.NewReader(body), func(event SSEEvent) error {
		frame, err := DecodeFeed(event)
		frames = append(frames, frame)

		return err
	})

	if len(frames) != 1 || frames[0].Detection == nil || frames[0].Detection.AcqTime != "0015" || frames[0].Detection.Confidence != "nominal" {
		t.Fatal("SSE framing or explicit wire normalization", frames)
	}

	for _, raw := range [][]byte{[]byte("event: detection\nid: -1\ndata: {}\n\n"), []byte("data: " + strings.Repeat("x", MaxFrameBytes+1) + "\n\n"), {'d', 'a', 't', 'a', ':', ' ', 0xff, '\n', '\n'}} {
		if err := ReadSSE(context.Background(), bytes.NewReader(raw), func(SSEEvent) error { return nil }); err == nil {
			t.Fatal("unsafe SSE boundary accepted")
		}
	}
}

func TestFeedDecoderRejectsMissingIdentityAndInvalidDepth(t *testing.T) {
	d := detection(106)

	raw, _ := json.Marshal(d)
	for _, body := range []string{strings.Replace(string(raw), `"latitude":-6,`, "", 1), `{"extra":` + strings.Repeat("[", 33) + "0" + strings.Repeat("]", 33) + "}", `{"satellite":"N20","acq_date":"2026-11-18","acq_time":1260,"latitude":0,"longitude":0,"confidence":"n","frp":4}`} {
		if _, err := DecodeFeed(SSEEvent{Type: "detection", Offset: 1, Data: []byte(body)}); err == nil {
			t.Fatal("invalid feed payload accepted", body)
		}
	}
}

func TestPracticeFeedCorrectionsResolveCanonicalIdentity(t *testing.T) {
	engine := New()
	records := []struct {
		kind, body string
	}{
		{"detection", `{"offset":0,"detection":{"satellite":"N20","acq_date":"2026-11-18","acq_time":4,"latitude":-6,"longitude":106,"confidence":"n","frp":4}}`},
		{"detection", `{"offset":1,"detection":{"satellite":"N21","acq_date":"2026-11-18","acq_time":4,"latitude":-6,"longitude":106.001,"confidence":"n","frp":6}}`},
		{"update", `{"offset":2,"key":"N20|2026-11-18|4|-6|106","changes":{"confidence":"h","frp":11,"bright_ti4":310.2}}`},
		{"retract", `{"offset":3,"key":"N20|2026-11-18|4|-6|106","reason":"correction"}`},
	}

	// Rebuild from durable state between a detection and its correction.
	var id string

	for offset, record := range records {
		if offset == 2 {
			snapshot, err := engine.Snapshot()
			if err != nil {
				t.Fatal(err)
			}

			engine, err = Restore(snapshot)
			if err != nil {
				t.Fatal(err)
			}
		}

		frame, err := DecodeFeed(SSEEvent{Type: record.kind, Offset: int64(offset), Data: []byte(record.body)})
		if err != nil {
			t.Fatal(offset, err)
		}

		fires, err := engine.Apply(frame)
		if err != nil {
			t.Fatal(offset, err)
		}

		switch offset {
		case 1:
			if len(fires) != 1 || fires[0].TotalFRP != 10 {
				t.Fatal("initial fire", fires)
			}

			id = fires[0].ID
		case 2:
			if len(fires) != 1 || fires[0].ID != id || fires[0].TotalFRP != 17 || fires[0].HighestConfidence != "high" {
				t.Fatal("partial update did not preserve identity and apply changed fields", fires)
			}
		case 3:
			if len(fires) != 1 || fires[0].ID != id || fires[0].Status != "withdrawn" {
				t.Fatal("retract did not withdraw the same fire", fires)
			}
		}
	}
}

func TestPracticeFeedRejectsInvalidCorrections(t *testing.T) {
	for _, body := range []string{
		`{"offset":2,"key":"N20|2026-11-18|4|-6|106","changes":{"frp":null}}`,
		`{"offset":2,"key":"N20|2026-11-18|4|-6|106","changes":{"latitude":-7}}`,
		`{"offset":2,"key":"N20|2026-11-18|1260|-6|106","changes":{"frp":11}}`,
		`{"offset":99,"key":"N20|2026-11-18|4|-6|106","changes":{"frp":11}}`,
	} {
		if _, err := DecodeFeed(SSEEvent{Type: "update", Offset: 2, Data: []byte(body)}); err == nil {
			t.Fatal("invalid correction accepted", body)
		}
	}
}

func TestReferenceUsesNearestCentreAndStableTies(t *testing.T) {
	ref, err := LoadReference("")
	if err != nil || len(ref.Provinces) != 34 || ref.Version == "" {
		t.Fatal("versioned fallback reference", err)
	}

	for _, p := range ref.Provinces {
		if got := ref.Nearest(p.Latitude, p.Longitude); got != p.Name {
			t.Fatal("centre does not select its own province", p.Name, got)
		}
	}

	ref = Reference{Provinces: []Province{{Name: "B", Latitude: 0, Longitude: 1}, {Name: "A", Latitude: 0, Longitude: -1}}}
	if got := ref.Nearest(0, 0); got != "A" {
		t.Fatal("distance tie depends on table order", got)
	}
}

func TestDetectionKeysKeepFeedTextWithoutRounding(t *testing.T) {
	engine := New()
	bodies := []struct{ kind, body string }{
		{"detection", `{"offset":0,"detection":{"satellite":"N20","acq_date":"2026-11-12","acq_time":243,"latitude":-2.51230,"longitude":113.80440,"confidence":"h","frp":20}}`},
		{"detection", `{"offset":1,"detection":{"satellite":"N21","acq_date":"2026-11-12","acq_time":"0250","latitude":-2.51300,"longitude":113.80500,"confidence":"n","frp":21.45}}`},
		{"update", `{"offset":2,"key":"N20|2026-11-12|243|-2.5123|113.8044","changes":{"frp":25}}`},
	}

	want := []string{"N20|2026-11-12|243|-2.51230|113.80440", "N21|2026-11-12|0250|-2.51300|113.80500"}

	for offset, record := range bodies {
		frame, err := DecodeFeed(SSEEvent{Type: record.kind, Offset: int64(offset), Data: []byte(record.body)})
		if err != nil {
			t.Fatal(offset, err)
		}

		fires, err := engine.Apply(frame)
		if err != nil {
			t.Fatal(offset, err)
		}

		if offset == 0 {
			continue
		}

		if len(fires) != 1 {
			t.Fatal(offset, "expected one fire", fires)
		}

		got := ReportKeys(fires[0])
		if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("offset %d: detection_keys = %q, want %q", offset, got, want)
		}
	}
}

func TestReportKeysConvertsStoredCanonicalKeys(t *testing.T) {
	fire := Fire{Keys: []string{`["N20","2026-11-12","0243","-2.5123","113.8044"]`}}
	if got := ReportKeys(fire); len(got) != 1 || got[0] != "N20|2026-11-12|243|-2.5123|113.8044" {
		t.Fatalf("ReportKeys = %q", got)
	}
}
