//go:build integration

package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"atlas/services/bnpb/aggregator/internal/satellite"
)

type replyLostSink struct {
	*SatelliteSink
	lost atomic.Bool
}

func (s *replyLostSink) Commit(ctx context.Context, offset int64, raw []byte, fires []satellite.Fire) error {
	if err := s.SatelliteSink.Commit(ctx, offset, raw, fires); err != nil {
		return err
	}

	if s.lost.CompareAndSwap(false, true) {
		return errors.New("injected lost commit response")
	}

	return nil
}

func satelliteFrames() []satellite.Frame {
	frames := []satellite.Frame{}

	for i, lon := range []float64{106, 106.006, 106.018, 106.024, 106.012} {
		d := satellite.Detection{Satellite: "N20", AcqDate: "2026-11-18", AcqTime: "1200", Latitude: -6, Longitude: lon, Confidence: "nominal", FRP: 4}
		frames = append(frames, satellite.Frame{Offset: int64(i + 1), Type: "detection", Detection: &d})
	}

	frames = append(frames, satellite.Frame{Offset: 6, Type: "retract", Key: frames[4].Detection.Key()}, satellite.Frame{Offset: 7, Type: "heartbeat"})

	return frames
}

func TestIntegrationSatelliteAuditConfidence(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	legacy := contractEvent("SATELIT", "legacy", now)

	legacy.Attributes["highest_confidence"] = json.RawMessage(`"nominal"`)
	if _, err := store.Upsert(ctx, WriteBatch{Source: "SATELIT", Epoch: 1, Events: []HazardEvent{legacy}, Now: now}); err != nil {
		t.Fatal(err)
	}

	reference, err := satellite.LoadReference("")
	if err != nil {
		t.Fatal(err)
	}

	sink := &SatelliteSink{Data: store, Epoch: 1, Reference: reference}
	if err = sink.Commit(ctx, 0, []byte(`{}`), []satellite.Fire{{
		ID: "new", Count: 2, Status: "active", HighestConfidence: "high", Severity: "WASPADA", OccurredAt: now,
	}, {
		ID: "closed", Status: "withdrawn", Keys: []string{}, Satellites: []string{}, Severity: "NORMAL", OccurredAt: now,
	}}); err != nil {
		t.Fatal(err)
	}

	var stored string
	if err = store.ingest.QueryRow(ctx, `SELECT attributes->>'max_confidence' FROM hazard_events WHERE source='SATELIT' AND source_ref_id='new'`).Scan(&stored); err != nil || stored != "h" {
		t.Fatal("new ingest did not persist the wire value", stored, err)
	}

	audit := (&AuditAPI{Store: store, Token: "audit"}).Handler()
	response := request(audit, "/internal/audit/events?source=SATELIT", "audit")

	var page struct {
		Items []HazardEvent `json:"items"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &page) != nil || len(page.Items) != 3 {
		t.Fatal("unexpected Audit response", response.Code, response.Body.String())
	}

	wanted := map[string]string{"legacy": `"n"`, "new": `"h"`, "closed": `null`}
	for _, event := range page.Items {
		if string(event.Attributes["max_confidence"]) != wanted[event.SourceRefID] || event.Attributes["highest_confidence"] != nil {
			t.Fatal("incorrect Audit confidence", event.SourceRefID, event.Attributes)
		}

		delete(wanted, event.SourceRefID)
	}
}

func TestIntegrationSatelliteResumeHeartbeatAndLostCommitReply(t *testing.T) {
	s := integrationStore(t)

	ref, err := satellite.LoadReference("")
	if err != nil {
		t.Fatal(err)
	}

	sink := &replyLostSink{SatelliteSink: &SatelliteSink{Data: s, Epoch: 1, Reference: ref}}
	frames := satelliteFrames()

	var (
		calls atomic.Int32
		mu    sync.Mutex
	)

	resumed := []int64{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer practice-test-token" {
			w.WriteHeader(401)
			return
		}

		call := calls.Add(1)
		if call == 1 {
			w.WriteHeader(429)
			return
		}

		if call == 2 {
			w.WriteHeader(503)
			return
		}

		from, _ := strconv.ParseInt(r.URL.Query().Get("from"), 10, 64)
		if from > 0 && r.Header.Get("Last-Event-ID") != strconv.FormatInt(from, 10) {
			t.Error("resume header differs from committed offset")
		}

		mu.Lock()

		resumed = append(resumed, from)
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")

		flusher := w.(http.Flusher)
		if call == 3 {
			flusher.Flush()
			<-r.Context().Done()

			return
		}
		// Comment heartbeats keep a read alive past its idle timeout.
		for range 6 {
			fmt.Fprint(w, ": heartbeat\n\n")
			flusher.Flush()

			select {
			case <-r.Context().Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}

		for _, frame := range frames {
			if frame.Offset < from {
				continue
			}

			var raw []byte

			switch frame.Type {
			case "retract":
				raw, _ = json.Marshal(map[string]string{"key": frame.Key})
			case "heartbeat":
				raw = []byte(`{}`)
			default:
				raw, _ = json.Marshal(frame.Detection)
			}

			fmt.Fprintf(w, "event: %s\nid: %d\ndata: %s\n\n", frame.Type, frame.Offset, raw)
			flusher.Flush()
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	latencies := []time.Duration{}
	runner := &satellite.Runner{Origin: server.URL, Token: "practice-test-token", Sink: sink, Guard: sink.Guard, Client: server.Client(), IdleTimeout: 40 * time.Millisecond, BatchSize: 2, BatchInterval: 10 * time.Millisecond, RenewInterval: 10 * time.Millisecond, Backoff: func(int) time.Duration { return time.Millisecond }, OnCommit: func(info satellite.CommitInfo) {
		latencies = append(latencies, info.Latencies...)
		if info.Stats.Offset == 7 {
			cancel()
		}
	}}
	runner.OnError = func(err error) { t.Log("injected stream retry:", err) }

	err = runner.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	stored, err := s.LoadState(context.Background(), "SATELIT")
	if err != nil || stored.Offset != 7 {
		t.Fatal("final committed stream position", stored.Offset, err)
	}

	baseline := satellite.New()
	for _, frame := range frames {
		if _, err = baseline.Apply(frame); err != nil {
			t.Fatal(err)
		}
	}

	want, _ := baseline.Snapshot()
	if !bytes.Equal(stored.Payload, want) {
		// PostgreSQL JSONB changes whitespace/key order, so compare normalized JSON.
		var a, b any

		_ = json.Unmarshal(stored.Payload, &a)
		_ = json.Unmarshal(want, &b)
		x, _ := json.Marshal(a)

		y, _ := json.Marshal(b)
		if !bytes.Equal(x, y) {
			t.Fatal("resume changed deterministic state")
		}
	}

	hasResume := false
	for _, position := range resumed {
		hasResume = hasResume || position > 0
	}

	if !sink.lost.Load() || len(resumed) < 3 || !hasResume || len(latencies) == 0 {
		t.Fatal("lost-reply resume or committed latency untested", resumed)
	}

	t.Log("429/503 retry, idle reconnect, comment heartbeat, inclusive resume and lost commit response preserved final state", resumed)
}

func TestIntegrationSatelliteEveryCrashCutUsesDurableState(t *testing.T) {
	s := integrationStore(t)

	ref, err := satellite.LoadReference("")
	if err != nil {
		t.Fatal(err)
	}

	frames := satelliteFrames()

	baseline := satellite.New()
	for _, frame := range frames {
		if _, err = baseline.Apply(frame); err != nil {
			t.Fatal(err)
		}
	}

	want, _ := baseline.Snapshot()

	ctx := context.Background()
	for cut := 0; cut <= len(frames); cut++ {
		if _, err = s.ingest.Exec(ctx, `TRUNCATE hazard_events,stream_state,publication_outbox`); err != nil {
			t.Fatal(err)
		}

		sink := &SatelliteSink{Data: s, Epoch: 1, Reference: ref}

		e := satellite.New()
		for _, frame := range frames[:cut] {
			fires, err := e.Apply(frame)
			if err != nil {
				t.Fatal(err)
			}

			raw, _ := e.Snapshot()
			if err = sink.Commit(ctx, frame.Offset, raw, fires); err != nil {
				t.Fatal(err)
			}
		}

		reopened, err := OpenStore(ctx, os.Getenv("TEST_DATABASE_URL"), 2, 4, defaultDBConnectTimeout)
		if err != nil {
			t.Fatal(err)
		}

		sink.Data = reopened

		raw, err := sink.Load(ctx)
		if err != nil {
			reopened.Close()
			t.Fatal(err)
		}

		e = satellite.New()
		if len(raw) > 0 {
			e, err = satellite.Restore(raw)
			if err != nil {
				reopened.Close()
				t.Fatal(err)
			}
		}

		for _, frame := range frames[cut:] {
			fires, err := e.Apply(frame)
			if err != nil {
				reopened.Close()
				t.Fatal(err)
			}

			raw, _ := e.Snapshot()
			if err = sink.Commit(ctx, frame.Offset, raw, fires); err != nil {
				reopened.Close()
				t.Fatal(err)
			}
		}

		got, _ := e.Snapshot()

		reopened.Close()

		if !bytes.Equal(got, want) {
			t.Fatal("durable crash cut changed result", cut)
		}

		page, err := s.Read(ctx, Query{Source: "SATELIT", Limit: 100})
		if err != nil || len(page.Data) != len(baseline.Fires()) {
			t.Fatal("event missing/duplicated after crash cut", cut, err)
		}

		for _, event := range page.Data {
			if event.Source != "SATELIT" || event.HazardType != "WILDFIRE" || event.AreaName == "" {
				t.Fatal("canonical mapping incomplete")
			}
		}
	}

	t.Log("all 8 crash cuts reloaded PostgreSQL state and retained identical fire identities and membership")
}
