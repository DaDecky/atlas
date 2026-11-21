//go:build integration

package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"atlas/services/bnpb/aggregator/internal/satellite"
)

func TestIntegrationSatelliteSyntheticBurst(t *testing.T) {
	store := integrationStore(t)

	ref, err := satellite.LoadReference("")
	if err != nil {
		t.Fatal(err)
	}

	sink := &SatelliteSink{Data: store, Epoch: 1, Reference: ref}

	const count = 600

	var lastSent atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")

		from, _ := strconv.Atoi(r.URL.Query().Get("from"))
		for i := max(0, from-1); i < count; i++ {
			group := i / 2
			d := satellite.Detection{Satellite: []string{"N20", "NPP"}[i%2], AcqDate: "2026-11-18", AcqTime: "1200", Latitude: -7 + float64(group/30)*0.1, Longitude: 105 + float64(group%30)*0.1 + float64(i%2)*0.002, Confidence: "nominal", FRP: 4}
			raw, _ := json.Marshal(d)

			if i == count-1 {
				lastSent.Store(time.Now().UnixNano())
			}

			fmt.Fprintf(w, "event: detection\nid: %d\ndata: %s\n\n", i+1, raw)
			w.(http.Flusher).Flush()
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	latencies := []time.Duration{}
	started := time.Now()
	runner := satellite.Runner{Origin: server.URL, Token: "synthetic", Client: server.Client(), Sink: sink, Guard: sink.Guard, BatchSize: 64, OnCommit: func(info satellite.CommitInfo) {
		latencies = append(latencies, info.Latencies...)
		if info.Stats.Offset == count {
			cancel()
		}
	}}
	_ = runner.Run(ctx)

	state, err := store.LoadState(context.Background(), "SATELIT")
	if err != nil || state.Offset != count {
		t.Fatal("burst lost committed frames", state.Offset, err)
	}

	page, err := store.Read(context.Background(), Query{Source: "SATELIT", Limit: 1000})
	if err != nil || len(page.Data) != count/2 || len(latencies) == 0 {
		t.Fatal("burst fire count or latency samples", len(page.Data), err)
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p95 := latencies[(len(latencies)*95+99)/100-1]

	catchup := time.Since(time.Unix(0, lastSent.Load()))
	if catchup > 60*time.Second {
		t.Fatal("synthetic catch-up exceeded 60 seconds")
	}

	t.Logf("synthetic_unpaced_burst_frames=%d fires=%d elapsed_s=%.3f catchup_s=%.3f p95_ingest_to_store_ms=%.3f", count, len(page.Data), time.Since(started).Seconds(), catchup.Seconds(), float64(p95.Microseconds())/1000)
}
