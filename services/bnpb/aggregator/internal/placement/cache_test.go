package placement

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type placementServer struct {
	mu          sync.Mutex
	table       Table
	version     int
	gets        atomic.Int64
	watches     atomic.Int64
	expire      atomic.Bool
	events      chan configMap
	reset       chan struct{}
	unavailable atomic.Bool
}

func newPlacementServer(t *testing.T) (*placementServer, *ConfigMap) {
	t.Helper()

	table, _ := New([]string{"a", "b", "c"})
	state := &placementServer{table: table, version: 1, events: make(chan configMap, 16), reset: make(chan struct{}, 1)}
	server := httptest.NewServer(http.HandlerFunc(state.serve))
	t.Cleanup(server.Close)

	return state, &ConfigMap{Client: server.Client(), Origin: server.URL, Namespace: "test", Name: "placement", Token: "test"}
}

func (s *placementServer) document() configMap {
	raw, _ := json.Marshal(s.table)
	doc := configMap{APIVersion: "v1", Kind: "ConfigMap", Data: map[string]string{"placement.json": string(raw)}}
	doc.Metadata.Name = "placement"
	doc.Metadata.ResourceVersion = fmt.Sprintf("opaque-%d", s.version)

	return doc
}

func (s *placementServer) serve(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer test" {
		w.WriteHeader(401)
		return
	}

	if r.URL.Query().Get("watch") == "true" {
		s.watches.Add(1)

		if r.URL.Query().Get("fieldSelector") != "metadata.name=placement" {
			w.WriteHeader(400)
			return
		}

		if s.expire.Swap(false) {
			w.WriteHeader(410)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()

		for {
			select {
			case <-r.Context().Done():
				return
			case <-s.reset:
				return
			case doc := <-s.events:
				if err := json.NewEncoder(w).Encode(map[string]any{"type": "MODIFIED", "object": doc}); err != nil {
					return
				}

				w.(http.Flusher).Flush()
			}
		}
	}

	if s.unavailable.Load() {
		w.WriteHeader(503)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if r.Method == http.MethodPut {
		var incoming configMap
		if json.NewDecoder(r.Body).Decode(&incoming) != nil {
			w.WriteHeader(400)
			return
		}

		if incoming.Metadata.ResourceVersion != s.document().Metadata.ResourceVersion {
			w.WriteHeader(409)
			return
		}

		if json.Unmarshal([]byte(incoming.Data["placement.json"]), &s.table) != nil {
			w.WriteHeader(400)
			return
		}

		s.version++
		s.events <- s.document()
	} else {
		s.gets.Add(1)
	}

	_ = json.NewEncoder(w).Encode(s.document())
}

func awaitCache(t *testing.T, check func() bool) {
	t.Helper()

	deadline := time.NewTimer(4 * time.Second)
	defer deadline.Stop()

	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()

	for {
		if check() {
			return
		}

		select {
		case <-deadline.C:
			t.Fatal("cache condition timed out")
		case <-tick.C:
		}
	}
}

func TestPlacementCacheEliminatesReadGETs(t *testing.T) {
	state, remote := newPlacementServer(t)

	ctx := context.Background()
	for range 100 {
		if _, _, err := remote.Load(ctx); err != nil {
			t.Fatal(err)
		}
	}

	before := state.gets.Load()

	cached, err := WatchConfigMap(ctx, remote)
	if err != nil {
		t.Fatal(err)
	}
	defer cached.Close()

	awaitCache(t, func() bool { return state.watches.Load() > 0 })

	for range 100 {
		if _, _, err = cached.Load(ctx); err != nil {
			t.Fatal(err)
		}
	}

	if requests := state.gets.Load() - before; requests != 1 {
		t.Fatal("cache still loads placement per read", requests)
	}

	t.Logf("100 uncached loads: %d GETs; 100 cached loads: 0 GETs after 1 initial GET", before)

	table, _, _ := cached.Load(ctx)
	table.Shards["shard-0"][0].State = "behind"

	fresh, _, _ := cached.Load(ctx)
	if fresh.Shards["shard-0"][0].State != "serving" {
		t.Fatal("caller mutated cached snapshot")
	}
}

func TestPlacementCacheWatchCASAndConflict(t *testing.T) {
	state, remote := newPlacementServer(t)
	ctx := context.Background()

	cached, err := WatchConfigMap(ctx, remote)
	if err != nil {
		t.Fatal(err)
	}
	defer cached.Close()

	awaitCache(t, func() bool { return state.watches.Load() > 0 })

	table, version, _ := cached.Load(ctx)
	table.Generation++

	table.Shards["shard-0"][0].State = "behind"
	if err = cached.CompareAndSwap(ctx, version, table); err != nil {
		t.Fatal(err)
	}

	loaded, next, err := cached.Load(ctx)
	if err != nil || loaded.Generation != table.Generation || next == version {
		t.Fatal("PUT not reflected synchronously", loaded.Generation, next, err)
	}
	// An independent writer changes placement, but its watch event is delayed.
	state.mu.Lock()
	state.table.Generation++
	state.version++
	generation := state.table.Generation
	state.mu.Unlock()

	loaded.Generation++
	if err = cached.CompareAndSwap(ctx, next, loaded); !errors.Is(err, ErrConflict) {
		t.Fatal("stale resourceVersion accepted", err)
	}

	loaded, _, err = cached.Load(ctx)
	if err != nil || loaded.Generation != generation {
		t.Fatal("conflict did not refresh cache", err)
	}
	// A newer remote event must update the reader without a GET.
	state.mu.Lock()
	state.table.Generation++
	state.version++

	generation = state.table.Generation
	state.events <- state.document()
	state.mu.Unlock()
	awaitCache(t, func() bool { fresh, _, err := cached.Load(ctx); return err == nil && fresh.Generation == generation })
}

func TestPlacementCacheReloadsExpiredWatch(t *testing.T) {
	state, remote := newPlacementServer(t)
	state.expire.Store(true)

	cached, err := WatchConfigMap(context.Background(), remote)
	if err != nil {
		t.Fatal(err)
	}
	defer cached.Close()

	awaitCache(t, func() bool { return state.watches.Load() >= 2 })
	awaitCache(t, func() bool { _, _, err := cached.Load(context.Background()); return err == nil })

	if state.gets.Load() < 2 {
		t.Fatal("expired version not reloaded")
	}
	// Disconnecting a watch must stop serving the old placement until reconnect.
	state.unavailable.Store(true)

	state.reset <- struct{}{}

	awaitCache(t, func() bool { _, _, err := cached.Load(context.Background()); return err != nil })
	state.unavailable.Store(false)
	awaitCache(t, func() bool { return state.watches.Load() >= 3 })
	awaitCache(t, func() bool { _, _, err := cached.Load(context.Background()); return err == nil })
}
