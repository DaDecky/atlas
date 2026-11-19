package placement

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestPlacementDistributionAndStableOrder(t *testing.T) {
	a, err := New([]string{"c", "a", "b"})
	if err != nil {
		t.Fatal(err)
	}

	b, _ := New([]string{"b", "c", "a"})
	if !reflect.DeepEqual(a, b) {
		t.Fatal("input order changes placement")
	}

	counts := map[string]int{}

	for _, copies := range a.Shards {
		for _, c := range copies {
			counts[c.Node]++
		}
	}

	for _, n := range a.Nodes {
		if counts[n] < 15 || counts[n] >= Shards {
			t.Fatal("unbalanced/full-node placement", counts)
		}
	}

	keys := map[string]int{}
	for i := 0; i < 10000; i++ {
		keys[Shard(string(rune(i)))]++
	}

	if len(keys) != Shards {
		t.Fatal("unused shards")
	}

	for _, n := range keys {
		if n < 130 || n > 290 {
			t.Fatal("poor hash distribution", n)
		}
	}
}

func TestConfigMapCAS(t *testing.T) {
	table, _ := New([]string{"a", "b", "c"})
	version := "1"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test" {
			w.WriteHeader(401)
			return
		}

		raw, _ := json.Marshal(table)
		cm := configMap{APIVersion: "v1", Kind: "ConfigMap", Data: map[string]string{"placement.json": string(raw)}}
		cm.Metadata.ResourceVersion = version

		if r.Method == "PUT" {
			var incoming configMap
			if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
				t.Error(err)
			}

			if incoming.Metadata.ResourceVersion != version {
				w.WriteHeader(409)
				return
			}

			version = "2"
		}

		_ = json.NewEncoder(w).Encode(cm)
	}))
	defer server.Close()

	repo := ConfigMap{Client: server.Client(), Origin: server.URL, Namespace: "test", Name: "placement", Token: "test"}

	loaded, v, err := repo.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if err = repo.CompareAndSwap(context.Background(), v, loaded); err != nil {
		t.Fatal(err)
	}

	if err = repo.CompareAndSwap(context.Background(), v, loaded); err != ErrConflict {
		t.Fatal("stale CAS", err)
	}
}

func TestLeaseUsesObservedVersionForTakeover(t *testing.T) {
	var doc leaseDocument

	doc.Metadata.Name = "placement-recovery"
	doc.Metadata.ResourceVersion = "1"
	doc.Spec.HolderIdentity = "other"
	doc.Spec.LeaseDurationSeconds = 1

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_ = json.NewEncoder(w).Encode(doc)
			return
		}

		var next leaseDocument
		if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
			t.Error(err)
			w.WriteHeader(400)

			return
		}

		if next.Metadata.ResourceVersion != doc.Metadata.ResourceVersion {
			w.WriteHeader(409)
			return
		}

		next.Metadata.ResourceVersion = "2"
		doc = next
		_ = json.NewEncoder(w).Encode(doc)
	}))
	defer server.Close()

	lease := &Lease{Client: server.Client(), Origin: server.URL, Name: "placement-recovery", Namespace: "test", Holder: "ours", Duration: time.Second}
	if err := lease.Acquire(context.Background()); err != ErrLeaseHeld {
		t.Fatal("stole unobserved lease", err)
	}
	// The remote renewTime can be arbitrarily far in the past or future; only
	// local observation of the same version authorizes takeover.
	lease.since = time.Now().Add(-2 * time.Second)
	if err := lease.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}

	if doc.Spec.HolderIdentity != "ours" {
		t.Fatal("not acquired")
	}

	if err := lease.Acquire(context.Background()); err != nil {
		t.Fatal("renew", err)
	}
}

func TestTransitionAllowsOnlyRefillPath(t *testing.T) {
	for _, tc := range []struct {
		from, to State
		allowed  bool
	}{
		{Serving, Behind, true},
		{Refilling, Behind, true},
		{Behind, Refilling, true},
		{Refilling, Refilling, true},
		{Refilling, Serving, true},
		{Serving, Refilling, false},
		{Behind, Serving, false},
		{Serving, Serving, false},
	} {
		table := Table{Shards: map[string][]Copy{"shard-0": {{Node: "a", State: tc.from}, {Node: "b", State: Serving}}}}

		err := table.Transition("shard-0", "a", tc.to)
		if (err == nil) != tc.allowed {
			t.Errorf("%s to %s: error %v", tc.from, tc.to, err)
		}

		if want := map[bool]State{true: tc.to, false: tc.from}[tc.allowed]; table.Shards["shard-0"][0].State != want {
			t.Errorf("%s to %s left state %s", tc.from, tc.to, table.Shards["shard-0"][0].State)
		}
	}
}

func TestTransitionRejectsUnknownCopy(t *testing.T) {
	table := Table{Shards: map[string][]Copy{"shard-0": {{Node: "a", State: Serving}}}}

	if err := table.Transition("shard-0", "z", Behind); err == nil {
		t.Fatal("unknown copy accepted")
	}
}
