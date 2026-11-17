package placement

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
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
