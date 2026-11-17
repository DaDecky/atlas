package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"atlas/services/bnpb/aggregator/internal/placement"
)

type memoryPlacement struct {
	mu      sync.Mutex
	table   placement.Table
	version int
}

func (m *memoryPlacement) Load(context.Context) (placement.Table, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	raw, _ := json.Marshal(m.table)

	var clone placement.Table

	_ = json.Unmarshal(raw, &clone)

	return clone, fmt.Sprint(m.version), nil
}

func (m *memoryPlacement) CompareAndSwap(_ context.Context, v string, t placement.Table) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if v != fmt.Sprint(m.version) {
		return placement.ErrConflict
	}

	m.table = t
	m.version++

	return nil
}

func dataCluster(t *testing.T) *Cluster {
	t.Helper()

	ctx := context.Background()
	nodes := map[string]*DataNode{}

	for i, name := range []string{"a", "b", "c"} {
		url := os.Getenv([]string{"DATA_TEST_A", "DATA_TEST_B", "DATA_TEST_C"}[i])
		if url == "" {
			t.Skip("run make data-integration")
		}

		node, err := OpenDataNode(ctx, name, url)
		if err != nil {
			t.Fatal(err)
		}

		nodes[name] = node
		if _, err = node.Store.ingest.Exec(ctx, `TRUNCATE public.publication_outbox; UPDATE public.source_fence SET epoch=1`); err != nil {
			t.Fatal(err)
		}

		t.Cleanup(node.Store.Close)

		for _, s := range node.Shards {
			tx, err := s.begin(ctx, false)
			if err != nil {
				t.Fatal(err)
			}

			_, err = tx.Exec(ctx, `TRUNCATE hazard_events,stream_state,unprocessed_messages; UPDATE source_progress SET checkpoint=NULL,last_success_at=NULL,stale_since=NULL`)
			if err == nil {
				err = tx.Commit(ctx)
			}

			_ = tx.Rollback(ctx)

			if err != nil {
				t.Fatal(err)
			}
		}
	}

	table, _ := placement.New([]string{"a", "b", "c"})
	repo := &memoryPlacement{table: table}

	c := &Cluster{Nodes: nodes, Placement: repo, Deadline: time.Second}
	for _, source := range []string{"BMKG", "PVMBG", "SATELIT", "SENSOR_PANTAI"} {
		if err := c.Seal(ctx, source, 1); err != nil {
			t.Fatal(err)
		}
	}

	return c
}

func TestDataIntegrationDualWriteWipeRefillAndNextLoss(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)

	batch := WriteBatch{Source: "BMKG", Epoch: 1, Now: now}
	for i := 0; i < 150; i++ {
		event := contractEvent("BMKG", fmt.Sprintf("quake-%d", i), now.Add(time.Duration(i)*time.Second))
		batch.Events = append(batch.Events, event)
	}

	if _, err := c.Upsert(ctx, batch); err != nil {
		t.Fatal(err)
	}

	page, err := c.Read(ctx, Query{Limit: 500})
	if err != nil || len(page.Data) != 150 || page.Partial {
		t.Fatal("initial read", len(page.Data), err)
	}

	for _, node := range c.Nodes {
		count := 0

		for _, s := range node.Shards {
			r, err := s.Read(ctx, Query{Limit: 500})
			if err != nil {
				t.Fatal(err)
			}

			count += len(r.Data)
		}

		if count == 0 || count >= 150 {
			t.Fatal("node has empty/all data", node.Name, count)
		}

		t.Logf("node=%s records=%d", node.Name, count)
	}

	table, _, _ := c.Placement.Load(ctx)
	for shard, copies := range table.Shards {
		for _, copy := range copies {
			if copy.Node == "b" {
				current, _, _ := c.Placement.Load(ctx)
				if err := c.MarkBehind(ctx, BehindRequest{Source: "BMKG", Epoch: 1, Shard: shard, Copy: "b", Generation: current.Generation}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	// Simulate a wiped volume: drop every shard and recreate it empty.
	b := c.Nodes["b"]
	for shard, s := range b.Shards {
		if _, err = b.Store.ingest.Exec(ctx, `DROP SCHEMA `+s.namespace+` CASCADE`); err != nil {
			t.Fatal(err)
		}

		b.Shards[shard], err = b.Store.initShard(ctx, s.namespace)
		if err != nil {
			t.Fatal(err)
		}
	}

	page, err = c.Read(ctx, Query{Limit: 500})
	if err != nil || len(page.Data) != 150 || page.Partial {
		t.Fatal("wipe lost data", len(page.Data), err)
	}
	// Accept new events during recovery, then restore every wiped copy.
	batch.Events = []HazardEvent{contractEvent("BMKG", "during-refill", now)}
	if _, err = c.Upsert(ctx, batch); err != nil {
		t.Fatal(err)
	}

	for shard, copies := range table.Shards {
		for _, copy := range copies {
			if copy.Node == "b" {
				if err = c.Refill(ctx, shard, "b"); err != nil {
					t.Fatal("refill", shard, err)
				}
			}
		}
	}

	a := c.Nodes["a"]

	delete(c.Nodes, "a")
	defer func() { c.Nodes["a"] = a }()

	page, err = c.Read(ctx, Query{Limit: 500})
	if err != nil || len(page.Data) != 151 || page.Partial {
		t.Fatal("next loss", len(page.Data), page.Partial, err)
	}

	delete(c.Nodes, "c")

	page, err = c.Read(ctx, Query{Limit: 500})
	if err != nil || !page.Partial || len(page.UnavailableUnits) == 0 {
		t.Fatal("missing partial markers", page, err)
	}

	t.Log("verified dual placement, empty-copy refill, continued writes, second loss, and explicit partial reads")
}

func TestDataIntegrationReplicatedContract(t *testing.T) {
	runShardStoreContract(t, func(t *testing.T) ShardStore { return dataCluster(t) })
}

