package aggregator

import (
	"context"
	"encoding/json"
	"errors"
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

		node, err := OpenDataNode(ctx, name, url, DefaultClusterLimits())
		if err != nil {
			t.Fatal(err)
		}

		nodes[name] = node
		if _, err = node.Store.ingest.Exec(ctx, `TRUNCATE public.publication_outbox,public.unprocessed_messages; UPDATE public.source_fence SET epoch=1; UPDATE public.ingestion_clock SET last_value='epoch'`); err != nil {
			t.Fatal(err)
		}

		t.Cleanup(node.Store.Close)

		for _, s := range node.Shards {
			tx, err := s.begin(ctx, false)
			if err != nil {
				t.Fatal(err)
			}

			_, err = tx.Exec(ctx, `TRUNCATE hazard_events,stream_state,unprocessed_messages,field_reports_inbox; UPDATE source_progress SET checkpoint=NULL,last_success_at=NULL,stale_since=NULL`)
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

	limits := DefaultClusterLimits()
	limits.ShardDeadline = time.Second

	c := &Cluster{Nodes: nodes, Placement: repo, Limits: limits}
	for _, source := range []string{"BMKG", "PVMBG", "SATELIT", "SENSOR_PANTAI"} {
		if err := c.Seal(ctx, source, 1); err != nil {
			t.Fatal(err)
		}
	}

	for name := range c.Nodes {
		if _, err := c.detectStore(ctx, name); err != nil {
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

	metrics, err := c.NodeMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}

	physical := int64(0)

	for _, m := range metrics {
		if m.Unavailable || m.Bytes <= 0 {
			t.Fatal("invalid node metrics", m)
		}

		physical += m.Records
	}

	if physical != 300 {
		t.Fatal("physical replica count", physical)
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
				if err := c.MarkBehind(ctx, BehindRequest{Source: "BMKG", Epoch: 1, Shard: shard, Copy: "b", StoreID: copy.StoreID}); err != nil {
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

func TestDataIntegrationLiveRefillAndAudit(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	event := contractEvent("BMKG", "live", now)
	shard := placement.Shard(event.HazardID)

	initial := WriteBatch{Source: "BMKG", Epoch: 1, Events: []HazardEvent{event}, Now: now}
	if _, err := c.Upsert(ctx, initial); err != nil {
		t.Fatal(err)
	}

	table, _, _ := c.Placement.Load(ctx)

	target := table.Shards[shard][1].Node
	if err := c.MarkBehind(ctx, BehindRequest{Source: "BMKG", Epoch: 1, Shard: shard, Copy: target, StoreID: table.Shards[shard][1].StoreID}); err != nil {
		t.Fatal(err)
	}

	dest := c.Nodes[target].Shards[shard]

	tx, err := dest.begin(ctx, false)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Exec(ctx, `TRUNCATE hazard_events`)
	if err == nil {
		err = tx.Commit(ctx)
	}

	_ = tx.Rollback(ctx)

	if err != nil {
		t.Fatal(err)
	}

	writes := make(chan error, 1)

	go func() {
		for i := 0; i < 30; i++ {
			b := initial

			b.Events = []HazardEvent{contractEvent("BMKG", fmt.Sprintf("live-%d", i), now)}
			if _, err := c.Upsert(ctx, b); err != nil {
				writes <- err
				return
			}
		}

		writes <- nil
	}()

	refillErr := c.Refill(ctx, shard, target)

	if err := <-writes; err != nil {
		t.Fatal("live writes", err)
	}

	if refillErr != nil {
		if err := c.Refill(ctx, shard, target); err != nil {
			t.Fatal("refill retry", err)
		}
	}
	// Finish all needed repair and compare full canonical rows on both holders.
	a, err := shardDigest(ctx, c.Nodes[table.Shards[shard][0].Node].Shards[shard])
	if err != nil {
		t.Fatal(err)
	}

	b, err := shardDigest(ctx, dest)
	if err != nil || a != b {
		t.Fatal("refill diverged", a, b, err)
	}

	all, err := c.Checksum(ctx, "BMKG")
	if err != nil || all.Count != 31 || all.Partial {
		t.Fatal("audit count", all, err)
	}

	summary, err := Analytics(ctx, c, nil, nil)
	if err != nil || summary.Total != 31 {
		t.Fatal("analytics vs audit", summary.Total, err)
	}

	q := Query{Limit: 7, Audit: true}
	seen := map[string]bool{}

	for {
		page, err := c.Read(ctx, q)
		if err != nil {
			t.Fatal(err)
		}

		for _, h := range page.Data {
			if seen[h.HazardID] {
				t.Fatal("duplicate scatter page")
			}

			seen[h.HazardID] = true
		}

		if page.NextCursor == nil {
			break
		}

		q.Cursor = *page.NextCursor
	}

	if len(seen) != 31 {
		t.Fatal("scatter pages lost rows", len(seen))
	}

	t.Log("live writes and replica refill agree; Audit, analytics and paginated scatter reads agree")
}

func TestDataIntegrationAutomaticWipeRecoveryAndParking(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	event := contractEvent("PVMBG", "recovered", now)
	if _, err := c.Upsert(ctx, WriteBatch{Source: "PVMBG", Epoch: 1, Now: now, Events: []HazardEvent{event}}); err != nil {
		t.Fatal(err)
	}

	m := ParkedMessage{MessageID: "replicated-message", Source: "PVMBG", SourceRefID: "new", ReceivedAt: now, Raw: []byte(`{"new":1}`), ReasonCode: "schema_incompatible", Reason: "changed schema"}
	if err := c.Park(ctx, m); err != nil {
		t.Fatal(err)
	}
	// A node returns with empty shard schemas, without manually marking placement.
	node := c.Nodes["b"]
	for _, shard := range node.Shards {
		if _, err := node.Store.ingest.Exec(ctx, `DROP SCHEMA `+shard.namespace+` CASCADE`); err != nil {
			t.Fatal(err)
		}
	}

	if err := c.RecoverOnce(ctx); err != nil {
		t.Fatal("automatic refill", err)
	}

	checksum, err := c.Checksum(ctx, "PVMBG")
	if err != nil || checksum.Count != 1 || checksum.Partial {
		t.Fatal("recovery lost events", checksum, err)
	}

	page, err := c.ListUnprocessed(ctx, "PVMBG", "", 100)
	if err != nil || len(page.Items) != 1 {
		t.Fatal("parking lost on recovery", page, err)
	}

	count, err := c.Replay(ctx, "PVMBG", 1, 100, func(m ParkedMessage) (HazardEvent, error) { return contractEvent(m.Source, m.SourceRefID, now), nil })
	if err != nil || count != 1 {
		t.Fatal("replicated replay", count, err)
	}

	page, err = c.ListUnprocessed(ctx, "PVMBG", "", 100)
	if err != nil || len(page.Items) != 0 {
		t.Fatal("recovery flag", page, err)
	}
}

func TestDataIntegrationNewNodeJoinsWithoutStoppingReads(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	events := []HazardEvent{}
	for i := 0; i < 100; i++ {
		events = append(events, contractEvent("BMKG", fmt.Sprintf("join-%d", i), now))
	}

	if _, err := c.Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: now, Events: events}); err != nil {
		t.Fatal(err)
	}

	before, err := c.Checksum(ctx, "BMKG")
	if err != nil {
		t.Fatal(err)
	}

	url := os.Getenv("DATA_TEST_D")
	if url == "" {
		t.Skip("fourth local database required")
	}

	node, err := OpenDataNode(ctx, "d", url, DefaultClusterLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer node.Store.Close()

	c.Nodes["d"] = node
	for _, s := range node.Shards {
		tx, err := s.begin(ctx, false)
		if err != nil {
			t.Fatal(err)
		}

		_, err = tx.Exec(ctx, `TRUNCATE hazard_events,stream_state,unprocessed_messages`)
		if err == nil {
			err = tx.Commit(ctx)
		}

		_ = tx.Rollback(ctx)

		if err != nil {
			t.Fatal(err)
		}
	}

	if err = c.RebalanceOnce(ctx); err != nil {
		t.Fatal(err)
	}

	after, err := c.Checksum(ctx, "BMKG")
	if err != nil || after.Count != before.Count || after.Checksum != before.Checksum || after.Partial {
		t.Fatal("join changed canonical data", before, after, err)
	}

	table, _, err := c.Placement.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}

	held := 0

	for _, copies := range table.Shards {
		for _, copy := range copies {
			if copy.Node == "d" {
				held++
			}
		}
	}

	if held == 0 {
		t.Fatal("new node has no shards")
	}

	if _, err = c.Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: now, Events: []HazardEvent{contractEvent("BMKG", "post-join", now)}}); err != nil {
		t.Fatal(err)
	}

	t.Logf("new node holds %d copies; checksum unchanged across join; writes continue", held)
}

func TestDataIntegrationTakeoverExcludesUnsealedReplica(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	offline := c.Nodes["c"]
	delete(c.Nodes, "c")

	if err := c.Seal(ctx, "BMKG", 2); err != nil {
		t.Fatal(err)
	}

	table, _, err := c.Placement.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for _, copies := range table.Shards {
		for _, copy := range copies {
			if copy.Node == "c" && copy.State != "behind" {
				t.Fatal("unsealed replica still serves reads")
			}
		}
	}

	now := time.Now().UTC()
	if _, err = c.Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: now}); !errors.Is(err, ErrStaleEpoch) {
		t.Fatal("old writer survived takeover", err)
	}

	c.Nodes["c"] = offline
	if err = c.RecoverOnce(ctx); err != nil {
		t.Fatal("takeover recovery", err)
	}

	epoch, err := offline.Store.Epoch(ctx, "BMKG")
	if err != nil || epoch != 2 {
		t.Fatal("returned replica not sealed", epoch, err)
	}
}

func TestDataIntegrationConcurrentWritersDoNotStarveIngestionPool(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	now := time.Now().UTC()
	errs := make(chan error, 8)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			_, err := c.Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: now, Events: []HazardEvent{contractEvent("BMKG", fmt.Sprintf("concurrent-%d", i), now)}})
			errs <- err
		})
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	table, _, err := c.Placement.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for _, copies := range table.Shards {
		for _, copy := range copies {
			if copy.State != "serving" {
				t.Fatal("healthy node marked behind by pool starvation", copy)
			}
		}
	}

	checksum, err := c.Checksum(ctx, "BMKG")
	if err != nil || checksum.Count != 8 {
		t.Fatal(checksum, err)
	}
}
