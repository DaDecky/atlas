//go:build integration

package aggregator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"atlas/services/bnpb/aggregator/internal/placement"
)

func eventInShard(t *testing.T, shard, prefix string) HazardEvent {
	t.Helper()

	for i := 0; i < 10000; i++ {
		h := contractEvent("BMKG", fmt.Sprintf("%s-%d", prefix, i), time.Now().UTC().Truncate(time.Microsecond))
		if placement.Shard(h.HazardID) == shard {
			return h
		}
	}

	t.Fatal("could not find event in shard", shard)

	return HazardEvent{}
}

func execData(t *testing.T, s *Store, sql string, args ...any) {
	t.Helper()

	if _, err := s.ingest.Exec(context.Background(), sql, args...); err != nil {
		t.Fatal(err)
	}
}

func copyState(t *testing.T, c *Cluster, shard, node string) placement.State {
	t.Helper()

	table, _, err := c.Placement.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	for _, holder := range table.Shards[shard] {
		if holder.Node == node {
			return holder.State
		}
	}

	t.Fatal("copy missing")

	return ""
}

func markCopy(t *testing.T, c *Cluster, shard, node string) {
	t.Helper()

	table, _, err := c.Placement.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	storeID := ""

	for _, holder := range table.Shards[shard] {
		if holder.Node == node {
			storeID = holder.StoreID
		}
	}

	if err = c.MarkBehind(context.Background(), BehindRequest{Source: "BMKG", Epoch: 1, Shard: shard, Copy: node, StoreID: storeID}); err != nil {
		t.Fatal(err)
	}
}

func TestFailedCopyWriteMarksCopyBehind(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	h := contractEvent("BMKG", "failed-write", time.Now().UTC())
	shard := placement.Shard(h.HazardID)
	table, _, _ := c.Placement.Load(ctx)
	target := table.Shards[shard][0].Node
	store := c.Nodes[target].Shards[shard]
	execData(t, store, `REVOKE INSERT ON `+store.namespace+`.hazard_events FROM aggregator`)
	t.Cleanup(func() { execData(t, store, `GRANT INSERT ON `+store.namespace+`.hazard_events TO aggregator`) })

	if _, err := c.Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: h.IngestedAt, Events: []HazardEvent{h}}); err != nil {
		t.Fatal(err)
	}

	if state := copyState(t, c, shard, target); state != "behind" {
		t.Fatal("failed copy still eligible for reads", state)
	}
	// The first holder is empty. Reading it would hide the successful peer write.
	page, err := c.Read(ctx, Query{Limit: 100})
	if err != nil || page.Partial || len(page.Data) != 1 {
		t.Fatal("read did not skip failed copy", page, err)
	}
}

func TestRefillRejectsDifferingDigests(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	h := contractEvent("BMKG", "refill", time.Now().UTC())

	shard := placement.Shard(h.HazardID)
	if _, err := c.Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: h.IngestedAt, Events: []HazardEvent{h}}); err != nil {
		t.Fatal(err)
	}

	table, _, _ := c.Placement.Load(ctx)
	target := table.Shards[shard][1].Node
	markCopy(t, c, shard, target)
	// An extra target row cannot be fixed by upsert and must fail verification.
	extra := eventInShard(t, shard, "target-only")
	if _, err := c.Nodes[target].Shards[shard].Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: extra.IngestedAt, Events: []HazardEvent{extra}}); err != nil {
		t.Fatal(err)
	}

	if err := c.Refill(ctx, shard, target); err == nil {
		t.Fatal("refill accepted different digests")
	}

	if state := copyState(t, c, shard, target); state != "refilling" {
		t.Fatal("mismatching target promoted", state)
	}
}

func TestWipedStoreGoesBehindAfterRecreate(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	table, _, _ := c.Placement.Load(ctx)
	shard := ""

	for key, holders := range table.Shards {
		if holders[0].Node == "b" {
			shard = key
			break
		}
	}

	h := eventInShard(t, shard, "before-wipe")
	if _, err := c.Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: h.IngestedAt, Events: []HazardEvent{h}}); err != nil {
		t.Fatal(err)
	}

	node := c.Nodes["b"]
	for _, s := range node.Shards {
		execData(t, node.Store, `DROP SCHEMA `+s.namespace+` CASCADE`)
	}

	execData(t, node.Store, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`)
	execData(t, node.Store, schema)
	execData(t, node.Store, schemaMigrations)
	execData(t, node.Store, publicSchema)

	for _, s := range node.Shards {
		if _, err := node.Store.initShard(ctx, s.namespace); err != nil {
			t.Fatal(err)
		}
	}
	// Tables already exist, but no planner has marked the wiped disk yet.
	page, err := c.Read(ctx, Query{Limit: 100})
	if err != nil || page.Partial || len(page.Data) != 1 {
		t.Fatal("empty incarnation served before detection", page, err)
	}

	if _, err = c.detectStore(ctx, "b"); err != nil {
		t.Fatal(err)
	}

	if state := copyState(t, c, shard, "b"); state != "behind" {
		t.Fatal("wiped disk not detected", state)
	}

	if err = c.RecoverOnce(ctx); err != nil {
		t.Fatal(err)
	}

	peer := table.Shards[shard][1].Node
	saved := c.Nodes[peer]

	delete(c.Nodes, peer)
	defer func() { c.Nodes[peer] = saved }()

	page, err = c.Read(ctx, Query{Limit: 100})
	if err != nil || page.Partial || len(page.Data) != 1 {
		t.Fatal("recovered copy lost data after next loss", page, err)
	}
}

func TestDataIntegrationParkingDegradedCopy(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	shard := placement.Shard("metadata:BMKG")
	table, _, _ := c.Placement.Load(ctx)
	target := table.Shards[shard][1].Node
	store := c.Nodes[target].Shards[shard]
	execData(t, store, `REVOKE INSERT ON `+store.namespace+`.unprocessed_messages FROM aggregator`)
	t.Cleanup(func() { execData(t, store, `GRANT INSERT ON `+store.namespace+`.unprocessed_messages TO aggregator`) })

	m := ParkedMessage{MessageID: "park-one", Source: "BMKG", ReceivedAt: time.Now().UTC(), Raw: []byte("bad"), ReasonCode: "schema_incompatible", Reason: "unknown schema"}
	if err := c.Park(ctx, m); err != nil {
		t.Fatal(err)
	}

	if copyState(t, c, shard, target) != "behind" {
		t.Fatal("failed parking copy not marked")
	}

	m.MessageID = "park-two"
	if err := c.Park(ctx, m); err != nil {
		t.Fatal("degraded parking rejected", err)
	}

	page, err := c.ListUnprocessed(ctx, "BMKG", "", 100)
	if err != nil || len(page.Items) != 2 {
		t.Fatal(page, err)
	}
}

func lockDataTables(t *testing.T, node *DataNode, tables []string) {
	t.Helper()

	ctx := context.Background()

	tx, err := node.Store.ingest.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if _, err = tx.Exec(ctx, `LOCK TABLE `+strings.Join(tables, ",")+` IN ACCESS EXCLUSIVE MODE`); err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}

	release, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)

		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()

		select {
		case <-release:
		case <-timer.C:
		}

		_ = tx.Rollback(ctx)
	}()

	t.Cleanup(func() { close(release); <-done })
}

func TestDataIntegrationSlowCopyUsesHealthyPeer(t *testing.T) {
	c := dataCluster(t)
	c.Limits.ShardDeadline = defaultShardDeadline
	ctx := context.Background()

	h := contractEvent("BMKG", "slow-read", time.Now().UTC())
	if _, err := c.Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: h.IngestedAt, Events: []HazardEvent{h}}); err != nil {
		t.Fatal(err)
	}

	var tables []string
	for _, s := range c.Nodes["b"].Shards {
		tables = append(tables, s.namespace+".hazard_events", s.namespace+".source_progress")
	}

	lockDataTables(t, c.Nodes["b"], tables)

	call, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	started := time.Now()

	page, err := c.Read(call, Query{Limit: 100})
	if err != nil || page.Partial || len(page.Data) != 1 {
		t.Fatal("slow node hid healthy data", page, err)
	}

	if call.Err() != nil {
		t.Fatal("request budget exhausted", time.Since(started))
	}

	for _, fresh := range page.Sources {
		if fresh.Source == "BMKG" && fresh.Status != "healthy" {
			t.Fatal("metadata fallback failed", fresh)
		}
	}

	t.Log("healthy peer answered in", time.Since(started))
}

func TestDataIntegrationCopyWritesShareDeadline(t *testing.T) {
	c := dataCluster(t)
	c.Limits.ShardDeadline = 300 * time.Millisecond
	ctx := context.Background()
	h := contractEvent("BMKG", "parallel-write", time.Now().UTC())
	shard := placement.Shard(h.HazardID)

	table, _, _ := c.Placement.Load(ctx)
	for _, holder := range table.Shards[shard] {
		lockDataTables(t, c.Nodes[holder.Node], []string{c.Nodes[holder.Node].Shards[shard].namespace + ".hazard_events"})
	}

	start := time.Now()

	_, err := c.writeCopies(ctx, table, shard, WriteBatch{Source: "BMKG", Epoch: 1, Now: h.IngestedAt, Events: []HazardEvent{h}, timestampsAssigned: true}, nil, nil)
	if err == nil {
		t.Fatal("blocked copies accepted write")
	}

	if elapsed := time.Since(start); elapsed > 550*time.Millisecond {
		t.Fatal("copy deadlines accumulated", elapsed)
	}

	for _, holder := range table.Shards[shard] {
		if copyState(t, c, shard, holder.Node) != "serving" {
			t.Fatal("unacknowledged write removed a serving copy", holder)
		}
	}
}

func TestDataIntegrationRecoveryContinuesAfterRefillFailure(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	table, _, _ := c.Placement.Load(ctx)

	var shards []string

	for shard, holders := range table.Shards {
		for _, h := range holders {
			if h.Node == "b" {
				shards = append(shards, shard)
			}
		}
	}

	sort.Strings(shards)

	bad, good := shards[0], shards[len(shards)-1]
	for _, shard := range []string{bad, good} {
		h := eventInShard(t, shard, "recovery")
		if _, err := c.Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: h.IngestedAt, Events: []HazardEvent{h}}); err != nil {
			t.Fatal(err)
		}

		markCopy(t, c, shard, "b")
	}

	extra := eventInShard(t, bad, "extra")
	if _, err := c.Nodes["b"].Shards[bad].Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: extra.IngestedAt, Events: []HazardEvent{extra}}); err != nil {
		t.Fatal(err)
	}

	if err := c.RecoverOnce(ctx); err == nil {
		t.Fatal("bad refill not reported")
	}

	if copyState(t, c, bad, "b") != "refilling" || copyState(t, c, good, "b") != "serving" {
		t.Fatal("later shard not recovered")
	}
}

func TestDataIntegrationPlannerDatabaseCallDeadline(t *testing.T) {
	c := dataCluster(t)
	c.Limits.ShardDeadline = 150 * time.Millisecond
	lockDataTables(t, c.Nodes["b"], []string{"public.source_fence"})

	started := time.Now()

	if err := c.RecoverOnce(context.Background()); err == nil {
		t.Fatal("stalled store not reported")
	}

	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatal("planner waited on frozen store", elapsed)
	}
}

func TestDataIntegrationRefillCopiesOnlyUnpublishedEvents(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	first := contractEvent("BMKG", "delivered", time.Now().UTC())
	shard := placement.Shard(first.HazardID)

	pending := eventInShard(t, shard, "pending")
	if _, err := c.Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: first.IngestedAt, Events: []HazardEvent{first, pending}}); err != nil {
		t.Fatal(err)
	}

	table, _, _ := c.Placement.Load(ctx)
	donor, target := table.Shards[shard][0].Node, table.Shards[shard][1].Node
	execData(t, c.Nodes[donor].Store, `DELETE FROM public.publication_outbox WHERE hazard_id=$1`, first.HazardID)
	execData(t, c.Nodes[target].Store, `TRUNCATE public.publication_outbox`)
	execData(t, c.Nodes[target].Store, `TRUNCATE `+c.Nodes[target].Shards[shard].namespace+`.hazard_events`)
	markCopy(t, c, shard, target)

	if err := c.Refill(ctx, shard, target); err != nil {
		t.Fatal(err)
	}

	rows, err := c.Nodes[target].Store.query.Query(ctx, `SELECT hazard_id FROM public.publication_outbox`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var ids []string

	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			t.Fatal(err)
		}

		ids = append(ids, id)
	}

	if rows.Err() != nil || len(ids) != 1 || ids[0] != pending.HazardID {
		t.Fatal("refill republished delivered history", ids, rows.Err())
	}
}

func TestDataIntegrationRestartDoesNotRebuildConstraints(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	node := c.Nodes["a"]
	before := map[string]uint32{}

	for _, s := range node.Shards {
		var oid uint32
		if err := node.Store.query.QueryRow(ctx, `SELECT oid FROM pg_constraint WHERE conrelid=$1::regclass AND conname='hazard_events_source_check'`, s.namespace+".hazard_events").Scan(&oid); err != nil {
			t.Fatal(err)
		}

		before[s.namespace] = oid
	}

	for _, s := range node.Shards {
		if _, err := node.Store.initShard(ctx, s.namespace); err != nil {
			t.Fatal(err)
		}
	}

	for namespace, oid := range before {
		var after uint32
		if err := node.Store.query.QueryRow(ctx, `SELECT oid FROM pg_constraint WHERE conrelid=$1::regclass AND conname='hazard_events_source_check'`, namespace+".hazard_events").Scan(&after); err != nil || after != oid {
			t.Fatal("constraint recreated on restart", namespace, oid, after, err)
		}
	}
}

func TestDataIntegrationRefillPreservesParkedPublications(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	h := contractEvent("BMKG", "parked-publication", time.Now().UTC())

	shard := placement.Shard(h.HazardID)
	if _, err := c.Upsert(ctx, WriteBatch{Source: h.Source, Epoch: 1, Now: h.IngestedAt, Events: []HazardEvent{h}}); err != nil {
		t.Fatal(err)
	}

	table, _, _ := c.Placement.Load(ctx)
	donor, target := table.Shards[shard][0].Node, table.Shards[shard][1].Node

	p := &selectivePublisher{bad: h.HazardID}
	for range publicationAttempts {
		if _, err := c.Nodes[donor].Store.RelayOne(ctx, p); err == nil {
			t.Fatal("failure not reported")
		}
	}

	execData(t, c.Nodes[target].Store, `TRUNCATE public.publication_outbox,public.unprocessed_messages`)
	markCopy(t, c, shard, target)

	if err := c.Refill(ctx, shard, target); err != nil {
		t.Fatal(err)
	}

	page, err := c.Nodes[target].Store.ListUnprocessed(ctx, "BMKG", "", 100)
	if err != nil || len(page.Items) != 1 || page.Items[0].ReasonCode != "publish_failed" {
		t.Fatal("refill lost publication parking", page, err)
	}
	// Losing the donor must still leave an automatic retry on the repaired copy.
	execData(t, c.Nodes[target].Store, `UPDATE public.unprocessed_messages SET next_attempt_at=now()-interval '1 second'`)

	p.bad = ""
	if sent, err := c.Nodes[target].Store.RelayOne(ctx, p); err != nil || !sent {
		t.Fatal("refilled publication could not retry", sent, err)
	}
}

func TestDataIntegrationRecoveryInitializesFreshStorePublicTables(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()

	node := c.Nodes["b"]
	for _, s := range node.Shards {
		execData(t, node.Store, `DROP SCHEMA `+s.namespace+` CASCADE`)
	}

	execData(t, node.Store, `DROP SCHEMA public CASCADE; CREATE SCHEMA public;
 CREATE TABLE public.store_identity(singleton boolean PRIMARY KEY DEFAULT true,id text NOT NULL DEFAULT gen_random_uuid()::text);
 INSERT INTO public.store_identity(singleton) VALUES(true)`)

	if err := c.RecoverOnce(ctx); err != nil {
		t.Fatal("store with only bootstrap identity did not recover", err)
	}

	table, _, _ := c.Placement.Load(ctx)
	for _, holder := range table.Shards["shard-0"] {
		if holder.State != "serving" {
			t.Fatal("copy not recovered", holder)
		}
	}
}

func TestDataIntegrationHealthyAbsenceDoesNotWaitForSlowPeer(t *testing.T) {
	c := dataCluster(t)
	c.Limits.ShardDeadline = defaultShardDeadline
	ctx := context.Background()
	table, _, _ := c.Placement.Load(ctx)
	shard := ""

	for key, holders := range table.Shards {
		if holders[1].Node == "b" {
			shard = key
			break
		}
	}

	h := eventInShard(t, shard, "new-event")
	lockDataTables(t, c.Nodes["b"], []string{c.Nodes["b"].Shards[shard].namespace + ".hazard_events"})

	call, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	if _, err := c.event(call, h.HazardID); !errors.Is(err, ErrNotFound) {
		t.Fatal("healthy absence hidden by failed peer", err)
	}
}

func TestDataIntegrationPublicationParkingDoesNotStarveSourceReplay(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()

	now := time.Now().UTC()
	if err := c.Nodes["a"].Store.Park(ctx, ParkedMessage{MessageID: "publication:old", Source: "BMKG", ReceivedAt: now.Add(-time.Hour), Raw: []byte("broken publication"), ReasonCode: "publish_failed", Reason: "decode failed"}); err != nil {
		t.Fatal(err)
	}

	if err := c.Park(ctx, ParkedMessage{MessageID: "source-message", Source: "BMKG", SourceRefID: "source-event", ReceivedAt: now, Raw: []byte("upstream"), ReasonCode: "schema_incompatible", Reason: "new schema"}); err != nil {
		t.Fatal(err)
	}

	recovered, err := c.Replay(ctx, "BMKG", 1, 1, func(m ParkedMessage) (HazardEvent, error) { return contractEvent(m.Source, m.SourceRefID, now), nil })
	if err != nil || recovered != 1 {
		t.Fatal("older publication blocked source replay", recovered, err)
	}
}
