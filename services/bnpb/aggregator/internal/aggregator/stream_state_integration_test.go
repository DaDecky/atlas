package aggregator

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"atlas/services/bnpb/aggregator/internal/placement"
)

func TestDataIntegrationStreamStateUsesNewestServingOffset(t *testing.T) {
	cluster := dataCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const source = "SATELIT"

	table, _, err := cluster.Placement.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}

	shard := placement.Shard("metadata:" + source)
	batch := WriteBatch{Source: source, Epoch: table.WriterEpochs[source], Now: time.Now().UTC()}
	copies := table.Shards[shard]
	older := StreamState{Offset: 25, Payload: json.RawMessage(`{"offset":25}`)}
	newer := StreamState{Offset: 30, Payload: json.RawMessage(`{"offset":30}`)}

	if _, err = cluster.copyStore(copies[0], shard).CommitState(ctx, batch, older); err != nil {
		t.Fatal(err)
	}

	store := cluster.copyStore(copies[1], shard)
	if _, err = store.CommitState(ctx, batch, newer); err != nil {
		t.Fatal(err)
	}

	if !store.verifyStreamCommit(ctx, source, newer) || store.verifyStreamCommit(ctx, source, older) {
		t.Fatal("commit verification did not match the exact offset and payload")
	}

	wrongPayload := StreamState{Offset: newer.Offset, Payload: older.Payload}
	if store.verifyStreamCommit(ctx, source, wrongPayload) {
		t.Fatal("a different payload was accepted at the same offset")
	}

	for range 4 {
		state, err := cluster.LoadState(ctx, source)
		if err != nil || state.Offset != newer.Offset {
			t.Fatalf("resume returned offset %d, want %d: %v", state.Offset, newer.Offset, err)
		}
	}
}

func TestDataIntegrationLargeStreamStateHasDedicatedDeadline(t *testing.T) {
	cluster := dataCluster(t)
	cluster.Deadline = shardDeadline

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	const source = "SATELIT"

	table, _, err := cluster.Placement.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}

	shard := placement.Shard("metadata:" + source)

	var locks []pgx.Tx

	for _, copy := range table.Shards[shard] {
		s := cluster.copyStore(copy, shard)

		tx, err := s.ingest.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)

		// SHARE permits the offset query but delays the following INSERT.
		relation := pgx.Identifier{s.namespace, "stream_state"}.Sanitize()
		if _, err = tx.Exec(ctx, "LOCK TABLE "+relation+" IN SHARE MODE"); err != nil {
			t.Fatal(err)
		}

		locks = append(locks, tx)
	}

	released := make(chan struct{})

	timer := time.AfterFunc(750*time.Millisecond, func() {
		defer close(released)

		for _, tx := range locks {
			_ = tx.Rollback(ctx)
		}
	})
	defer timer.Stop()

	batch := WriteBatch{Source: source, Epoch: table.WriterEpochs[source], Now: time.Now().UTC()}
	state := StreamState{Offset: 42, Payload: json.RawMessage(`{"snapshot":"` + strings.Repeat("x", 8<<20) + `"}`)}
	_, err = cluster.CommitState(ctx, batch, state)

	<-released

	if err != nil {
		t.Fatal(err)
	}

	for _, copy := range table.Shards[shard] {
		got, err := cluster.copyStore(copy, shard).streamOffset(ctx, source)
		if err != nil || got != state.Offset {
			t.Fatalf("store %s committed %d, want %d: %v", copy.Node, got, state.Offset, err)
		}
	}

	got, err := cluster.LoadState(ctx, source)
	if err != nil || got.Offset != state.Offset || len(got.Payload) < 8<<20 {
		t.Fatalf("large checkpoint read offset %d, bytes %d: %v", got.Offset, len(got.Payload), err)
	}
}
