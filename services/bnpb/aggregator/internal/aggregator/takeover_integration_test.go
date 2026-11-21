//go:build integration

package aggregator

import (
	"context"
	"errors"
	"testing"
	"time"

	"atlas/services/bnpb/aggregator/internal/placement"
)

func TestDataIntegrationMarkBehindKeepsLastServingCopy(t *testing.T) {
	cluster := dataCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const source = "SATELIT"

	shard := placement.Shard("metadata:" + source)

	current, _, err := cluster.Placement.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}

	first, last := current.Shards[shard][0], current.Shards[shard][1]

	request := BehindRequest{Source: source, Epoch: current.WriterEpochs[source], Shard: shard, Copy: first.Node, StoreID: first.StoreID}
	if err = cluster.MarkBehind(ctx, request); err != nil {
		t.Fatal(err)
	}

	request.Copy, request.StoreID = last.Node, last.StoreID
	if err = cluster.MarkBehind(ctx, request); err == nil {
		t.Fatal("last serving copy was marked behind")
	}

	if state := copyState(t, cluster, shard, last.Node); state != "serving" {
		t.Fatalf("last copy state is %q", state)
	}
}

func TestDataIntegrationSourceTakeoverRejectsOldWrites(t *testing.T) {
	cluster := dataCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const source = "BMKG"

	first, err := cluster.AcquireWriterEpoch(ctx, source)
	if err != nil || first < 2 {
		t.Fatalf("first epoch %d: %v", first, err)
	}

	if err = cluster.seal(ctx, source, first, true); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("strict takeover reused epoch %d: %v", first, err)
	}

	second, err := cluster.AcquireWriterEpoch(ctx, source)
	if err != nil || second <= first {
		t.Fatalf("takeover epoch %d, prior %d: %v", second, first, err)
	}

	now := time.Now().UTC()
	stale := &EpochIngest{Data: cluster, Source: source, WriterEpoch: first}
	active := &EpochIngest{Data: cluster, Source: source, WriterEpoch: second}
	message := ParkedMessage{MessageID: "takeover-old", Source: source, SourceRefID: "broken", ReceivedAt: now, Raw: []byte("{}"), ReasonCode: "schema_violation", Reason: "test"}
	event := contractEvent(source, "handoff", now)

	if _, err = stale.Save(ctx, source, []HazardEvent{event}, nil, now); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("stale write: %v", err)
	}

	if err = stale.Park(ctx, message); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("stale parking: %v", err)
	}

	if err = stale.Failure(ctx, source, now); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("stale freshness mark: %v", err)
	}

	if _, err = active.Save(ctx, source, []HazardEvent{event}, &now, now); err != nil {
		t.Fatal(err)
	}

	if err = active.Park(ctx, message); err != nil {
		t.Fatal(err)
	}

	if err = active.Failure(ctx, source, now); err != nil {
		t.Fatal(err)
	}
}
