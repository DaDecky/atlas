package aggregator

import (
	"context"
	"errors"
	"testing"
	"time"
)

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
