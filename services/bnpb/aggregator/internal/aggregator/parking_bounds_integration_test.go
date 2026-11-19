//go:build integration

package aggregator

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestIntegrationReplayReasonPreservesOrder(t *testing.T) {
	checkReplayReasonOrder(t, integrationStore(t))
}

func TestDataIntegrationReplayReasonPreservesOrder(t *testing.T) {
	checkReplayReasonOrder(t, dataCluster(t))
}

func checkReplayReasonOrder(t *testing.T, store Parking) {
	t.Helper()

	ctx := context.Background()
	now := time.Now().UTC().Add(-time.Minute)
	rows := []struct{ id, ref, reason string }{
		{"older", "same-ref", "schema_violation"},
		{"newer", "same-ref", "invalid_utf8"},
		{"unrelated", "other-ref", "invalid_utf8"},
	}

	for i, row := range rows {
		if err := store.Park(ctx, ParkedMessage{MessageID: row.id, Source: "PVMBG", SourceRefID: row.ref, ReceivedAt: now.Add(time.Duration(i) * time.Second), Raw: []byte("{}"), ReasonCode: row.reason, Reason: "test"}); err != nil {
			t.Fatal(err)
		}
	}

	var decoded []string

	decode := func(m ParkedMessage) (HazardEvent, error) {
		decoded = append(decoded, m.MessageID)
		return contractEvent(m.Source, m.SourceRefID, m.ReceivedAt), nil
	}

	for _, tc := range []struct {
		reason string
		count  int
	}{{"invalid_utf8", 1}, {"invalid_utf8", 0}, {"schema_violation", 1}, {"invalid_utf8", 1}, {"invalid_utf8", 0}} {
		count, err := store.ReplayReason(ctx, "PVMBG", 1, 100, tc.reason, decode)
		if err != nil || count != tc.count {
			t.Fatalf("reason %s recovered %d, want %d: %v", tc.reason, count, tc.count, err)
		}
	}

	if !slices.Equal(decoded, []string{"unrelated", "older", "newer"}) {
		t.Fatalf("incorrect replay selection/order: %v", decoded)
	}

	if err := store.Park(ctx, ParkedMessage{MessageID: "concurrent", Source: "PVMBG", SourceRefID: "concurrent", ReceivedAt: now, Raw: []byte("{}"), ReasonCode: "schema_violation", Reason: "test"}); err != nil {
		t.Fatal(err)
	}

	var requests sync.WaitGroup

	var recovered atomic.Int64

	for range 2 {
		requests.Go(func() {
			count, err := store.ReplayReason(ctx, "PVMBG", 1, 100, "schema_violation", func(m ParkedMessage) (HazardEvent, error) {
				return contractEvent(m.Source, m.SourceRefID, m.ReceivedAt), nil
			})
			if err != nil {
				t.Error(err)
			}

			recovered.Add(int64(count))
		})
	}

	requests.Wait()

	if recovered.Load() != 1 {
		t.Fatalf("concurrent replay recovered %d copies of one message", recovered.Load())
	}
}

func TestIntegrationParkingByteBoundAndDueRetryFairness(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Add(-time.Hour)
	raw := bytes.Repeat([]byte("x"), 4<<20)

	for i := 0; i < 5; i++ {
		ref := fmt.Sprintf("large-%d", i)
		if err := s.Park(ctx, ParkedMessage{MessageID: ref, Source: "BMKG", SourceRefID: ref, ReceivedAt: now.Add(time.Duration(i) * time.Second), Raw: raw, ReasonCode: "schema_incompatible", Reason: "waiting for repair"}); err != nil {
			t.Fatal(err)
		}
	}

	if err := s.Park(ctx, ParkedMessage{MessageID: "due-after-permanent", Source: "BMKG", SourceRefID: "due", ReceivedAt: now.Add(10 * time.Second), Raw: []byte(`{}`), ReasonCode: "temporary", Reason: "retry possible", Retryable: true}); err != nil {
		t.Fatal(err)
	}

	decode := func(m ParkedMessage) (HazardEvent, error) {
		return contractEvent(m.Source, m.SourceRefID, m.ReceivedAt), nil
	}
	if n, err := s.RetryDue(ctx, "BMKG", 1, 1000, decode); err != nil || n != 1 {
		t.Fatal("permanent backlog starved an unrelated due retry", n, err)
	}

	page, err := s.ListUnprocessed(ctx, "BMKG", "", 1000)
	if err != nil || len(page.Items) != 2 || page.NextCursor == nil {
		t.Fatal("raw byte bound did not create a short cursor page", len(page.Items), err)
	}

	seen := map[string]bool{}

	for {
		for _, message := range page.Items {
			if seen[message.MessageID] {
				t.Fatal("duplicate cursor record")
			}

			seen[message.MessageID] = true
		}

		if page.NextCursor == nil {
			break
		}

		page, err = s.ListUnprocessed(ctx, "BMKG", *page.NextCursor, 1000)
		if err != nil {
			t.Fatal(err)
		}
	}

	if len(seen) != 5 {
		t.Fatal("short byte pages lost messages", len(seen))
	}

	if n, err := s.Replay(ctx, "BMKG", 1, 1000, decode); err != nil || n != 2 {
		t.Fatal("replay raw allocation exceeded the byte batch", n, err)
	}

	t.Log("8 MiB raw pages/replay batches bounded; unrelated due retry passed a permanent backlog")
}

func TestDataIntegrationParkingBytePagesKeepGlobalOrder(t *testing.T) {
	c := dataCluster(t)
	ctx := context.Background()
	now := time.Now().UTC().Add(-time.Hour)
	raw := bytes.Repeat([]byte("x"), 4<<20)

	for i := 0; i < 6; i++ {
		source := []string{"BMKG", "PVMBG"}[i%2]

		ref := fmt.Sprintf("interleaved-large-%d", i)
		if err := c.Park(ctx, ParkedMessage{MessageID: ref, Source: source, SourceRefID: ref, ReceivedAt: now.Add(time.Duration(i) * time.Second), Raw: raw, ReasonCode: "schema_incompatible", Reason: "waiting for repair"}); err != nil {
			t.Fatal(err)
		}
	}

	cursor := ""
	seen := []string{}

	for {
		page, err := c.ListUnprocessed(ctx, "", cursor, 1000)
		if err != nil {
			t.Fatal(err)
		}

		for _, message := range page.Items {
			seen = append(seen, message.MessageID)
		}

		if page.NextCursor == nil {
			break
		}

		cursor = *page.NextCursor
	}

	if len(seen) != 6 {
		t.Fatal("byte-short shard pages skipped global records", seen)
	}

	for i, id := range seen {
		if id != fmt.Sprintf("interleaved-large-%d", i) {
			t.Fatal("global cursor order changed", seen)
		}
	}

	t.Log("interleaved multi-source four-MiB messages paginated without skipping across short local pages")
}
