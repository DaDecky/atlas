package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func contractEvent(source, ref string, now time.Time) HazardEvent {
	types := map[string]string{"BMKG": "SEISMIC", "PVMBG": "VOLCANIC", "SATELIT": "WILDFIRE", "SENSOR_PANTAI": "COASTAL"}

	return HazardEvent{
		HazardID: hazardID(source, ref), Source: source, SourceRefID: ref,
		HazardType: types[source], Severity: "WASPADA", AreaName: "test",
		OccurredAt: now, IngestedAt: now, Attributes: map[string]json.RawMessage{},
	}
}

// New implementations run the same behavioral contract with a fresh store per subtest.
func runShardStoreContract(t *testing.T, open func(*testing.T) ShardStore) {
	t.Helper()

	ctx := context.Background()
	now := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)

	t.Run("idempotent revisions and checkpoint rollback", func(t *testing.T) {
		s := open(t)
		event := contractEvent("BMKG", "one", now)
		batch := WriteBatch{Source: "BMKG", Epoch: 1, Events: []HazardEvent{event}, Now: now}

		changed, err := s.CommitCheckpoint(ctx, batch, now)
		if err != nil || len(changed) != 1 || changed[0].Revision != 1 {
			t.Fatal("first write", changed, err)
		}

		changed, err = s.Upsert(ctx, batch)
		if err != nil || len(changed) != 0 {
			t.Fatal("duplicate write changed data", changed, err)
		}

		batch.Events[0].Severity = "AWAS"
		batch.Events[0].IngestedAt = now.Add(time.Hour)

		changed, err = s.CommitCheckpoint(ctx, batch, now.Add(-time.Hour))
		if err != nil || len(changed) != 1 || changed[0].Revision != 2 || !changed[0].IngestedAt.Equal(now) {
			t.Fatal("revision or original ingestion time", changed, err)
		}

		good := contractEvent("BMKG", "must-rollback", now)
		bad := contractEvent("BMKG", "invalid", now)
		bad.Severity = "BROKEN"
		batch.Events = []HazardEvent{good, bad}

		if _, err = s.CommitCheckpoint(ctx, batch, now.Add(time.Hour)); err == nil {
			t.Fatal("invalid batch committed")
		}

		checkpoint, err := s.Checkpoint(ctx, "BMKG")
		if err != nil || checkpoint == nil || !checkpoint.Equal(now) {
			t.Fatal("checkpoint advanced or regressed", checkpoint, err)
		}

		page, err := s.Read(ctx, Query{Source: "BMKG", Limit: 100})
		if err != nil || len(page.Data) != 1 || page.Data[0].Severity != "AWAS" || page.Partial || len(page.UnavailableUnits) != 0 {
			t.Fatal("atomic batch or complete read markers", page, err)
		}
	})

	t.Run("source epochs fence every write path", func(t *testing.T) {
		s := open(t)

		if err := s.Seal(ctx, "BMKG", 2); err != nil {
			t.Fatal(err)
		}

		if err := s.Seal(ctx, "BMKG", 2); err != nil {
			t.Fatal("seal is not idempotent", err)
		}

		if err := s.Seal(ctx, "BMKG", 1); !errors.Is(err, ErrStaleEpoch) {
			t.Fatal("seal regressed", err)
		}

		batch := WriteBatch{Source: "BMKG", Epoch: 1, Events: []HazardEvent{contractEvent("BMKG", "old", now)}, Now: now}
		if _, err := s.Upsert(ctx, batch); !errors.Is(err, ErrStaleEpoch) {
			t.Fatal("stale upsert", err)
		}

		if _, err := s.CommitCheckpoint(ctx, batch, now); !errors.Is(err, ErrStaleEpoch) {
			t.Fatal("stale checkpoint", err)
		}

		if _, err := s.CommitState(ctx, batch, StreamState{Offset: 1, Payload: json.RawMessage(`{}`)}); !errors.Is(err, ErrStaleEpoch) {
			t.Fatal("stale state", err)
		}

		batch.Epoch = 3
		if _, err := s.Upsert(ctx, batch); !errors.Is(err, ErrStaleEpoch) {
			t.Fatal("unsealed future epoch accepted", err)
		}

		batch.Epoch = 2
		if _, err := s.Upsert(ctx, batch); err != nil {
			t.Fatal("current writer rejected", err)
		}

		for _, source := range []string{"PVMBG", "SATELIT", "SENSOR_PANTAI"} {
			batch = WriteBatch{Source: source, Epoch: 1, Events: []HazardEvent{contractEvent(source, "one", now)}, Now: now}
			if _, err := s.Upsert(ctx, batch); err != nil {
				t.Fatal("independent source", source, err)
			}
		}

		batch.Source = "BMKG"

		batch.Epoch = 2
		if _, err := s.Upsert(ctx, batch); err == nil {
			t.Fatal("cross-source batch accepted")
		}
	})

	t.Run("events state and offset commit together", func(t *testing.T) {
		s := open(t)
		batch := WriteBatch{Source: "SATELIT", Epoch: 1, Events: []HazardEvent{contractEvent("SATELIT", "fire", now)}, Now: now}
		state := StreamState{Offset: 10, Payload: json.RawMessage(`{"watermark":"first"}`)}

		if _, err := s.LoadState(ctx, batch.Source); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal("unexpected initial state", err)
		}

		if _, err := s.CommitState(ctx, batch, state); err != nil {
			t.Fatal(err)
		}

		batch.Events[0].Severity = "BROKEN"
		state.Offset = 11

		state.Payload = json.RawMessage(`{"watermark":"must-rollback"}`)
		if _, err := s.CommitState(ctx, batch, state); err == nil {
			t.Fatal("invalid event advanced state")
		}

		stored, err := s.LoadState(ctx, batch.Source)
		if err != nil || stored.Offset != 10 || string(stored.Payload) != `{"watermark": "first"}` {
			t.Fatal("state did not roll back", stored, err)
		}

		batch.Events[0].Severity = "AWAS"

		for _, offset := range []int64{9, 10} {
			state.Offset = offset
			if _, err := s.CommitState(ctx, batch, state); !errors.Is(err, ErrStaleOffset) {
				t.Fatal("replay accepted", offset, err)
			}
		}

		page, err := s.Read(ctx, Query{Source: "SATELIT", Limit: 100})
		if err != nil || len(page.Data) != 1 || page.Data[0].Severity != "WASPADA" {
			t.Fatal("replay mutated events", page, err)
		}

		state.Offset = 11
		if _, err := s.CommitState(ctx, batch, state); err != nil {
			t.Fatal("next state rejected", err)
		}
	})

	t.Run("concurrent replay has one effect", func(t *testing.T) {
		s := open(t)
		batch := WriteBatch{Source: "SATELIT", Epoch: 1, Events: []HazardEvent{contractEvent("SATELIT", "one", now)}, Now: now}
		state := StreamState{Offset: 1, Payload: json.RawMessage(`{}`)}
		results := make(chan error, 2)

		var wg sync.WaitGroup
		for range 2 {
			wg.Go(func() {
				_, err := s.CommitState(ctx, batch, state)
				results <- err
			})
		}

		wg.Wait()
		close(results)

		successes, replays := 0, 0

		for err := range results {
			switch {
			case err == nil:
				successes++
			case errors.Is(err, ErrStaleOffset):
				replays++
			default:
				t.Fatal(err)
			}
		}

		if successes != 1 || replays != 1 {
			t.Fatal("concurrent replay results", successes, replays)
		}
	})
}

func TestIntegrationShardStoreContract(t *testing.T) {
	runShardStoreContract(t, func(t *testing.T) ShardStore { return integrationStore(t) })
}

func TestIntegrationShardStoreRestart(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	batch := WriteBatch{Source: "SATELIT", Epoch: 2, Events: []HazardEvent{contractEvent("SATELIT", "restart", now)}, Now: now}
	if err := s.Seal(ctx, batch.Source, batch.Epoch); err != nil {
		t.Fatal(err)
	}

	if _, err := s.CommitState(ctx, batch, StreamState{Offset: 42, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}

	restarted, err := OpenStore(ctx, os.Getenv("TEST_DATABASE_URL"), 2, 4, defaultDBConnectTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()

	epoch, err := restarted.Epoch(ctx, batch.Source)
	if err != nil || epoch != 2 {
		t.Fatal("epoch lost on reopen", epoch, err)
	}

	state, err := restarted.LoadState(ctx, batch.Source)
	if err != nil || state.Offset != 42 {
		t.Fatal("state lost on reopen", state, err)
	}

	batch.Epoch = 1
	if _, err := restarted.Upsert(ctx, batch); !errors.Is(err, ErrStaleEpoch) {
		t.Fatal("reopen reset fence", err)
	}

	if err := restarted.MarkBehind(ctx, BehindRequest{
		Source: batch.Source, Epoch: 2, Shard: "shard-1", Copy: "b", Generation: 1,
	}); !errors.Is(err, ErrSingleInstance) {
		t.Fatal("single instance claimed placement changed", err)
	}
}
