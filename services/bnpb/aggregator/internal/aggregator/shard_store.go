package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ShardStore is the M2 data contract v1. Adapter and leadership integrations
// target this interface. Store is its single-instance PostgreSQL implementation.
type ShardStore interface {
	ReadStore
	Checkpoint(context.Context, string) (*time.Time, error)
	Upsert(context.Context, WriteBatch) ([]HazardEvent, error)
	CommitCheckpoint(context.Context, WriteBatch, time.Time) ([]HazardEvent, error)
	CommitState(context.Context, WriteBatch, StreamState) ([]HazardEvent, error)
	LoadState(context.Context, string) (StreamState, error)
	Epoch(context.Context, string) (int64, error)
	Seal(context.Context, string, int64) error
	MarkBehind(context.Context, BehindRequest) error
	WarningEvents(context.Context, []string) ([]HazardEvent, error)
	failureEpoch(context.Context, string, time.Time, int64) error
	park(context.Context, ParkedMessage, int64) error
}

// WriteBatch belongs to exactly one source and one sealed writer epoch.
// Now is the successful ingestion time, supplied by the caller for reproducibility.
type WriteBatch struct {
	Source             string
	Epoch              int64
	Events             []HazardEvent
	Now                time.Time
	sourceLocked       bool
	timestampsAssigned bool
}

// StreamState is an opaque JSON object owned by the source adapter. Offset and
// payload commit atomically with the batch, so restart cannot skip unsaved events.
type StreamState struct {
	Offset  int64
	Payload json.RawMessage
}

// BehindRequest identifies a placement change authorized by the source's current writer.
// Replica code must reject an outdated epoch or copy incarnation before permitting a
// single-copy write. B1 has no replica placement.
type BehindRequest struct {
	Source  string
	Epoch   int64
	Shard   string
	Copy    string
	StoreID string
}

var (
	ErrStaleEpoch     = errors.New("writer epoch is not the sealed epoch")
	ErrStaleOffset    = errors.New("stream offset is already committed")
	ErrSingleInstance = errors.New("single-instance store has no replica placement")
)

var _ ShardStore = (*Store)(nil)

func (s *Store) Epoch(ctx context.Context, source string) (int64, error) {
	var epoch int64

	err := s.ingest.QueryRow(ctx, `SELECT epoch FROM public.source_fence WHERE source = $1`, source).Scan(&epoch)

	return epoch, err
}

// Seal is monotonic and idempotent. Updating the row waits for in-flight writes
// holding its lock; after this returns, an older epoch cannot commit a new write.
func (s *Store) Seal(ctx context.Context, source string, epoch int64) error {
	return s.seal(ctx, source, epoch, false)
}

func (s *Store) seal(ctx context.Context, source string, epoch int64, strict bool) error {
	if epoch < 1 {
		return ErrStaleEpoch
	}

	var sealed int64

	err := s.ingest.QueryRow(ctx, `
		UPDATE public.source_fence SET epoch = $2
		WHERE source = $1 AND (epoch < $2 OR (NOT $3 AND epoch = $2))
		RETURNING epoch`, source, epoch, strict).Scan(&sealed)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrStaleEpoch
	}

	return err
}

func checkWriterEpoch(ctx context.Context, tx pgx.Tx, source string, expected int64) error {
	var current int64

	if err := tx.QueryRow(ctx, `SELECT epoch FROM public.source_fence WHERE source=$1 FOR SHARE`, source).Scan(&current); err != nil {
		return err
	}

	if current != expected {
		return ErrStaleEpoch
	}

	return nil
}

func (s *Store) Upsert(ctx context.Context, batch WriteBatch) ([]HazardEvent, error) {
	return s.commitBatch(ctx, batch, nil, nil)
}

// CommitCheckpoint saves a batch and advances its time checkpoint in one
// transaction. Replayed older checkpoints never move the checkpoint backwards.
func (s *Store) CommitCheckpoint(ctx context.Context, batch WriteBatch, checkpoint time.Time) ([]HazardEvent, error) {
	if checkpoint.IsZero() {
		return nil, errors.New("checkpoint is required")
	}

	return s.commitBatch(ctx, batch, &checkpoint, nil)
}

func (s *Store) CommitState(ctx context.Context, batch WriteBatch, state StreamState) ([]HazardEvent, error) {
	var object map[string]json.RawMessage

	if state.Offset < 0 || json.Unmarshal(state.Payload, &object) != nil || object == nil {
		return nil, errors.New("stream state requires a nonnegative offset and a JSON object")
	}

	return s.commitBatch(ctx, batch, nil, &state)
}

// LoadState returns pgx.ErrNoRows before the first committed state.
func (s *Store) LoadState(ctx context.Context, source string) (StreamState, error) {
	var state StreamState

	tx, err := s.begin(ctx, true)
	if err != nil {
		return state, err
	}
	defer tx.Rollback(ctx)

	err = tx.QueryRow(ctx, `SELECT committed_offset, payload FROM stream_state WHERE source = $1`, source).Scan(&state.Offset, &state.Payload)
	if err != nil {
		return state, err
	}

	return state, tx.Commit(ctx)
}

// MarkBehind cannot succeed until replicated placement exists. Returning an
// explicit error prevents callers from assuming a copy was safely fenced off.
func (s *Store) MarkBehind(_ context.Context, _ BehindRequest) error {
	return ErrSingleInstance
}

func (s *Store) commitBatch(ctx context.Context, batch WriteBatch, checkpoint *time.Time, state *StreamState) (
	[]HazardEvent,
	error,
) {
	if err := validateBatch(batch); err != nil {
		return nil, err
	}

	if batch.Epoch < 1 {
		return nil, ErrStaleEpoch
	}

	if batch.Now.IsZero() {
		return nil, errors.New("ingestion time is required")
	}

	for _, event := range batch.Events {
		if event.Source != batch.Source {
			return nil, errors.New("event source differs from batch source")
		}
	}

	tx, err := s.begin(ctx, false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var epoch int64
	// Serialize source commits, including state-offset checks, with takeover.
	err = tx.QueryRow(ctx, `SELECT epoch FROM public.source_fence WHERE source = $1 FOR UPDATE`, batch.Source).Scan(&epoch)
	if err != nil {
		return nil, fmt.Errorf("lock source fence: %w", err)
	}

	if epoch != batch.Epoch {
		return nil, ErrStaleEpoch
	}

	if state != nil {
		var previous int64

		err = tx.QueryRow(ctx, `SELECT committed_offset FROM stream_state WHERE source = $1`, batch.Source).Scan(&previous)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}

		if err == nil && state.Offset <= previous {
			return nil, ErrStaleOffset
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO stream_state (source, committed_offset, payload) VALUES ($1, $2, $3::jsonb)
			ON CONFLICT (source) DO UPDATE SET
				committed_offset = EXCLUDED.committed_offset, payload = EXCLUDED.payload`,
			batch.Source, state.Offset, string(state.Payload))
		if err != nil {
			return nil, err
		}
	}

	changed, err := saveEvents(ctx, tx, batch, checkpoint)
	if err != nil {
		return nil, err
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}

	return changed, nil
}
