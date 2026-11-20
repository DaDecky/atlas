package aggregator

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"atlas/services/bnpb/aggregator/internal/placement"
)

// Stream snapshots can contain several MiB. Keep their budget separate from
// the deadline used for API reads and writes of ordinary hazard records.
const streamStateDeadline = 3 * time.Second

func (c *Cluster) streamStateHolders(ctx context.Context, table placement.Table, source string) ([]placement.Copy, int64, error) {
	shard := placement.Shard("metadata:" + source)
	copies := table.Shards[shard]

	type result struct {
		holder placement.Copy
		offset int64
		err    error
	}

	results := make(chan result, len(copies))
	pending := 0

	for _, holder := range copies {
		if holder.State != "serving" || c.Nodes[holder.Node] == nil {
			continue
		}

		pending++

		go func() {
			call, cancel := c.timeout(ctx)
			defer cancel()

			offset, err := c.copyStore(holder, shard).streamOffset(call, source)
			results <- result{holder: holder, offset: offset, err: err}
		}()
	}

	if pending == 0 {
		return nil, 0, errors.New("no serving stream state copy")
	}

	latest := int64(-1)

	var (
		holders  []placement.Copy
		failures []error
	)

	for range pending {
		r := <-results
		switch {
		case errors.Is(r.err, pgx.ErrNoRows):
		case r.err != nil:
			failures = append(failures, r.err)
		case r.offset > latest:
			latest = r.offset
			holders = append(holders[:0], r.holder)
		case r.offset == latest:
			holders = append(holders, r.holder)
		}
	}

	if len(holders) != 0 {
		return holders, latest, nil
	}

	if len(failures) != 0 {
		return nil, 0, errors.Join(failures...)
	}

	return nil, 0, pgx.ErrNoRows
}

func (s *Store) streamOffset(ctx context.Context, source string) (int64, error) {
	tx, err := s.begin(ctx, true)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var offset int64

	err = tx.QueryRow(ctx, "SELECT committed_offset FROM stream_state WHERE source=$1", source).Scan(&offset)

	return offset, err
}

// A client-side error can follow a successful commit. Confirm both the offset
// and snapshot on this disk before counting the copy as written.
func (s *Store) verifyStreamCommit(ctx context.Context, source string, state StreamState) bool {
	tx, err := s.begin(ctx, true)
	if err != nil {
		return false
	}
	defer tx.Rollback(ctx)

	var matches bool

	err = tx.QueryRow(ctx, "SELECT committed_offset=$2 AND payload=$3::jsonb FROM stream_state WHERE source=$1",
		source, state.Offset, string(state.Payload)).Scan(&matches)

	return err == nil && matches
}

func (c *Cluster) commitStreamCopies(ctx context.Context, table placement.Table, shard string, b WriteBatch, state StreamState) ([]HazardEvent, error) {
	return c.replicate(ctx, table, shard, b.Source, b.Epoch, streamStateDeadline, func(call context.Context, s *Store) ([]HazardEvent, error) {
		changed, err := s.CommitState(call, b, state)
		if err == nil || errors.Is(err, ErrStaleEpoch) || errors.Is(err, ErrStaleOffset) {
			return changed, err
		}

		verify, cancel := context.WithTimeout(ctx, streamStateDeadline)
		defer cancel()

		if s.verifyStreamCommit(verify, b.Source, state) {
			return changed, nil
		}

		return nil, err
	})
}
