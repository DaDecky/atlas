package aggregator

import (
	"context"
	"errors"
	"time"

	"atlas/services/bnpb/aggregator/internal/placement"
)

type copyResult struct {
	holder placement.Copy
	events []HazardEvent
	err    error
}

type readResult[T any] struct {
	value T
	err   error
}

const (
	// Leave enough of the 2 s request budget for fallback and shard queueing.
	shardDeadline = 400 * time.Millisecond
	// Hedge a slow copy without doubling the normal read traffic.
	readHedgeDelay = 150 * time.Millisecond
	// Bound database pressure while allowing 48 shards to share the request budget.
	shardReadWorkers = 8
)

// Each copy has its own deadline. A failed copy is fenced out of reads before
// acknowledging a degraded write, including failures while parking a message.
func (c *Cluster) replicate(ctx context.Context, t placement.Table, shard, source string, epoch int64, write func(context.Context, *Store) ([]HazardEvent, error)) ([]HazardEvent, error) {
	results := make(chan copyResult, len(t.Shards[shard]))
	pending := 0

	for _, holder := range t.Shards[shard] {
		if holder.State == "behind" {
			continue
		}

		pending++

		go func() {
			call, cancel := c.timeout(ctx)
			defer cancel()

			result := copyResult{holder: holder, err: errors.New("copy node unavailable")}
			if c.Nodes[holder.Node] != nil {
				result.events, result.err = write(call, c.copyStore(holder, shard))
			}

			results <- result
		}()
	}

	var (
		changed  []HazardEvent
		failures []error
	)

	succeeded, replay := 0, false

	for range pending {
		r := <-results
		switch {
		case errors.Is(r.err, ErrStaleEpoch):
			failures = append(failures, r.err)
		case errors.Is(r.err, ErrStaleOffset):
			replay = true
			succeeded++
		case r.err != nil:
			if err := c.markFailedCopy(ctx, shard, source, epoch, r.holder); err != nil {
				failures = append(failures, err)
			}
		default:
			succeeded++

			if len(r.events) > len(changed) {
				changed = r.events
			}
		}
	}

	if len(failures) != 0 {
		return nil, errors.Join(failures...)
	}

	if succeeded == 0 {
		return nil, errors.New("no serving write copy available")
	}

	if replay && len(changed) == 0 {
		return nil, ErrStaleOffset
	}

	return changed, nil
}

func (c *Cluster) markFailedCopy(ctx context.Context, shard, source string, epoch int64, holder placement.Copy) error {
	for range 5 {
		t, _, err := c.Placement.Load(ctx)
		if err != nil {
			return err
		}

		found := false

		for _, current := range t.Shards[shard] {
			if current.Node == holder.Node && current.StoreID == holder.StoreID {
				found = true
			}
		}

		if !found {
			return placement.ErrConflict
		}

		err = c.MarkBehind(ctx, BehindRequest{Source: source, Epoch: epoch, Shard: shard, Copy: holder.Node, Generation: t.Generation})
		if !errors.Is(err, placement.ErrConflict) {
			return err
		}
	}

	return placement.ErrConflict
}

// Results belong to individual attempts; a canceled loser never mutates the
// winner's output. A buffered channel lets losing database calls finish safely.
func readCopies[T any](ctx context.Context, c *Cluster, copies []placement.Copy, shard string, read func(context.Context, *Store) (T, error)) (T, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		zero    T
		serving []placement.Copy
	)

	for _, holder := range copies {
		if holder.State == "serving" && c.Nodes[holder.Node] != nil {
			serving = append(serving, holder)
		}
	}

	if len(serving) == 0 {
		return zero, errors.New("no serving copy")
	}

	results := make(chan readResult[T], len(serving))
	started, finished := 0, 0
	start := func() {
		holder := serving[started]
		store := c.copyStore(holder, shard)
		started++

		go func() {
			call, stop := c.timeout(ctx)
			defer stop()

			value, err := read(call, store)
			results <- readResult[T]{value, err}
		}()
	}
	start()

	timer := time.NewTimer(readHedgeDelay)
	defer timer.Stop()

	var last error

	for finished < len(serving) {
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-timer.C:
			if started < len(serving) {
				start()
			}
		case r := <-results:
			finished++

			if r.err == nil {
				return r.value, nil
			}

			last = r.err

			if started < len(serving) {
				start()
			}
		}
	}

	return zero, last
}

func readMetadata[T any](ctx context.Context, c *Cluster, source string, read func(context.Context, *Store) (T, error)) (T, error) {
	t, _, err := c.Placement.Load(ctx)
	if err != nil {
		var zero T
		return zero, err
	}

	shard := placement.Shard("metadata:" + source)

	return readCopies(ctx, c, t.Shards[shard], shard, read)
}
