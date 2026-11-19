package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"atlas/services/bnpb/aggregator/internal/placement"
)

type DataNode struct {
	Name   string
	Store  *Store
	Shards map[string]*Store
}

// Cluster routes writes to both holders and reads one serving copy per shard.
// Placement snapshots are supplied by the watched repository.
type Cluster struct {
	Nodes     map[string]*DataNode
	Placement placement.Repository
	Limits    ClusterLimits
}

// ClusterLimits bound each copy call and the pools of each data node.
type ClusterLimits struct {
	ShardDeadline, HedgeDelay time.Duration
	ReadWorkers               int
	IngestPool, QueryPool     int32
}

// Five attempts follow the default of the Kubernetes client retry on conflict.
const placementAttempts = 5

var _ ShardStore = (*Cluster)(nil)

func OpenDataNode(ctx context.Context, name, url string, limits ClusterLimits) (*DataNode, error) {
	// A fresh Kubernetes rollout can initialize the same node concurrently.
	// CREATE TABLE IF NOT EXISTS alone does not serialize PostgreSQL catalogue
	// inserts. Hold a node-local bootstrap lock across the base and shard DDL.
	bootstrap, err := pgx.Connect(ctx, url)
	if err != nil {
		return nil, err
	}
	defer bootstrap.Close(context.Background())

	guard, err := bootstrap.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer guard.Rollback(context.Background())

	if _, err = guard.Exec(ctx, `SELECT pg_advisory_xact_lock(419031,4)`); err != nil {
		return nil, err
	}

	s, err := OpenStore(ctx, url, limits.IngestPool, limits.QueryPool, defaultDBConnectTimeout)
	if err != nil {
		return nil, err
	}

	coordinator, err := openPool(ctx, url, 4, defaultDBConnectTimeout)
	if err != nil {
		s.Close()
		return nil, err
	}

	s.coordination = coordinator

	n := &DataNode{Name: name, Store: s, Shards: map[string]*Store{}}
	for i := 0; i < placement.Shards; i++ {
		key := fmt.Sprintf("shard-%d", i)

		shard, err := s.initShard(ctx, strings.ReplaceAll(key, "-", "_"))
		if err != nil {
			s.Close()
			return nil, err
		}

		n.Shards[key] = shard
	}

	return n, nil
}

func (c *Cluster) timeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.Limits.ShardDeadline)
}

func (c *Cluster) Health(ctx context.Context) error {
	if _, _, err := c.Placement.Load(ctx); err != nil {
		return err
	}

	for _, node := range c.Nodes {
		call, cancel := c.timeout(ctx)
		err := node.Store.Health(call)

		cancel()

		if err == nil {
			return nil
		}
	}

	return errors.New("all data stores unavailable")
}

func (c *Cluster) Epoch(ctx context.Context, source string) (int64, error) {
	var highest int64

	count := 0

	for _, n := range c.Nodes {
		call, cancel := c.timeout(ctx)
		epoch, err := n.Store.Epoch(call, source)

		cancel()

		if err == nil {
			count++

			if epoch > highest {
				highest = epoch
			}
		}
	}

	if count < len(c.Nodes)/2+1 {
		return 0, errors.New("source epoch majority unavailable")
	}

	return highest, nil
}

func (c *Cluster) Seal(ctx context.Context, source string, epoch int64) error {
	return c.seal(ctx, source, epoch, false)
}

// AcquireWriterEpoch may only run while holding the source lease. A proposal
// must exceed the latest reachable majority epoch and the placement record.
// Strict acceptance and quorum intersection prevent reusing an epoch.
func (c *Cluster) AcquireWriterEpoch(ctx context.Context, source string) (int64, error) {
	highest, err := c.Epoch(ctx, source)
	if err != nil {
		return 0, err
	}

	table, _, err := c.Placement.Load(ctx)
	if err != nil {
		return 0, err
	}

	highest = max(highest, table.WriterEpochs[source])
	if highest == 1<<63-1 {
		return 0, errors.New("source epoch exhausted")
	}

	next := highest + 1
	if err = c.seal(ctx, source, next, true); err != nil {
		return 0, err
	}

	return next, nil
}

func (c *Cluster) seal(ctx context.Context, source string, epoch int64, strict bool) error {
	table, _, err := c.Placement.Load(ctx)
	if err != nil {
		return err
	}

	if epoch < table.WriterEpochs[source] || strict && epoch == table.WriterEpochs[source] {
		return ErrStaleEpoch
	}

	count := 0
	sealed := map[string]bool{}

	for name, n := range c.Nodes {
		call, cancel := c.timeout(ctx)
		err = n.Store.seal(call, source, epoch, strict)

		cancel()

		if errors.Is(err, ErrStaleEpoch) {
			return err
		}

		if err == nil {
			count++
			sealed[name] = true
		}
	}

	if count < len(table.Nodes)/2+1 {
		return errors.New("seal majority unavailable")
	}

	return c.updatePlacement(ctx, func(t *placement.Table) (bool, error) {
		if epoch < t.WriterEpochs[source] || strict && epoch == t.WriterEpochs[source] {
			return false, ErrStaleEpoch
		}
		// An unsealed holder must not serve a stale writer's data after takeover.
		for shard, copies := range t.Shards {
			for _, holder := range copies {
				if sealed[holder.Node] {
					continue
				}

				if err := t.Transition(shard, holder.Node, placement.Behind); err != nil {
					return false, err
				}
			}
		}

		t.WriterEpochs[source] = epoch

		return true, nil
	})
}

// MarkBehind fences one copy out of reads. Only the writer epoch and the incarnation of that
// copy must still hold, so a change to another shard does not refuse the mark.
func (c *Cluster) MarkBehind(ctx context.Context, r BehindRequest) error {
	return c.updatePlacement(ctx, func(t *placement.Table) (bool, error) {
		if t.WriterEpochs[r.Source] != r.Epoch {
			return false, ErrStaleEpoch
		}

		found, serving := false, false

		for _, holder := range t.Shards[r.Shard] {
			if holder.Node == r.Copy {
				found = holder.StoreID == r.StoreID
			} else if holder.State == placement.Serving {
				serving = true
			}
		}

		if !found {
			return false, fmt.Errorf("copy %s of %s was replaced: %w", r.Copy, r.Shard, placement.ErrConflict)
		}

		if !serving {
			return false, errors.New("mark behind requires another serving copy")
		}

		return true, t.Transition(r.Shard, r.Copy, placement.Behind)
	})
}

// updatePlacement applies change to the newest table and swaps it in, and retries when another
// writer swapped first. change sees a fresh table on every attempt, so it rechecks its own rules.
func (c *Cluster) updatePlacement(ctx context.Context, change func(*placement.Table) (bool, error)) error {
	for range placementAttempts {
		t, version, err := c.Placement.Load(ctx)
		if err != nil {
			return err
		}

		changed, err := change(&t)
		if err != nil || !changed {
			return err
		}

		t.Generation++

		err = c.Placement.CompareAndSwap(ctx, version, t)
		if !errors.Is(err, placement.ErrConflict) {
			return err
		}
	}

	return placement.ErrConflict
}

// swapPlacement swaps in t only if the table is still at version. A step whose preconditions
// came from an earlier read uses it, since a retry would apply them to a table they no longer fit.
func (c *Cluster) swapPlacement(ctx context.Context, version string, t placement.Table) error {
	t.Generation++

	return c.Placement.CompareAndSwap(ctx, version, t)
}

func (c *Cluster) Upsert(ctx context.Context, b WriteBatch) ([]HazardEvent, error) {
	return c.write(ctx, b, nil, nil)
}

func (c *Cluster) CommitCheckpoint(ctx context.Context, b WriteBatch, t time.Time) ([]HazardEvent, error) {
	return c.write(ctx, b, &t, nil)
}

func (c *Cluster) CommitState(ctx context.Context, b WriteBatch, s StreamState) ([]HazardEvent, error) {
	return c.write(ctx, b, nil, &s)
}

func (c *Cluster) write(ctx context.Context, b WriteBatch, checkpoint *time.Time, state *StreamState) ([]HazardEvent, error) {
	if err := checkWrite(b, checkpoint, state); err != nil {
		return nil, err
	}

	if !b.sourceLocked {
		release, err := c.lockSource(ctx, b.Source)
		if err != nil {
			return nil, err
		}
		defer release()
	}

	table, _, err := c.Placement.Load(ctx)
	if err != nil {
		return nil, err
	}

	if table.WriterEpochs[b.Source] != b.Epoch {
		return nil, ErrStaleEpoch
	}

	if state != nil {
		prior, err := c.LoadState(ctx, b.Source)
		if err == nil && prior.Offset >= state.Offset {
			return nil, ErrStaleOffset
		}

		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
	}

	if err := c.assignIngestionTimes(ctx, &b); err != nil {
		return nil, err
	}
	// Events commit idempotently before progress; an interrupted distributed batch
	// resumes from prior state. It is not a distributed SQL transaction.
	groups, keys := shardGroups(b.Events)
	changed := map[string]HazardEvent{}

	for _, shard := range keys {
		part := b
		part.Events = groups[shard]

		events, err := c.writeCopies(ctx, table, shard, part, nil, nil)
		if err != nil {
			return nil, err
		}

		for _, e := range events {
			changed[e.HazardID] = e
		}

		table, _, err = c.Placement.Load(ctx)
		if err != nil {
			return nil, err
		}
	}
	{
		meta := b

		meta.Events = nil
		if _, err = c.writeCopies(ctx, table, placement.Shard("metadata:"+b.Source), meta, checkpoint, state); err != nil {
			return nil, err
		}
	}

	out := make([]HazardEvent, 0, len(changed))
	for _, e := range changed {
		out = append(out, e)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].HazardID < out[j].HazardID })

	return out, nil
}

func (c *Cluster) writeCopies(ctx context.Context, t placement.Table, shard string, b WriteBatch, cp *time.Time, state *StreamState) ([]HazardEvent, error) {
	return c.replicate(ctx, t, shard, b.Source, b.Epoch, func(call context.Context, s *Store) ([]HazardEvent, error) {
		switch {
		case state != nil:
			return s.CommitState(call, b, *state)
		case cp != nil:
			return s.CommitCheckpoint(call, b, *cp)
		default:
			return s.Upsert(call, b)
		}
	})
}

func (c *Cluster) Checkpoint(ctx context.Context, source string) (*time.Time, error) {
	return readMetadata(ctx, c, source, func(call context.Context, s *Store) (*time.Time, error) { return s.Checkpoint(call, source) })
}

func (c *Cluster) LoadState(ctx context.Context, source string) (StreamState, error) {
	state, err := readMetadata(ctx, c, source, func(call context.Context, s *Store) (*StreamState, error) {
		value, err := s.LoadState(call, source)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}

		return &value, err
	})
	if err != nil {
		return StreamState{}, err
	}

	if state == nil {
		return StreamState{}, pgx.ErrNoRows
	}

	return *state, nil
}

func (c *Cluster) fanout(ctx context.Context, read func(context.Context, *Store) ([]HazardEvent, error)) ([]HazardEvent, []string, error) {
	t, _, err := c.Placement.Load(ctx)
	if err != nil {
		return nil, nil, err
	}

	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)

	events := []HazardEvent{}
	unavailable := []string{}
	sem := make(chan struct{}, c.Limits.ReadWorkers)

	for shard, copies := range t.Shards {
		wg.Go(func() {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				mu.Lock()

				unavailable = append(unavailable, shard)
				mu.Unlock()

				return
			}

			defer func() { <-sem }()

			rows, err := readCopies(ctx, c, copies, shard, read)
			if err == nil {
				mu.Lock()

				events = append(events, rows...)
				mu.Unlock()

				return
			}

			mu.Lock()

			unavailable = append(unavailable, shard)
			mu.Unlock()
		})
	}

	wg.Wait()
	sort.Strings(unavailable)

	return events, unavailable, nil
}

func (c *Cluster) Read(ctx context.Context, q Query) (Response, error) {
	if _, err := decodeCursor(q); err != nil {
		return Response{}, err
	}

	if q.Limit < 1 || q.Limit > 1000 {
		return Response{}, errors.New("invalid limit")
	}

	more := false

	var mu sync.Mutex

	rows, missing, err := c.fanout(ctx, func(ctx context.Context, s *Store) ([]HazardEvent, error) {
		r, err := s.Read(ctx, q)
		if r.NextCursor != nil {
			mu.Lock()
			more = true
			mu.Unlock()
		}

		return r.Data, err
	})

	out := Response{Data: []HazardEvent{}, Sources: []Freshness{}, Partial: len(missing) > 0, UnavailableUnits: missing}
	if err != nil {
		return out, err
	}

	sortEvents(rows, q.Audit)

	if len(rows) > q.Limit {
		more = true
		rows = rows[:q.Limit]
	}

	out.Data = rows
	if more && len(rows) > 0 {
		next := encodeCursor(q, rows[len(rows)-1])
		out.NextCursor = &next
	}

	for _, source := range sources(q) {
		fresh, err := readMetadata(ctx, c, source, func(call context.Context, store *Store) (Freshness, error) {
			fresh := Freshness{Source: source, Status: "unavailable"}

			tx, err := store.begin(call, true)
			if err != nil {
				return fresh, err
			}
			defer tx.Rollback(call)

			err = tx.QueryRow(call, `SELECT last_success_at,stale_since FROM source_progress WHERE source=$1`, source).Scan(&fresh.LastSuccessAt, &fresh.StaleSince)
			if err == nil {
				// The events sit in 48 shards, so a source that never succeeded counts as having none.
				fresh.Status = deriveFreshness(fresh.LastSuccessAt, fresh.StaleSince, fresh.LastSuccessAt != nil)
			}

			return fresh, err
		})
		if err != nil {
			fresh = Freshness{Source: source, Status: "unavailable"}
		}

		out.Sources = append(out.Sources, fresh)
	}

	return out, nil
}

func sortEvents(rows []HazardEvent, audit bool) {
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]

		ta, tb := a.OccurredAt, b.OccurredAt
		if audit {
			ta, tb = a.IngestedAt, b.IngestedAt
		}

		if ta.Equal(tb) {
			return a.HazardID < b.HazardID
		}

		if audit {
			return ta.Before(tb)
		}

		return ta.After(tb)
	})
}

// lockSource serializes source batches on the metadata holder without holding a
// fence row across connections. A lease takeover still needs majority fencing.
func (c *Cluster) lockSource(ctx context.Context, source string) (func(), error) {
	t, _, err := c.Placement.Load(ctx)
	if err != nil {
		return nil, err
	}

	for _, holder := range t.Shards[placement.Shard("metadata:"+source)] {
		if holder.State != placement.Serving || c.Nodes[holder.Node] == nil {
			continue
		}

		call, cancel := c.timeout(ctx)

		tx, err := c.Nodes[holder.Node].Store.coordination.Begin(call)
		if err == nil {
			_, err = tx.Exec(call, `SELECT pg_advisory_xact_lock(419032,hashtext($1))`, source)
		}

		cancel()

		if err != nil {
			if tx != nil {
				_ = tx.Rollback(ctx)
			}

			continue
		}

		return func() {
			release, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			_ = tx.Rollback(release)
		}, nil
	}

	return nil, errors.New("source coordination unavailable")
}

func checkWrite(b WriteBatch, checkpoint *time.Time, state *StreamState) error {
	if err := validateBatch(b); err != nil {
		return err
	}

	if checkpoint != nil && checkpoint.IsZero() {
		return errors.New("checkpoint required")
	}

	if state != nil {
		var object map[string]json.RawMessage
		if state.Offset < 0 || json.Unmarshal(state.Payload, &object) != nil || object == nil {
			return errors.New("invalid stream state")
		}
	}

	return nil
}

// shardGroups splits events by shard and returns the shards in a fixed order, so
// concurrent writers lock copies in the same order.
func shardGroups(events []HazardEvent) (map[string][]HazardEvent, []string) {
	groups := map[string][]HazardEvent{}

	for _, e := range events {
		key := placement.Shard(e.HazardID)
		groups[key] = append(groups[key], e)
	}

	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return groups, keys
}
