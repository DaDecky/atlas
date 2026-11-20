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
	Deadline  time.Duration
}

var _ ShardStore = (*Cluster)(nil)

func OpenDataNode(ctx context.Context, name, url string) (*DataNode, error) {
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

	s, err := OpenStore(ctx, url, 4, 8, defaultDBConnectTimeout)
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
	d := c.Deadline
	if d <= 0 {
		d = shardDeadline
	}

	return context.WithTimeout(ctx, d)
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
	table, v, err := c.Placement.Load(ctx)
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
	// An unsealed holder must not serve a stale writer's data after takeover.
	for shard, copies := range table.Shards {
		for i, copy := range copies {
			if !sealed[copy.Node] {
				table.Shards[shard][i].State = "behind"
			}
		}
	}

	table.WriterEpochs[source] = epoch
	table.Generation++

	return c.Placement.CompareAndSwap(ctx, v, table)
}

func (c *Cluster) MarkBehind(ctx context.Context, r BehindRequest) error {
	table, v, err := c.Placement.Load(ctx)
	if err != nil {
		return err
	}

	if table.Generation != r.Generation {
		return placement.ErrConflict
	}

	if table.WriterEpochs[r.Source] != r.Epoch {
		return ErrStaleEpoch
	}

	copies, ok := table.Shards[r.Shard]
	if !ok {
		return errors.New("unknown shard")
	}

	found, serving := false, false

	for i := range copies {
		if copies[i].Node == r.Copy {
			copies[i].State = "behind"
			found = true
		} else if copies[i].State == "serving" {
			serving = true
		}
	}

	if !found {
		return errors.New("unknown copy")
	}

	if !serving {
		return errors.New("mark behind requires another serving copy")
	}

	table.Shards[r.Shard] = copies
	table.Generation++

	return c.Placement.CompareAndSwap(ctx, v, table)
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
	if err := validateBatch(b); err != nil {
		return nil, err
	}

	if checkpoint != nil && checkpoint.IsZero() {
		return nil, errors.New("checkpoint required")
	}

	if state != nil {
		var object map[string]json.RawMessage
		if state.Offset < 0 || json.Unmarshal(state.Payload, &object) != nil || object == nil {
			return nil, errors.New("invalid stream state")
		}
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
		_, prior, err := c.streamStateHolders(ctx, table, b.Source)
		if err == nil && prior >= state.Offset {
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
	groups := map[string][]HazardEvent{}

	for _, e := range b.Events {
		key := placement.Shard(e.HazardID)
		groups[key] = append(groups[key], e)
	}

	changed := map[string]HazardEvent{}

	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}

	sort.Strings(keys)

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
	if state != nil {
		return c.commitStreamCopies(ctx, t, shard, b, *state)
	}

	return c.replicate(ctx, t, shard, b.Source, b.Epoch, 0, func(call context.Context, s *Store) ([]HazardEvent, error) {
		if cp != nil {
			return s.CommitCheckpoint(call, b, *cp)
		}

		return s.Upsert(call, b)
	})
}

func (c *Cluster) Checkpoint(ctx context.Context, source string) (*time.Time, error) {
	return readMetadata(ctx, c, source, func(call context.Context, s *Store) (*time.Time, error) { return s.Checkpoint(call, source) })
}

func (c *Cluster) LoadState(ctx context.Context, source string) (StreamState, error) {
	table, _, err := c.Placement.Load(ctx)
	if err != nil {
		return StreamState{}, err
	}

	holders, _, err := c.streamStateHolders(ctx, table, source)
	if err != nil {
		return StreamState{}, err
	}

	return readCopies(ctx, c, holders, placement.Shard("metadata:"+source), streamStateDeadline, func(call context.Context, store *Store) (StreamState, error) {
		return store.LoadState(call, source)
	})
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
	sem := make(chan struct{}, shardReadWorkers)

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

			rows, err := readCopies(ctx, c, copies, shard, 0, read)
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
			if err == nil && fresh.LastSuccessAt != nil {
				fresh.Status = "healthy"
				if fresh.StaleSince != nil {
					fresh.Status = "stale"
				}
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

	for _, copy := range t.Shards[placement.Shard("metadata:"+source)] {
		if copy.State != "serving" || c.Nodes[copy.Node] == nil {
			continue
		}

		call, cancel := c.timeout(ctx)

		tx, err := c.Nodes[copy.Node].Store.coordination.Begin(call)
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
