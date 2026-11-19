package aggregator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"atlas/services/bnpb/aggregator/internal/placement"
)

type PlacementLease interface{ Acquire(context.Context) error }

// RunRecovery is the placement planner: recover missing schemas, fence the
// restored stores, rebuild behind copies, then restore two serving copies.
func (c *Cluster) RunRecovery(ctx context.Context, lease PlacementLease, onError func(error)) {
	for ctx.Err() == nil {
		if err := lease.Acquire(ctx); err != nil {
			if !errors.Is(err, placement.ErrLeaseHeld) {
				onError(err)
			}

			if !pause(ctx, 2*time.Second) {
				return
			}

			continue
		}

		// A renewing lease must not keep a stalled cycle alive indefinitely.
		cycle, cancel := context.WithTimeout(ctx, 2*time.Minute)

		renewDone := make(chan struct{})
		go func() {
			defer close(renewDone)

			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-cycle.Done():
					return
				case <-ticker.C:
					if err := lease.Acquire(cycle); err != nil {
						cancel()
						return
					}
				}
			}
		}()

		if err := c.RecoverOnce(cycle); err != nil {
			onError(err)
		}

		cancel()
		<-renewDone

		if !pause(ctx, 2*time.Second) {
			return
		}
	}
}

func (c *Cluster) RecoverOnce(ctx context.Context) error {
	var failures []error

	healthy := map[string]bool{}

	for name := range c.Nodes {
		missing, err := c.detectStore(ctx, name)
		if err == nil {
			err = c.prepareStore(ctx, name, missing)
		}

		if err != nil {
			failures = append(failures, fmt.Errorf("store %s: %w", name, err))
			continue
		}

		healthy[name] = true
	}

	table, _, err := c.Placement.Load(ctx)
	if err != nil {
		return err
	}

	keys := make([]string, 0, len(table.Shards))
	for shard := range table.Shards {
		keys = append(keys, shard)
	}

	sort.Strings(keys)

	for _, shard := range keys {
		for _, holder := range table.Shards[shard] {
			if holder.State == "serving" || !healthy[holder.Node] {
				continue
			}

			if err := c.Refill(ctx, shard, holder.Node); err != nil {
				failures = append(failures, fmt.Errorf("refill %s/%s: %w", shard, holder.Node, err))
			}
		}
	}
	// Do not move additional copies until the required repairs have completed.
	if len(failures) == 0 {
		return c.RebalanceOnce(ctx)
	}

	return errors.Join(failures...)
}

func (c *Cluster) prepareStore(ctx context.Context, name string, missing bool) error {
	node := c.Nodes[name]

	if missing {
		for _, sql := range []string{schema, m2Schema} {
			call, cancel := c.timeout(ctx)
			_, err := node.Store.ingest.Exec(call, sql)

			cancel()

			if err != nil {
				return err
			}
		}

		for _, s := range node.Shards {
			call, cancel := c.timeout(ctx)
			_, err := node.Store.initShard(call, s.namespace)

			cancel()

			if err != nil {
				return err
			}
		}
	}

	table, _, err := c.Placement.Load(ctx)
	if err != nil {
		return err
	}

	for source, epoch := range table.WriterEpochs {
		call, cancel := c.timeout(ctx)
		err := node.Store.Seal(call, source, epoch)

		cancel()

		if err != nil {
			return err
		}
	}

	return nil
}

// RebalanceOnce uses the current healthy configured membership. Copy moves are
// incremental, retain a serving donor, and never stop the application globally.
func (c *Cluster) RebalanceOnce(ctx context.Context) error {
	names := []string{}

	for name, node := range c.Nodes {
		call, cancel := c.timeout(ctx)
		err := node.Store.Health(call)

		cancel()

		if err == nil {
			names = append(names, name)
		}
	}

	if len(names) < 3 {
		return nil
	}

	desired, err := placement.New(names)
	if err != nil {
		return err
	}

	for shard, want := range desired.Shards {
		for _, target := range want {
			current, _, err := c.Placement.Load(ctx)
			if err != nil {
				return err
			}

			has := false

			for _, copy := range current.Shards[shard] {
				if copy.Node == target.Node {
					has = true
				}
			}

			if has {
				continue
			}

			old := ""

			for _, copy := range current.Shards[shard] {
				keep := false

				for _, w := range want {
					if copy.Node == w.Node {
						keep = true
					}
				}

				if !keep {
					old = copy.Node
					break
				}
			}

			if old == "" {
				continue
			}

			if err = c.MoveCopy(ctx, shard, old, target.Node); err != nil {
				return err
			}
		}
	}

	return nil
}
