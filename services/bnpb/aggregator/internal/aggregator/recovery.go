package aggregator

import (
	"context"
	"errors"
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

		cycle, cancel := context.WithCancel(ctx)

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
	table, _, err := c.Placement.Load(ctx)
	if err != nil {
		return err
	}

	for name, node := range c.Nodes {
		call, cancel := c.timeout(ctx)
		err = node.Store.Health(call)

		cancel()

		if err != nil {
			continue
		}

		var schemaCount int

		err = node.Store.query.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_name='hazard_events' AND table_schema LIKE 'shard_%'`).Scan(&schemaCount)
		if err != nil {
			return err
		}

		if schemaCount < placement.Shards {
			// Mark affected copies behind BEFORE recreating any wiped schemas.
			for shard, copies := range table.Shards {
				for i, copy := range copies {
					if copy.Node != name {
						continue
					}

					fresh, v, err := c.Placement.Load(ctx)
					if err != nil {
						return err
					}

					fresh.Shards[shard][i].State = "behind"

					fresh.Generation++
					if err = c.Placement.CompareAndSwap(ctx, v, fresh); err != nil {
						return err
					}
				}
			}

			if _, err = node.Store.ingest.Exec(ctx, schema); err != nil {
				return err
			}

			if _, err = node.Store.ingest.Exec(ctx, m2Schema); err != nil {
				return err
			}

			for _, s := range node.Shards {
				if _, err = node.Store.initShard(ctx, s.namespace); err != nil {
					return err
				}
			}
		}

		for source, epoch := range table.WriterEpochs {
			if err = node.Store.Seal(ctx, source, epoch); err != nil {
				return err
			}
		}
	}

	table, _, err = c.Placement.Load(ctx)
	if err != nil {
		return err
	}

	for shard, copies := range table.Shards {
		for _, copy := range copies {
			if copy.State == "serving" {
				continue
			}

			n := c.Nodes[copy.Node]
			if n == nil {
				continue
			}

			call, cancel := c.timeout(ctx)
			err = n.Store.Health(call)

			cancel()

			if err != nil {
				continue
			}

			if err = c.Refill(ctx, shard, copy.Node); err != nil {
				return err
			}
		}
	}

	return nil
}
