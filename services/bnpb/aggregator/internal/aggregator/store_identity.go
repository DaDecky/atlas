package aggregator

import (
	"context"

	"atlas/services/bnpb/aggregator/internal/placement"
)

func (s *Store) identity(ctx context.Context) (string, error) {
	var id string

	err := s.query.QueryRow(ctx, `SELECT id FROM public.store_identity WHERE singleton`).Scan(&id)

	return id, err
}

// Bind each operation to the disk incarnation recorded in placement. Checking
// inside begin also protects readers before the planner notices a wiped disk.
func (c *Cluster) copyStore(holder placement.Copy, shard string) *Store {
	s := *c.Nodes[holder.Node].Shards[shard]
	s.storeID = holder.StoreID

	s.deadline = c.Limits.ShardDeadline

	return &s
}

// detectStore marks every affected copy in one CAS before creating schemas or
// resealing a returning store. Missing IDs are bound once for existing installs.
func (c *Cluster) detectStore(ctx context.Context, name string) (bool, error) {
	call, cancel := c.timeout(ctx)
	defer cancel()

	node := c.Nodes[name]

	id, err := node.Store.identity(call)
	if err != nil {
		return false, err
	}

	var count int
	if err = node.Store.query.QueryRow(call, `SELECT count(*) FROM information_schema.tables WHERE table_name='hazard_events' AND table_schema LIKE 'shard_%'`).Scan(&count); err != nil {
		return false, err
	}

	err = placement.Update(call, c.Placement, func(t *placement.Table) (bool, error) {
		changed := false

		for shard, copies := range t.Shards {
			for i, holder := range copies {
				if holder.Node != name {
					continue
				}

				if count < placement.Shards || (holder.StoreID != "" && holder.StoreID != id) {
					if err := t.Transition(shard, name, placement.Behind); err != nil {
						return false, err
					}

					changed = true
				}

				if holder.StoreID != id {
					t.Shards[shard][i].StoreID = id
					changed = true
				}
			}
		}

		return changed, nil
	})

	return count < placement.Shards, err
}
