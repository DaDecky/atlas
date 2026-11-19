package aggregator

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"atlas/services/bnpb/aggregator/internal/placement"
)

// racingPlacement stands in for the Kubernetes ConfigMap. The first swap loses to a
// concurrent writer that changes another shard, like a planner moving a copy.
type racingPlacement struct {
	table   placement.Table
	version int
	raced   bool
}

func (r *racingPlacement) Load(context.Context) (placement.Table, string, error) {
	return cloneForTest(r.table), strconv.Itoa(r.version), nil
}

func (r *racingPlacement) CompareAndSwap(_ context.Context, version string, t placement.Table) error {
	if !r.raced {
		r.raced = true
		r.table.Shards["shard-1"][0].StoreID = "moved"
		r.table.Generation++
		r.version++

		return placement.ErrConflict
	}

	if version != strconv.Itoa(r.version) {
		return placement.ErrConflict
	}

	r.table, r.version = cloneForTest(t), r.version+1

	return nil
}

func cloneForTest(t placement.Table) placement.Table {
	out := t
	out.Shards = map[string][]placement.Copy{}

	for shard, copies := range t.Shards {
		out.Shards[shard] = append([]placement.Copy(nil), copies...)
	}

	out.WriterEpochs = map[string]int64{}
	for source, epoch := range t.WriterEpochs {
		out.WriterEpochs[source] = epoch
	}

	return out
}

func racingCluster(t *testing.T) (*Cluster, *racingPlacement) {
	t.Helper()

	table, err := placement.New([]string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}

	table.WriterEpochs = map[string]int64{"BMKG": 3}
	repo := &racingPlacement{table: table}

	return &Cluster{Placement: repo, Limits: DefaultClusterLimits()}, repo
}

func TestMarkBehindSurvivesChangeToOtherShard(t *testing.T) {
	c, repo := racingCluster(t)
	holder := repo.table.Shards["shard-0"][1]

	err := c.MarkBehind(context.Background(), BehindRequest{Source: "BMKG", Epoch: 3, Shard: "shard-0", Copy: holder.Node, StoreID: holder.StoreID})
	if err != nil {
		t.Fatal(err)
	}

	if got := repo.table.Shards["shard-0"][1].State; got != placement.Behind {
		t.Fatalf("copy state %s", got)
	}
}

func TestMarkBehindRefusesReplacedCopy(t *testing.T) {
	c, repo := racingCluster(t)
	holder := repo.table.Shards["shard-0"][1]

	err := c.MarkBehind(context.Background(), BehindRequest{Source: "BMKG", Epoch: 3, Shard: "shard-0", Copy: holder.Node, StoreID: "older"})
	if !errors.Is(err, placement.ErrConflict) {
		t.Fatalf("replaced copy marked: %v", err)
	}
}

func TestMarkBehindRefusesStaleEpoch(t *testing.T) {
	c, repo := racingCluster(t)
	holder := repo.table.Shards["shard-0"][1]

	err := c.MarkBehind(context.Background(), BehindRequest{Source: "BMKG", Epoch: 2, Shard: "shard-0", Copy: holder.Node, StoreID: holder.StoreID})
	if !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("stale writer marked a copy: %v", err)
	}
}

func TestGuardedSwapFailsAfterConcurrentChange(t *testing.T) {
	c, repo := racingCluster(t)

	table, version, err := c.Placement.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if err = c.swapPlacement(context.Background(), version, table); !errors.Is(err, placement.ErrConflict) {
		t.Fatalf("guarded swap retried past a change: %v", err)
	}

	if repo.table.Shards["shard-1"][0].StoreID != "moved" {
		t.Fatal("concurrent change lost")
	}
}
