package aggregator

import (
	"context"
	"errors"
	"testing"

	"atlas/services/bnpb/aggregator/internal/placement"
)

// copyCluster holds one empty store per node. The probe tells them apart by namespace, so no database is needed.
func copyCluster(nodes ...string) *Cluster {
	c := &Cluster{Nodes: map[string]*DataNode{}, Limits: DefaultClusterLimits()}

	for _, name := range nodes {
		c.Nodes[name] = &DataNode{Name: name, Shards: map[string]*Store{"shard-0": {namespace: name}}}
	}

	return c
}

func TestAnsweringCopySkipsBehindAndFailedCopies(t *testing.T) {
	c := copyCluster("a", "b", "c")
	copies := []placement.Copy{
		{Node: "a", State: placement.Behind},
		{Node: "b", State: placement.Serving},
		{Node: "c", State: placement.Serving},
	}

	var probed []string

	answered, err := answeringCopy(context.Background(), c, copies, "shard-0", func(_ context.Context, s *Store) error {
		probed = append(probed, s.namespace)

		if s.namespace == "b" {
			return errors.New("copy unavailable")
		}

		return nil
	})
	if err != nil || answered.namespace != "c" {
		t.Fatalf("answered %v, error %v", answered, err)
	}

	for _, node := range probed {
		if node == "a" {
			t.Fatal("a copy that is behind was probed")
		}
	}
}

func TestAnsweringCopyFailsWithoutServingCopy(t *testing.T) {
	c := copyCluster("a", "b")
	copies := []placement.Copy{{Node: "a", State: placement.Behind}, {Node: "b", State: placement.Refilling}}

	if _, err := answeringCopy(context.Background(), c, copies, "shard-0", func(context.Context, *Store) error { return nil }); err == nil {
		t.Fatal("a shard with no serving copy answered")
	}
}

func TestAnsweringCopyFailsWhenEveryCopyFails(t *testing.T) {
	c := copyCluster("a", "b")
	copies := []placement.Copy{{Node: "a", State: placement.Serving}, {Node: "b", State: placement.Serving}}

	if _, err := answeringCopy(context.Background(), c, copies, "shard-0", func(context.Context, *Store) error { return errors.New("down") }); err == nil {
		t.Fatal("a shard whose copies all failed answered")
	}
}

func TestRefillAttemptsOutsideRangeFailsAtStart(t *testing.T) {
	for _, value := range []string{"0", "21", "many"} {
		t.Run(value, func(t *testing.T) {
			validTestEnv(t)
			t.Setenv("REFILL_VERIFY_ATTEMPTS", value)

			if _, err := ConfigFromEnv(); err == nil {
				t.Errorf("REFILL_VERIFY_ATTEMPTS=%s accepted", value)
			}
		})
	}

	validTestEnv(t)

	c, err := ConfigFromEnv()
	if err != nil || c.RefillAttempts != 5 || c.ClusterLimits().RefillAttempts != 5 {
		t.Fatalf("default attempts %d, error %v", c.RefillAttempts, err)
	}
}
