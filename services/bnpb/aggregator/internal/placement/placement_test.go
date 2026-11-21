package placement

import (
	"reflect"
	"testing"
)

func TestPlacementDistributionAndStableOrder(t *testing.T) {
	a, err := New([]string{"c", "a", "b"})
	if err != nil {
		t.Fatal(err)
	}

	b, _ := New([]string{"b", "c", "a"})
	if !reflect.DeepEqual(a, b) {
		t.Fatal("input order changes placement")
	}

	counts := map[string]int{}

	for _, copies := range a.Shards {
		for _, c := range copies {
			counts[c.Node]++
		}
	}

	for _, n := range a.Nodes {
		if counts[n] < 15 || counts[n] >= Shards {
			t.Fatal("unbalanced/full-node placement", counts)
		}
	}

	keys := map[string]int{}
	for i := 0; i < 10000; i++ {
		keys[Shard(string(rune(i)))]++
	}

	if len(keys) != Shards {
		t.Fatal("unused shards")
	}

	for _, n := range keys {
		if n < 130 || n > 290 {
			t.Fatal("poor hash distribution", n)
		}
	}
}

func TestTransitionAllowsOnlyRefillPath(t *testing.T) {
	for _, tc := range []struct {
		from, to State
		allowed  bool
	}{
		{Serving, Behind, true},
		{Refilling, Behind, true},
		{Behind, Refilling, true},
		{Refilling, Refilling, true},
		{Refilling, Serving, true},
		{Serving, Refilling, false},
		{Behind, Serving, false},
		{Serving, Serving, false},
	} {
		table := Table{Shards: map[string][]Copy{"shard-0": {{Node: "a", State: tc.from}, {Node: "b", State: Serving}}}}

		err := table.Transition("shard-0", "a", tc.to)
		if (err == nil) != tc.allowed {
			t.Errorf("%s to %s: error %v", tc.from, tc.to, err)
		}

		if want := map[bool]State{true: tc.to, false: tc.from}[tc.allowed]; table.Shards["shard-0"][0].State != want {
			t.Errorf("%s to %s left state %s", tc.from, tc.to, table.Shards["shard-0"][0].State)
		}
	}
}

func TestTransitionRejectsUnknownCopy(t *testing.T) {
	table := Table{Shards: map[string][]Copy{"shard-0": {{Node: "a", State: Serving}}}}

	if err := table.Transition("shard-0", "z", Behind); err == nil {
		t.Fatal("unknown copy accepted")
	}
}
