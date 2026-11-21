// Package placement owns deterministic shard assignment and versioned placement.
package placement

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

// State is the role of one copy of a shard. Only Transition changes it.
type State string

type Copy struct {
	Node    string `json:"node"`
	State   State  `json:"state"`
	StoreID string `json:"store_id,omitempty"`
}

type Table struct {
	Generation   int64             `json:"generation"`
	Nodes        []string          `json:"nodes"`
	Shards       map[string][]Copy `json:"shards"`
	WriterEpochs map[string]int64  `json:"writer_epochs"`
}

const (
	Serving   State = "serving"
	Behind    State = "behind"
	Refilling State = "refilling"
)

const Shards = 48

func Shard(key string) string {
	h := sha256.Sum256([]byte(key))
	return fmt.Sprintf("shard-%d", binary.BigEndian.Uint64(h[:8])%Shards)
}

func New(nodes []string) (Table, error) {
	t := Table{Generation: 1, Nodes: append([]string(nil), nodes...), Shards: map[string][]Copy{}, WriterEpochs: map[string]int64{}}
	sort.Strings(t.Nodes)

	if len(nodes) < 3 {
		return t, errors.New("replicated placement requires at least three nodes")
	}

	for i, n := range t.Nodes {
		if n == "" || (i > 0 && n == t.Nodes[i-1]) {
			return t, errors.New("nodes must be unique and nonempty")
		}
	}

	for i := 0; i < Shards; i++ {
		shard := fmt.Sprintf("shard-%d", i)

		ranked := append([]string(nil), t.Nodes...)
		sort.Slice(ranked, func(i, j int) bool { return score(shard, ranked[i]) > score(shard, ranked[j]) })
		t.Shards[shard] = []Copy{{Node: ranked[0], State: Serving}, {Node: ranked[1], State: Serving}}
	}

	return t, nil
}

func score(shard, node string) uint64 {
	h := sha256.Sum256([]byte(shard + "\x00" + node))
	return binary.BigEndian.Uint64(h[:8])
}

func (t Table) Validate() error {
	if t.Generation < 1 || len(t.Nodes) < 3 || len(t.Shards) != Shards || t.WriterEpochs == nil {
		return errors.New("invalid placement shape")
	}

	for source, epoch := range t.WriterEpochs {
		if source == "" || epoch < 1 {
			return errors.New("invalid writer epoch")
		}
	}

	nodes := map[string]bool{}
	for _, n := range t.Nodes {
		if n == "" || nodes[n] {
			return errors.New("invalid node")
		}

		nodes[n] = true
	}

	for i := 0; i < Shards; i++ {
		copies := t.Shards[fmt.Sprintf("shard-%d", i)]
		if len(copies) != 2 || copies[0].Node == copies[1].Node {
			return errors.New("each shard needs two distinct copies")
		}

		for _, c := range copies {
			if !nodes[c.Node] || (c.State != Serving && c.State != Behind && c.State != Refilling) {
				return errors.New("invalid copy")
			}
		}
	}

	return nil
}

// Transition moves the copy of shard on node to the given state. Any copy may fall behind,
// only a copy that is behind may refill, and only a refilled copy may serve.
func (t *Table) Transition(shard, node string, to State) error {
	for i, holder := range t.Shards[shard] {
		if holder.Node != node {
			continue
		}

		if !allowed(holder.State, to) {
			return fmt.Errorf("copy on %s of %s cannot move from %s to %s", node, shard, holder.State, to)
		}

		t.Shards[shard][i].State = to

		return nil
	}

	return fmt.Errorf("%s holds no copy of %s", node, shard)
}

func allowed(from, to State) bool {
	switch to {
	case Behind:
		return true
	case Refilling:
		return from == Behind || from == Refilling
	case Serving:
		return from == Refilling
	}

	return false
}
