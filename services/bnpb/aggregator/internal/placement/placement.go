// Package placement owns deterministic shard assignment and versioned placement.
package placement

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

type Copy struct {
	Node  string `json:"node"`
	State string `json:"state"`
}
type Table struct {
	Generation   int64             `json:"generation"`
	Nodes        []string          `json:"nodes"`
	Shards       map[string][]Copy `json:"shards"`
	WriterEpochs map[string]int64  `json:"writer_epochs"`
}

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
		t.Shards[shard] = []Copy{{ranked[0], "serving"}, {ranked[1], "serving"}}
	}

	return t, nil
}

func score(shard, node string) uint64 {
	h := sha256.Sum256([]byte(shard + "\x00" + node))
	return binary.BigEndian.Uint64(h[:8])
}

func (t Table) Validate() error {
	if t.Generation < 1 || len(t.Nodes) < 3 || len(t.Shards) != Shards {
		return errors.New("invalid placement shape")
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
			if !nodes[c.Node] || (c.State != "serving" && c.State != "behind" && c.State != "refilling") {
				return errors.New("invalid copy")
			}
		}
	}

	return nil
}
