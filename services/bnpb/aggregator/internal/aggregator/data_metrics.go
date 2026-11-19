package aggregator

import (
	"context"
	"fmt"
	"net/http"
	"sort"
)

type NodeMetric struct {
	Node        string `json:"node"`
	Records     int64  `json:"records"`
	Bytes       int64  `json:"bytes"`
	Inserts     int64  `json:"inserts"`
	Updates     int64  `json:"updates"`
	Unavailable bool   `json:"unavailable"`
}

func (c *Cluster) NodeMetrics(ctx context.Context) ([]NodeMetric, error) {
	t, _, err := c.Placement.Load(ctx)
	if err != nil {
		return nil, err
	}

	result := []NodeMetric{}

	for _, name := range t.Nodes {
		m := NodeMetric{Node: name}

		node := c.Nodes[name]
		if node == nil {
			m.Unavailable = true
			result = append(result, m)

			continue
		}

		for shard := range node.Shards {
			call, cancel := c.timeout(ctx)

			tx, err := node.Shards[shard].begin(call, true)
			if err == nil {
				var count, size, inserts, updates int64

				err = tx.QueryRow(call, `SELECT (SELECT count(*) FROM hazard_events),pg_total_relation_size('hazard_events'),coalesce(n_tup_ins,0),coalesce(n_tup_upd,0) FROM pg_stat_user_tables WHERE schemaname=current_schema() AND relname='hazard_events'`).Scan(&count, &size, &inserts, &updates)
				m.Records += count
				m.Bytes += size
				m.Inserts += inserts
				m.Updates += updates
				_ = tx.Rollback(call)
			}

			cancel()

			if err != nil {
				m.Unavailable = true
				break
			}
		}

		result = append(result, m)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Node < result[j].Node })

	return result, nil
}

func DataMetricsHandler(c *Cluster, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !bearerMatches(r, token) {
			auditError(w, 401, "unauthorized", "token tidak valid")
			return
		}

		metrics, err := c.NodeMetrics(r.Context())
		if err != nil {
			auditError(w, 503, "unavailable", "metrics unavailable")
			return
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")

		for _, m := range metrics {
			available := 1
			if m.Unavailable {
				available = 0
			}

			_, _ = fmt.Fprintf(w, "atlas_store_available{node=%q} %d\n", m.Node, available)
			_, _ = fmt.Fprintf(w, "atlas_store_records{node=%q} %d\natlas_store_bytes{node=%q} %d\natlas_store_inserts{node=%q} %d\natlas_store_updates{node=%q} %d\n", m.Node, m.Records, m.Node, m.Bytes, m.Node, m.Inserts, m.Node, m.Updates)
		}
	})
}
