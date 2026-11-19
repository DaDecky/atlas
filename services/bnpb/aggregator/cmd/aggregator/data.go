package main

import (
	"context"
	"net/http"
	"os"
	"time"

	"atlas/services/bnpb/aggregator/internal/aggregator"
)

type runtimeData interface {
	aggregator.ShardStore
	aggregator.AuditData
	aggregator.Parking
	aggregator.Outbox
	aggregator.FieldReportStore
	Close()
}

func openData(ctx context.Context, c aggregator.Config) (runtimeData, error) {
	if path := os.Getenv("CLUSTER_DATA_CONFIG"); path != "" {
		return aggregator.OpenClusterConfig(ctx, path)
	}

	return aggregator.OpenStore(ctx, c.DatabaseURL, c.IngestPool, c.QueryPool, c.DBConnectTimeout)
}

func dataHandler(api *aggregator.API, store runtimeData, c aggregator.Config, replay map[string]aggregator.ReplaySource) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/internal/audit/", (&aggregator.AuditAPI{Store: store, Parking: store, Token: os.Getenv("AUDIT_TOKEN"), Timeout: 10 * time.Second}).Handler())
	ops := &aggregator.OpsAPI{Parking: store, Token: c.InternalToken, Replay: replay}

	if cluster, ok := store.(*aggregator.Cluster); ok {
		mux.Handle("GET /internal/data/metrics", aggregator.DataMetricsHandler(cluster, c.InternalToken))
		ops.Requests = &aggregator.ReplayRequests{URL: c.RabbitMQURL, Heartbeat: c.BrokerHeartbeat}
	}

	mux.Handle("/internal/ops/", ops.Handler())
	mux.Handle("/", api.Handler())

	return mux
}
