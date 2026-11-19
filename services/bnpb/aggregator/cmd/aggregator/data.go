package main

import (
	"context"
	"errors"
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
	Close()
}

func openData(ctx context.Context, c aggregator.Config) (runtimeData, error) {
	if path := os.Getenv("M2_DATA_CONFIG"); path != "" {
		return aggregator.OpenClusterConfig(ctx, path)
	}

	return aggregator.OpenStore(ctx, c.DatabaseURL, c.IngestPool, c.QueryPool, c.DBConnectTimeout)
}

func dataHandler(api *aggregator.API, store runtimeData, c aggregator.Config) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/internal/audit/", (&aggregator.AuditAPI{Store: store, Parking: store, Token: os.Getenv("AUDIT_TOKEN"), Timeout: 10 * time.Second}).Handler())

	if cluster, ok := store.(*aggregator.Cluster); ok {
		mux.Handle("GET /internal/data/metrics", aggregator.DataMetricsHandler(cluster, c.InternalToken))
	}

	mux.Handle("/", api.Handler())

	return mux
}

func ingestData(store runtimeData, source string) (aggregator.IngestStore, error) {
	if local, ok := store.(*aggregator.Store); ok {
		return local, nil
	}
	// Fail closed instead of running three uncoordinated M1 pollers. Razi starts
	// EpochIngest only while holding the source Leadership contract.
	return nil, errors.New("cluster ingestion requires source leadership; set M2_READ_ONLY=1 to serve data routes only")
}
