package aggregator

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"atlas/services/bnpb/aggregator/internal/placement"
)

func TestDataIntegrationConcurrentFreshNodeInitialization(t *testing.T) {
	databaseURL := os.Getenv("DATA_TEST_D")
	if databaseURL == "" {
		t.Skip("run make data-integration")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	adminURL := os.Getenv("DATA_TEST_ADMIN_D")
	if adminURL == "" {
		t.Skip("run make data-integration with its disposable admin connection")
	}

	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatal(err)
	}

	defer admin.Close(context.Background())

	name := fmt.Sprintf("atlas_startup_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()+" OWNER aggregator"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
	}()

	config, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}

	config.Path = "/" + name
	start := make(chan struct{})

	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			<-start

			node, err := OpenDataNode(ctx, "fresh", config.String())
			if err != nil {
				t.Error("simultaneous fresh rollout failed", err)
				return
			}
			defer node.Store.Close()

			var missing int

			err = node.Store.query.QueryRow(ctx, `
				SELECT count(*) FROM generate_series(0, $1::int - 1) AS shard(id)
				CROSS JOIN unnest(ARRAY['hazard_events', 'source_progress', 'schema_migrations',
					'source_fence', 'stream_state', 'publication_outbox', 'unprocessed_messages',
					'field_reports_inbox', 'ingestion_clock']) AS expected(name)
				WHERE NOT EXISTS (SELECT FROM pg_tables
					WHERE schemaname = 'shard_' || shard.id AND tablename = expected.name)`, placement.Shards).Scan(&missing)
			if err != nil || missing != 0 {
				t.Errorf("fresh rollout left %d missing shard tables: %v", missing, err)
			}
		})
	}

	close(start)
	wg.Wait()
	t.Logf("three simultaneous fresh initializers completed all %d shard schemas", placement.Shards)
}
