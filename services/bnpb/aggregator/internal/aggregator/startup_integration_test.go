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

			var count int
			if err = node.Store.query.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE schemaname LIKE 'shard_%'`).Scan(&count); err != nil || count != 48*8 {
				t.Error("fresh rollout left missing shard tables", count, err)
			}
		})
	}

	close(start)
	wg.Wait()
	t.Log("three simultaneous fresh initializers completed all 48 shard schemas")
}
