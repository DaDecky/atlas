//go:build integration

package pvmbg

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestIntegrationFiveWireShapesSurviveRestart(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	gen := NewGenerator(Seed, time.Unix(1, 0))
	expected := map[string][]byte{}

	for version := 1; version <= 5; version++ {
		r := gen.Next(time.Unix(int64(version+1), 0), version)

		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}

		expected[r.ReportID] = raw
		if err = s.Add(ctx, r); err != nil {
			t.Fatal(err)
		}

		if err = s.SetSchemaVersion(ctx, version); err != nil {
			t.Fatal(err)
		}
	}

	s.Close()

	reopened, err := OpenStore(ctx, os.Getenv("PVMBG_TEST_DATABASE_URL"), defaultDBPoolSize, defaultDBConnectTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if version, err := reopened.SchemaVersion(ctx); err != nil || version != 5 {
		t.Fatal("current schema version lost", version, err)
	}

	reports, err := reopened.Reports(ctx, nil)
	if err != nil || len(reports) != 5 {
		t.Fatal("report persistence", len(reports), err)
	}

	for _, r := range reports {
		raw, err := json.Marshal(r)
		if err != nil || !bytes.Equal(raw, expected[r.ReportID]) {
			t.Fatalf("wire shape changed across restart: %s, %v", r.ReportID, err)
		}
	}

	t.Log("all five wire shapes and current mode survived PostgreSQL reopen")
}
