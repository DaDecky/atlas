//go:build integration

package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"atlas/services/bnpb/aggregator/internal/satellite"
)

func TestIntegrationSatelliteProcessCrash(t *testing.T) {
	frames := satelliteFrames()

	ref, err := satellite.LoadReference("")
	if err != nil {
		t.Fatal(err)
	}

	if cutRaw := os.Getenv("ATLAS_SATELLITE_CRASH_CUT"); cutRaw != "" {
		cut, err := strconv.Atoi(cutRaw)
		if err != nil {
			t.Fatal(err)
		}

		ctx := context.Background()

		store, err := OpenStore(ctx, os.Getenv("TEST_DATABASE_URL"), 2, 4, defaultDBConnectTimeout)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()

		sink := &SatelliteSink{Data: store, Epoch: 1, Reference: ref}

		engine := satellite.New()
		for index, frame := range frames {
			fires, err := engine.Apply(frame)
			if err != nil {
				t.Fatal(err)
			}

			if index+1 == cut && os.Getenv("ATLAS_SATELLITE_CRASH_PHASE") == "before" {
				os.Exit(77)
			}

			raw, _ := engine.Snapshot()
			if err = sink.Commit(ctx, frame.Offset, raw, fires); err != nil {
				t.Fatal(err)
			}

			if index+1 == cut && os.Getenv("ATLAS_SATELLITE_CRASH_PHASE") == "after" {
				os.Exit(77)
			}
		}

		t.Fatal("crash point not reached")
	}

	store := integrationStore(t)
	ctx := context.Background()
	sink := &SatelliteSink{Data: store, Epoch: 1, Reference: ref}

	baseline := satellite.New()
	for _, frame := range frames {
		fires, err := baseline.Apply(frame)
		if err != nil {
			t.Fatal(err)
		}

		raw, _ := baseline.Snapshot()
		if err = sink.Commit(ctx, frame.Offset, raw, fires); err != nil {
			t.Fatal(err)
		}
	}

	wantState, _ := baseline.Snapshot()
	eventBytes := func() []byte {
		t.Helper()

		page, err := store.Read(ctx, Query{Source: "SATELIT", Limit: 100})
		if err != nil {
			t.Fatal(err)
		}

		for i := range page.Data {
			page.Data[i].IngestedAt = time.Time{}
		}

		raw, _ := json.Marshal(page.Data)

		return raw
	}
	wantEvents := eventBytes()

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	for cut := 1; cut <= len(frames); cut++ {
		for _, phase := range []string{"before", "after"} {
			if _, err = store.ingest.Exec(ctx, `TRUNCATE hazard_events,stream_state,publication_outbox`); err != nil {
				t.Fatal(err)
			}

			childCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			child := exec.CommandContext(childCtx, executable, "-test.run=^TestIntegrationSatelliteProcessCrash$")

			child.Env = append(os.Environ(), "ATLAS_SATELLITE_CRASH_CUT="+strconv.Itoa(cut), "ATLAS_SATELLITE_CRASH_PHASE="+phase)
			output, childErr := child.CombinedOutput()

			cancel()

			var exit *exec.ExitError
			if !errors.As(childErr, &exit) || exit.ExitCode() != 77 {
				t.Fatalf("child did not exit at cut %d/%s: %v %s", cut, phase, childErr, output)
			}

			raw, err := sink.Load(ctx)
			if err != nil {
				t.Fatal(err)
			}

			engine := satellite.New()
			if len(raw) > 0 {
				engine, err = satellite.Restore(raw)
				if err != nil {
					t.Fatal(err)
				}
			}

			for _, frame := range frames {
				position, hasPosition := engine.Position()
				if hasPosition && frame.Offset <= position {
					continue
				}

				fires, err := engine.Apply(frame)
				if err != nil {
					t.Fatal(err)
				}

				raw, _ := engine.Snapshot()
				if err = sink.Commit(ctx, frame.Offset, raw, fires); err != nil {
					t.Fatal(err)
				}
			}

			gotState, _ := engine.Snapshot()
			if !bytes.Equal(gotState, wantState) || !bytes.Equal(eventBytes(), wantEvents) {
				t.Fatalf("process crash changed state/events/revisions at cut %d/%s", cut, phase)
			}
		}
	}

	t.Log("14 real process exits before/after commits preserved state, fire identities, event contents and revisions; ingestion wall time excluded")
}
