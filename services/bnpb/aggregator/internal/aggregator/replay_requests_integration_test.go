package aggregator

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestIntegrationReplayRequestSurvivesHolderReplacement(t *testing.T) {
	url := os.Getenv("TEST_RABBITMQ_URL")
	if url == "" {
		t.Skip("run make integration")
	}

	store := integrationStore(t)

	ctx, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()

	queue := ReplayRequests{URL: url, Heartbeat: 10 * time.Second}

	const source = "PVMBG"

	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		_ = queue.channel(cleanup, source, func(ch *amqp.Channel, _ net.Conn, name string) error {
			_, err := ch.QueueDelete(name, false, false, false)
			return err
		})
	})

	now := time.Now().UTC()

	err := store.Park(ctx, ParkedMessage{MessageID: "queued", Source: source, SourceRefID: "queued", Raw: []byte("{}"), ReasonCode: "schema_violation", Reason: "test", ReceivedAt: now})
	if err != nil {
		t.Fatal(err)
	}

	api := (&OpsAPI{Token: "replay-token", Parking: store, Requests: &queue, Capacity: defaultOpsCapacity}).Handler()
	req := httptest.NewRequest(http.MethodPost, "/internal/ops/replay", strings.NewReader(`{"source":"PVMBG","reason_code":"schema_violation","limit":1}`))
	req.Header.Set("Authorization", "Bearer replay-token")

	resp := httptest.NewRecorder()
	api.ServeHTTP(resp, req)

	if resp.Code != http.StatusAccepted || !strings.Contains(resp.Body.String(), `"status":"queued"`) {
		t.Fatalf("queue request before a holder exists: %d %s", resp.Code, resp.Body)
	}

	observed := make(chan [2]int, 2)

	apply := func(epoch int64) ReplaySource {
		return func(ctx context.Context, limit int, reason string) (int, error) {
			n, err := store.ReplayReason(ctx, source, epoch, limit, reason, func(m ParkedMessage) (HazardEvent, error) {
				return contractEvent(m.Source, m.SourceRefID, m.ReceivedAt), nil
			})
			if err == nil {
				observed <- [2]int{limit, n}
			}

			return n, err
		}
	}

	if err = queue.Serve(ctx, source, apply(2)); !errors.Is(err, ErrStaleEpoch) {
		t.Fatal("stale holder must leave request unacknowledged", err)
	}

	workerCtx, shutdown := context.WithCancel(ctx)
	defer shutdown()

	workerDone := make(chan error, 1)

	go func() { workerDone <- queue.Serve(workerCtx, source, apply(1)) }()

	if _, err = queue.Enqueue(ctx, ReplayRequest{Source: source, Limit: 1, ReasonCode: "schema_violation"}); err != nil {
		t.Fatal(err)
	}

	for i, want := range [][2]int{{1, 1}, {1, 0}} {
		select {
		case got := <-observed:
			if got != want {
				t.Fatalf("attempt %d: got %v, want %v", i, got, want)
			}
		case err := <-workerDone:
			t.Fatal("worker stopped before processing queued requests", err)
		case <-ctx.Done():
			t.Fatal("timeout waiting for replay request")
		}
	}

	page, err := store.Read(ctx, Query{Source: source, Limit: 10})
	if err != nil || len(page.Data) != 1 || page.Data[0].Revision != 1 {
		t.Fatalf("request replay caused duplicate effects: %d %v", len(page.Data), err)
	}

	shutdown()
	<-workerDone
}
