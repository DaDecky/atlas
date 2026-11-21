package notifier

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// broker records what the consumer answers, in place of the RabbitMQ channel.
type broker struct{ acks, requeues, drops int }

func (b *broker) Ack(uint64, bool) error { b.acks++; return nil }

func (b *broker) Nack(_ uint64, _ bool, requeue bool) error {
	if requeue {
		b.requeues++
	} else {
		b.drops++
	}

	return nil
}

func (b *broker) Reject(_ uint64, requeue bool) error { return b.Nack(0, false, requeue) }

func delivery(t *testing.T, b *broker, revision int64, ingested time.Time) amqp.Delivery {
	t.Helper()

	body, err := json.Marshal(alert{HazardID: "hz-7f3a91", Revision: revision, Severity: "AWAS", AreaName: "Padang", IngestedAt: ingested})
	if err != nil {
		t.Fatal(err)
	}

	return amqp.Delivery{Acknowledger: b, Body: body, CorrelationId: "corr-1"}
}

func newState(t *testing.T) *ProcessedState {
	t.Helper()

	state, err := OpenProcessedState(filepath.Join(t.TempDir(), "delivered.txt"))
	if err != nil {
		t.Fatal(err)
	}

	return state
}

func TestRedeliveredAlertIsAppliedOnce(t *testing.T) {
	var logs bytes.Buffer

	b, state, log := &broker{}, newState(t), slog.New(slog.NewJSONHandler(&logs, nil))
	now := time.Now()

	for range 2 {
		if err := handle(state, delivery(t, b, 1, now), now, log); err != nil {
			t.Fatal(err)
		}
	}

	if got := strings.Count(logs.String(), `"msg":"field_alert_delivered"`); got != 1 {
		t.Fatalf("delivered %d times, want 1", got)
	}

	if !strings.Contains(logs.String(), `"msg":"duplicate_alert"`) || b.acks != 2 {
		t.Fatalf("duplicate not acknowledged: acks %d, log %s", b.acks, logs.String())
	}
}

func TestNewRevisionIsDeliveredAgain(t *testing.T) {
	var logs bytes.Buffer

	b, state, log := &broker{}, newState(t), slog.New(slog.NewJSONHandler(&logs, nil))
	now := time.Now()

	for _, revision := range []int64{1, 2} {
		if err := handle(state, delivery(t, b, revision, now), now, log); err != nil {
			t.Fatal(err)
		}
	}

	if got := strings.Count(logs.String(), `"msg":"field_alert_delivered"`); got != 2 {
		t.Fatalf("delivered %d revisions, want 2", got)
	}
}

func TestLatencyCountsFromIngestion(t *testing.T) {
	var logs bytes.Buffer

	ingested := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	received := ingested.Add(1500 * time.Millisecond)

	if err := handle(newState(t), delivery(t, &broker{}, 1, ingested), received, slog.New(slog.NewJSONHandler(&logs, nil))); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(logs.String(), `"latency_ms":1500`) {
		t.Fatalf("latency missing: %s", logs.String())
	}
}

func TestInvalidAlertIsDroppedNotRequeued(t *testing.T) {
	b := &broker{}

	err := handle(newState(t), amqp.Delivery{Acknowledger: b, Body: []byte(`{"revision":1}`)}, time.Now(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}

	if b.drops != 1 || b.requeues != 0 || b.acks != 0 {
		t.Fatalf("drops %d, requeues %d, acks %d", b.drops, b.requeues, b.acks)
	}
}

func TestPrefetchOutsideRangeFailsAtStart(t *testing.T) {
	t.Setenv("AMQP_URL", "amqp://localhost/")
	t.Setenv("CONSUMER_PREFETCH", "0")

	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("prefetch 0 was accepted")
	}
}
