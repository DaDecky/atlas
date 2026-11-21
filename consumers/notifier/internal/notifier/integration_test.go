//go:build integration

package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *lockedBuffer) count(text string) int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return strings.Count(b.buf.String(), text)
}

func publishAlert(t *testing.T, url string, revision int64) {
	t.Helper()

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}

	if _, err = ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}

	body, err := json.Marshal(alert{HazardID: "hz-notifier-it", Revision: revision, Severity: "AWAS", IngestedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}

	err = ch.PublishWithContext(context.Background(), "", queue, false, false, amqp.Publishing{DeliveryMode: amqp.Persistent, Body: body})
	if err != nil {
		t.Fatal(err)
	}
}

func runUntil(t *testing.T, cfg Config, logs *lockedBuffer, done func() bool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	result := make(chan error, 1)

	go func() { result <- Run(ctx, cfg, slog.New(slog.NewJSONHandler(logs, nil))) }()

	for ctx.Err() == nil && !done() {
		time.Sleep(50 * time.Millisecond)
	}

	cancel()

	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationAlertAppliedOnceAcrossRestart(t *testing.T) {
	url := os.Getenv("CONSUMER_TEST_AMQP_URL")
	if url == "" {
		t.Fatal("CONSUMER_TEST_AMQP_URL is not set")
	}

	cfg := Config{
		AMQPURL: url, StatePath: filepath.Join(t.TempDir(), "delivered.txt"), HealthAddr: "127.0.0.1:0", Prefetch: 1,
		HTTPReadHeaderTimeout: time.Second, HTTPReadTimeout: time.Second, HTTPWriteTimeout: time.Second,
		HTTPIdleTimeout: time.Second, ShutdownTimeout: time.Second, BrokerDialTimeout: time.Second,
		BrokerHeartbeat: 10 * time.Second, BrokerReconnectDelay: 100 * time.Millisecond,
	}
	logs := &lockedBuffer{}

	publishAlert(t, url, 1)
	runUntil(t, cfg, logs, func() bool { return logs.count(`"msg":"field_alert_delivered"`) == 1 })

	publishAlert(t, url, 1)
	runUntil(t, cfg, logs, func() bool { return logs.count(`"msg":"duplicate_alert"`) == 1 })

	if got := logs.count(`"msg":"field_alert_delivered"`); got != 1 {
		t.Fatalf("alert delivered %d times across a restart, want 1", got)
	}
}
