package aggregator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type relayRecorder struct {
	mu   sync.Mutex
	sent []string
	fail bool
}

func (r *relayRecorder) Publish(_ context.Context, p Publication) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.fail {
		return errors.New("broker down")
	}

	r.sent = append(r.sent, p.Event.HazardID)

	return nil
}

func (r *relayRecorder) PublishCritical(_ context.Context, p Publication) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.sent = append(r.sent, "critical:"+p.Event.HazardID)

	return nil
}

func (r *relayRecorder) Close() {}

func TestIntegrationOutboxAtomicPriorityAndRetry(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	regular := contractEvent("BMKG", "regular", now)
	if _, err := s.Save(ctx, "BMKG", []HazardEvent{regular}, nil, now); err != nil {
		t.Fatal(err)
	}

	critical := contractEvent("SENSOR_PANTAI", "critical", now)

	critical.Attributes["critical"] = []byte(`true`)
	if _, err := s.Upsert(ctx, WriteBatch{Source: "SENSOR_PANTAI", Epoch: 1, Events: []HazardEvent{critical}, Now: now}); err != nil {
		t.Fatal(err)
	}

	r := &relayRecorder{fail: true}
	if _, err := s.RelayOne(ctx, r); err == nil {
		t.Fatal("failed confirmation acknowledged")
	}

	r.fail = false

	r.sent = nil
	if sent, err := s.RelayOne(ctx, r); err != nil || !sent {
		t.Fatal(sent, err)
	}

	if len(r.sent) != 2 || r.sent[0] != "critical:"+critical.HazardID || r.sent[1] != critical.HazardID {
		t.Fatal("critical lane not first", r.sent)
	}

	if sent, err := s.RelayOne(ctx, r); err != nil || !sent {
		t.Fatal(sent, err)
	}

	if sent, err := s.RelayOne(ctx, r); err != nil || sent {
		t.Fatal("outbox not drained", sent, err)
	}
}
