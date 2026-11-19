package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

type timeoutPublisher struct{ cancel context.CancelCauseFunc }

type selectivePublisher struct {
	bad  string
	sent []string
}

func TestCriticalAttributeMustBeBoolean(t *testing.T) {
	for _, raw := range []string{`"true"`, `1`, `null`, `{}`, `false`, `true`, ``} {
		t.Run(raw, func(t *testing.T) {
			h := contractEvent("SENSOR_PANTAI", "critical-type", time.Now().UTC())
			if raw != "" {
				h.Attributes["critical"] = json.RawMessage(raw)
			}

			critical, err := eventCritical(h)

			valid := raw == "" || raw == "false" || raw == "true"
			if valid && (err != nil || critical != (raw == "true")) {
				t.Fatal(critical, err)
			}

			if !valid && (err == nil || !strings.Contains(err.Error(), h.HazardID)) {
				t.Fatal("malformed critical silently accepted", err)
			}
		})
	}
}

func (p *selectivePublisher) Publish(_ context.Context, item Publication) error {
	if item.Event.HazardID == p.bad {
		return errors.New("unpublishable payload")
	}

	p.sent = append(p.sent, item.Event.HazardID)

	return nil
}

func (p *selectivePublisher) PublishCritical(ctx context.Context, item Publication) error {
	return p.Publish(ctx, item)
}

func (p *selectivePublisher) Close() {}

func TestIntegrationOutboxParksPoisonAndRecoversAfterBrokerOutage(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	first := contractEvent("BMKG", "poison", now)

	second := contractEvent("BMKG", "next", now.Add(time.Second))
	if _, err := s.Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: now, Events: []HazardEvent{first, second}}); err != nil {
		t.Fatal(err)
	}

	execData(t, s, `UPDATE public.publication_outbox SET created_at=$2 WHERE hazard_id=$1`, first.HazardID, now.Add(-time.Hour))

	publisher := &selectivePublisher{bad: first.HazardID}
	for range publicationAttempts {
		if sent, err := s.RelayOne(ctx, publisher); err == nil || sent {
			t.Fatal("failed publish acknowledged", sent, err)
		}
	}

	page, err := s.ListUnprocessed(ctx, "BMKG", "", 100)
	if err != nil || len(page.Items) != 1 {
		t.Fatal(page, err)
	}

	parked := page.Items[0]
	if parked.ReasonCode != "publish_failed" || parked.Attempts != 5 || parked.Status != "retrying" || parked.NextAttemptAt == nil {
		t.Fatal(parked)
	}

	if sent, err := s.RelayOne(ctx, publisher); err != nil || !sent {
		t.Fatal("poison blocked next row", sent, err)
	}

	if len(publisher.sent) != 1 || publisher.sent[0] != second.HazardID {
		t.Fatal(publisher.sent)
	}
	// Simulate the automatic retry becoming due after the broker recovers.
	publisher.bad = ""

	execData(t, s, `UPDATE public.unprocessed_messages SET next_attempt_at=now()-interval '1 second' WHERE message_id=$1`, parked.MessageID)

	if sent, err := s.RelayOne(ctx, publisher); err != nil || !sent {
		t.Fatal("parked publication did not recover", sent, err)
	}

	page, err = s.ListUnprocessed(ctx, "BMKG", "", 100)
	if err != nil || len(page.Items) != 0 {
		t.Fatal("successful publication still parked", page, err)
	}

	if len(publisher.sent) != 2 || publisher.sent[1] != first.HazardID {
		t.Fatal("publication lost after outage", publisher.sent)
	}
}

func TestIntegrationOutboxMalformedPayloadDoesNotBlockQueue(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	h := contractEvent("BMKG", "good", now)
	if _, err := s.Upsert(ctx, WriteBatch{Source: "BMKG", Epoch: 1, Now: now, Events: []HazardEvent{h}}); err != nil {
		t.Fatal(err)
	}

	execData(t, s, `INSERT INTO public.publication_outbox(hazard_id,revision,payload,correlation_id,created_at) VALUES('corrupt',1,'{"source":"BMKG","revision":"bad"}','correlation',now()-interval '1 hour')`)

	p := &selectivePublisher{}
	for range publicationAttempts {
		if _, err := s.RelayOne(ctx, p); err == nil {
			t.Fatal("bad payload accepted")
		}
	}

	if sent, err := s.RelayOne(ctx, p); err != nil || !sent {
		t.Fatal(sent, err)
	}

	page, err := s.ListUnprocessed(ctx, "BMKG", "", 100)
	if err != nil || len(page.Items) != 1 || page.Items[0].Status != "parked" || page.Items[0].Retryable {
		t.Fatal(page, err)
	}
}

func TestIntegrationMalformedCriticalRollsBackEventAndOutbox(t *testing.T) {
	s := integrationStore(t)
	h := contractEvent("SENSOR_PANTAI", "bad-critical", time.Now().UTC())

	h.Attributes["critical"] = []byte(`"true"`)
	if _, err := s.Upsert(context.Background(), WriteBatch{Source: h.Source, Epoch: 1, Now: h.IngestedAt, Events: []HazardEvent{h}}); err == nil {
		t.Fatal("malformed critical accepted")
	}

	var events, outbox int
	if err := s.query.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM hazard_events),(SELECT count(*) FROM public.publication_outbox)`).Scan(&events, &outbox); err != nil || events != 0 || outbox != 0 {
		t.Fatal(events, outbox, err)
	}
}

func (p *timeoutPublisher) Publish(ctx context.Context, _ Publication) error {
	// Trigger the timeout at publish time, after the database has locked the row.
	p.cancel(context.DeadlineExceeded)
	return context.Cause(ctx)
}

func (p *timeoutPublisher) PublishCritical(ctx context.Context, item Publication) error {
	return p.Publish(ctx, item)
}

func (*timeoutPublisher) Close() {}

func TestIntegrationOutboxPersistsTimeoutAttempts(t *testing.T) {
	s := integrationStore(t)

	h := contractEvent("BMKG", "timeout", time.Now().UTC())
	if _, err := s.Upsert(context.Background(), WriteBatch{Source: h.Source, Epoch: 1, Now: h.IngestedAt, Events: []HazardEvent{h}}); err != nil {
		t.Fatal(err)
	}

	for range publicationAttempts {
		call, cancel := context.WithCancelCause(context.Background())
		_, err := s.RelayOne(call, &timeoutPublisher{cancel: cancel})

		cancel(nil)

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("publish timeout not reported: %v", err)
		}
	}

	page, err := s.ListUnprocessed(context.Background(), "BMKG", "", 100)
	if err != nil || len(page.Items) != 1 || page.Items[0].Attempts != publicationAttempts {
		t.Fatal("timeouts did not advance durable retry counter", page, err)
	}
}
