package aggregator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type Outbox interface {
	RelayOne(context.Context, EventPublisher) (bool, error)
}

const (
	// A poison row must not indefinitely block newer alerts.
	publicationAttempts = 5
	// Broker outages are retried automatically from durable parking, not discarded.
	publicationRetryDelay = 30 * time.Second
)

func eventCritical(h HazardEvent) (bool, error) {
	if h.Source != "SENSOR_PANTAI" {
		return false, nil
	}

	raw, present := h.Attributes["critical"]
	if !present {
		return false, nil
	}

	var critical *bool
	if err := json.Unmarshal(raw, &critical); err != nil {
		return false, fmt.Errorf("hazard %s: invalid critical attribute: %w", h.HazardID, err)
	}

	if critical == nil {
		return false, fmt.Errorf("hazard %s: critical must be a boolean", h.HazardID)
	}

	return *critical, nil
}

func enqueueEvent(ctx context.Context, tx pgx.Tx, h HazardEvent, id string) error {
	critical, err := eventCritical(h)
	if err != nil {
		return err
	}

	raw, err := json.Marshal(h)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `INSERT INTO public.publication_outbox(hazard_id,revision,payload,correlation_id,critical) VALUES($1,$2,$3::jsonb,$4,$5) ON CONFLICT DO NOTHING`, h.HazardID, h.Revision, string(raw), id, critical)

	return err
}

// RelayOne holds a SKIP LOCKED claim until confirmation and deletion commit.
// Failed delivery is persisted; an uncertain confirmation permits a duplicate.
func (s *Store) RelayOne(ctx context.Context, p EventPublisher) (bool, error) {
	return s.relayOne(ctx, p, false)
}

func (s *Store) relayOne(ctx context.Context, p EventPublisher, criticalOnly bool) (bool, error) {
	tx, err := s.ingest.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	if err = retryParkedPublication(ctx, tx); err != nil {
		return false, err
	}

	var (
		raw              []byte
		item             Publication
		critical         bool
		hazardID, source string
		revision         int64
		attempts         int
		created          time.Time
	)

	err = tx.QueryRow(ctx, `SELECT hazard_id,revision,payload,correlation_id,critical,attempts,created_at,COALESCE(payload->>'source','UNKNOWN') FROM public.publication_outbox WHERE (NOT $1 OR critical) ORDER BY critical DESC,created_at,hazard_id,revision FOR UPDATE SKIP LOCKED LIMIT 1`, criticalOnly).Scan(&hazardID, &revision, &raw, &item.CorrelationID, &critical, &attempts, &created, &source)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, tx.Commit(ctx)
	}

	if err != nil {
		return false, err
	}

	decodeErr := json.Unmarshal(raw, &item.Event)

	publishErr := decodeErr
	if publishErr == nil {
		publishErr = publishEvent(ctx, p, item, critical)
	}

	messageID := publicationMessageID(hazardID, revision)

	if publishErr != nil {
		// The broker may have exhausted the caller's deadline. Persist the failed
		// attempt under a separate bounded context so timeouts cannot poison the head.
		record, cancel := context.WithTimeout(context.WithoutCancel(ctx), shardDeadline)
		defer cancel()

		attempts++
		if attempts < publicationAttempts {
			_, err = tx.Exec(record, `UPDATE public.publication_outbox SET attempts=$3 WHERE hazard_id=$1 AND revision=$2`, hazardID, revision, attempts)
		} else {
			err = parkPublication(record, tx, messageID, source, raw, item.CorrelationID, attempts, created, publishErr, decodeErr == nil)
			if err == nil {
				_, err = tx.Exec(record, `DELETE FROM public.publication_outbox WHERE hazard_id=$1 AND revision=$2`, hazardID, revision)
			}
		}

		if err != nil {
			return false, err
		}

		if err = tx.Commit(record); err != nil {
			return false, err
		}

		return false, publishErr
	}

	if _, err = tx.Exec(ctx, `DELETE FROM public.publication_outbox WHERE hazard_id=$1 AND revision=$2`, hazardID, revision); err != nil {
		return false, err
	}

	if _, err = tx.Exec(ctx, `UPDATE public.unprocessed_messages SET status='recovered',next_attempt_at=NULL WHERE message_id=$1 AND reason_code='publish_failed'`, messageID); err != nil {
		return false, err
	}

	return true, tx.Commit(ctx)
}

func publishEvent(ctx context.Context, p EventPublisher, item Publication, critical bool) error {
	if critical {
		publisher, ok := p.(interface {
			PublishCritical(context.Context, Publication) error
		})
		if !ok {
			return errors.New("publisher has no critical lane")
		}

		if err := publisher.PublishCritical(ctx, item); err != nil {
			return err
		}
	}

	return p.Publish(ctx, item)
}

func publicationMessageID(id string, revision int64) string {
	return fmt.Sprintf("publication:%s:%d", id, revision)
}

func parkPublication(ctx context.Context, tx pgx.Tx, id, source string, raw []byte, correlation string, attempts int, created time.Time, failure error, retryable bool) error {
	sum := sha256.Sum256(raw)
	status := parkingParked

	var next *time.Time

	if retryable {
		status = parkingRetrying
		value := time.Now().UTC().Add(publicationRetryDelay)
		next = &value
	}

	_, err := tx.Exec(ctx, `INSERT INTO public.unprocessed_messages (`+parkingColumns+`)
 VALUES($1,$2,$3,'',$4,$5,$6,false,'publish_failed',$7,$8,$9,$10,$11,$12)
 ON CONFLICT(message_id) DO UPDATE SET status=EXCLUDED.status,reason=EXCLUDED.reason,
 attempts=unprocessed_messages.attempts+EXCLUDED.attempts,next_attempt_at=EXCLUDED.next_attempt_at`, id, source, created, raw, len(raw), hex.EncodeToString(sum[:]), failure.Error(), retryable, attempts, next, status, correlation)

	return err
}

// Publications use their own retry path. Source replay must not parse an event
// publication as an upstream message or reapply its canonical event.
func retryParkedPublication(ctx context.Context, tx pgx.Tx) error {
	var (
		id, correlation string
		raw             []byte
	)

	err := tx.QueryRow(ctx, `SELECT message_id,raw,correlation_id FROM public.unprocessed_messages
 WHERE reason_code='publish_failed' AND status='retrying' AND next_attempt_at<=now()
 ORDER BY received_at,message_id LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&id, &raw, &correlation)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}

	if err != nil {
		return err
	}

	var h HazardEvent

	decodeErr := json.Unmarshal(raw, &h)

	critical, criticalErr := eventCritical(h)
	if decodeErr != nil || criticalErr != nil {
		_, err = tx.Exec(ctx, `UPDATE public.unprocessed_messages SET status='parked',next_attempt_at=NULL WHERE message_id=$1`, id)
		return err
	}

	if _, err = tx.Exec(ctx, `INSERT INTO public.publication_outbox(hazard_id,revision,payload,correlation_id,critical)
 VALUES($1,$2,$3::jsonb,$4,$5) ON CONFLICT DO NOTHING`, h.HazardID, h.Revision, string(raw), correlation, critical); err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `UPDATE public.unprocessed_messages SET next_attempt_at=NULL WHERE message_id=$1`, id)

	return err
}

func RunRelay(ctx context.Context, s Outbox, p EventPublisher, interval time.Duration, onError func(error)) {
	defer p.Close()

	for ctx.Err() == nil {
		call, cancel := context.WithTimeout(ctx, 5*time.Second)
		sent, err := s.RelayOne(call, p)

		cancel()

		if err != nil {
			onError(err)
		}

		if sent && err == nil {
			continue
		}

		if !pause(ctx, interval) {
			return
		}
	}
}
